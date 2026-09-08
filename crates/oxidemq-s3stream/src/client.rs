use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client as AwsS3Client;
use bytes::Bytes;
use oxidemq_core::error::{OxideMqError, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Object storage abstraction for S3 / RustStack S3.
pub trait ObjectStorage: Send + Sync {
    /// Uploads an immutable object to the specified key.
    fn put_object(&self, key: &str, data: Bytes) -> Result<()>;

    /// Retrieves an entire object from storage.
    fn get_object(&self, key: &str) -> Result<Bytes>;

    /// Retrieves a byte range `[start, end]` (inclusive) from the specified object.
    fn get_object_range(&self, key: &str, start: u64, end: u64) -> Result<Bytes>;

    /// Deletes a single object from storage.
    fn delete_object(&self, key: &str) -> Result<()>;

    /// Deletes multiple objects in a single batch.
    fn delete_objects(&self, keys: &[String]) -> Result<()>;

    /// Lists object keys matching the given prefix.
    fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;
}

/// A high-performance, in-memory object store for Tier 1 tests, RustStack emulation,
/// and nanosecond microbenchmarks.
#[derive(Debug, Default)]
pub struct MemoryObjectStorage {
    objects: Arc<RwLock<HashMap<String, Bytes>>>,
    put_count: AtomicU64,
    get_count: AtomicU64,
    bytes_stored: AtomicU64,
}

impl MemoryObjectStorage {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            put_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            bytes_stored: AtomicU64::new(0),
        }
    }

    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.bytes_stored.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let mut obs = self.objects.write();
        obs.clear();
        self.bytes_stored.store(0, Ordering::Relaxed);
    }
}

impl ObjectStorage for MemoryObjectStorage {
    fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
        let len = data.len() as u64;
        let mut obs = self.objects.write();
        if let Some(old) = obs.insert(key.to_string(), data) {
            self.bytes_stored
                .fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
        self.bytes_stored.fetch_add(len, Ordering::Relaxed);
        self.put_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get_object(&self, key: &str) -> Result<Bytes> {
        let obs = self.objects.read();
        self.get_count.fetch_add(1, Ordering::Relaxed);
        obs.get(key)
            .cloned()
            .ok_or_else(|| OxideMqError::Storage(format!("Object not found: {}", key)))
    }

    fn get_object_range(&self, key: &str, start: u64, end: u64) -> Result<Bytes> {
        let obs = self.objects.read();
        self.get_count.fetch_add(1, Ordering::Relaxed);
        let obj = obs
            .get(key)
            .ok_or_else(|| OxideMqError::Storage(format!("Object not found: {}", key)))?;

        let obj_len = obj.len() as u64;
        if start >= obj_len {
            return Ok(Bytes::new());
        }

        let actual_end = (end + 1).min(obj_len);
        let slice = &obj[start as usize..actual_end as usize];
        Ok(Bytes::copy_from_slice(slice))
    }

    fn delete_object(&self, key: &str) -> Result<()> {
        let mut obs = self.objects.write();
        if let Some(data) = obs.remove(key) {
            self.bytes_stored
                .fetch_sub(data.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn delete_objects(&self, keys: &[String]) -> Result<()> {
        let mut obs = self.objects.write();
        for key in keys {
            if let Some(data) = obs.remove(key) {
                self.bytes_stored
                    .fetch_sub(data.len() as u64, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let obs = self.objects.read();
        let mut matches: Vec<String> = obs
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        matches.sort();
        Ok(matches)
    }
}

/// Live cloud S3 / RustStack S3 object storage implementation backed by `aws-sdk-s3`.
#[derive(Clone)]
pub struct S3ClientStorage {
    client: AwsS3Client,
    bucket: String,
}

impl std::fmt::Debug for S3ClientStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ClientStorage")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl S3ClientStorage {
    /// Creates a new `S3ClientStorage` instance with optional custom endpoint and region.
    pub fn new(
        bucket: impl Into<String>,
        endpoint: Option<&str>,
        region: Option<&str>,
    ) -> Result<Self> {
        let region_str = region.unwrap_or("us-east-1");
        let creds = Credentials::new(
            std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "ruststack".to_string()),
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "ruststack".to_string()),
            None,
            None,
            "oxidemq",
        );

        let mut conf_builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region_str.to_string()))
            .credentials_provider(creds)
            .force_path_style(true);

        if let Some(ep) = endpoint {
            conf_builder = conf_builder.endpoint_url(ep);
        }

        let s3_conf = conf_builder.build();
        let client = AwsS3Client::from_conf(s3_conf);

        Ok(Self {
            client,
            bucket: bucket.into(),
        })
    }

    /// Creates an `S3ClientStorage` using standard oxideMq environment variables:
    /// - `OXIDEMQ_S3_BUCKET` (default: "oxidemq-data")
    /// - `OXIDEMQ_S3_ENDPOINT` (e.g. "http://localhost:4566")
    /// - `OXIDEMQ_S3_REGION` (default: "us-east-1")
    pub fn from_env() -> Result<Self> {
        let bucket =
            std::env::var("OXIDEMQ_S3_BUCKET").unwrap_or_else(|_| "oxidemq-data".to_string());
        let endpoint = std::env::var("OXIDEMQ_S3_ENDPOINT").ok();
        let region = std::env::var("OXIDEMQ_S3_REGION").ok();
        Self::new(bucket, endpoint.as_deref(), region.as_deref())
    }

    /// Returns a reference to the inner AWS S3 client.
    pub fn client(&self) -> &AwsS3Client {
        &self.client
    }

    /// Returns the target bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Uploads an immutable object asynchronously.
    pub async fn put_object_async(&self, key: &str, data: Bytes) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| {
                OxideMqError::Storage(format!("S3 PutObject error for key '{key}': {e}"))
            })?;
        Ok(())
    }

    /// Retrieves an entire object asynchronously.
    pub async fn get_object_async(&self, key: &str) -> Result<Bytes> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let err_str = e.to_string();
                if err_str.contains("NoSuchKey")
                    || err_str.contains("404")
                    || err_str.contains("NotFound")
                {
                    OxideMqError::Storage(format!("Object not found: {key}"))
                } else {
                    OxideMqError::Storage(format!("S3 GetObject error for key '{key}': {e}"))
                }
            })?;

        let body = resp.body.collect().await.map_err(|e| {
            OxideMqError::Storage(format!("Failed to read S3 body for key '{key}': {e}"))
        })?;
        Ok(body.into_bytes())
    }

    /// Retrieves a byte range asynchronously.
    pub async fn get_object_range_async(&self, key: &str, start: u64, end: u64) -> Result<Bytes> {
        let range_hdr = format!("bytes={start}-{end}");
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range_hdr)
            .send()
            .await
            .map_err(|e| {
                let err_str = e.to_string();
                if err_str.contains("NoSuchKey")
                    || err_str.contains("404")
                    || err_str.contains("NotFound")
                {
                    OxideMqError::Storage(format!("Object not found: {key}"))
                } else {
                    OxideMqError::Storage(format!("S3 GetObjectRange error for key '{key}': {e}"))
                }
            })?;

        let body = resp.body.collect().await.map_err(|e| {
            OxideMqError::Storage(format!("Failed to read S3 range body for key '{key}': {e}"))
        })?;
        Ok(body.into_bytes())
    }

    /// Deletes a single object asynchronously.
    pub async fn delete_object_async(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                OxideMqError::Storage(format!("S3 DeleteObject error for key '{key}': {e}"))
            })?;
        Ok(())
    }

    /// Deletes multiple objects asynchronously.
    pub async fn delete_objects_async(&self, keys: &[String]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut delete_b = Delete::builder();
        for k in keys {
            let obj_id = ObjectIdentifier::builder().key(k).build().map_err(|e| {
                OxideMqError::Storage(format!("Failed to build ObjectIdentifier: {e}"))
            })?;
            delete_b = delete_b.objects(obj_id);
        }

        let delete = delete_b
            .build()
            .map_err(|e| OxideMqError::Storage(format!("Failed to build Delete request: {e}")))?;

        self.client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await
            .map_err(|e| OxideMqError::Storage(format!("S3 DeleteObjects error: {e}")))?;

        Ok(())
    }

    /// Lists object keys matching the given prefix asynchronously.
    pub async fn list_objects_async(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| OxideMqError::Storage(format!("S3 ListObjectsV2 error: {e:?}")))?;

            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(k) = obj.key {
                        keys.push(k);
                    }
                }
            }

            if resp.is_truncated.unwrap_or(false) && resp.next_continuation_token.is_some() {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        keys.sort();
        Ok(keys)
    }
}

fn run_storage_future<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                tokio::task::block_in_place(|| handle.block_on(fut))
            } else {
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to build current_thread runtime");
                    rt.block_on(fut)
                })
                .join()
                .expect("S3 executor thread panicked")
            }
        }
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build current_thread runtime");
            rt.block_on(fut)
        }
    }
}

impl ObjectStorage for S3ClientStorage {
    fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
        let storage = self.clone();
        let key_str = key.to_string();
        run_storage_future(async move { storage.put_object_async(&key_str, data).await })
    }

    fn get_object(&self, key: &str) -> Result<Bytes> {
        let storage = self.clone();
        let key_str = key.to_string();
        run_storage_future(async move { storage.get_object_async(&key_str).await })
    }

    fn get_object_range(&self, key: &str, start: u64, end: u64) -> Result<Bytes> {
        let storage = self.clone();
        let key_str = key.to_string();
        run_storage_future(
            async move { storage.get_object_range_async(&key_str, start, end).await },
        )
    }

    fn delete_object(&self, key: &str) -> Result<()> {
        let storage = self.clone();
        let key_str = key.to_string();
        run_storage_future(async move { storage.delete_object_async(&key_str).await })
    }

    fn delete_objects(&self, keys: &[String]) -> Result<()> {
        let storage = self.clone();
        let keys_vec = keys.to_vec();
        run_storage_future(async move { storage.delete_objects_async(&keys_vec).await })
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let storage = self.clone();
        let prefix_str = prefix.to_string();
        run_storage_future(async move { storage.list_objects_async(&prefix_str).await })
    }
}

/// Decorator that retries object operations with exponential backoff on transient S3 errors (503 SlowDown / RateLimit).
pub struct RetryableObjectStorage<S> {
    inner: S,
    max_retries: usize,
    base_backoff_ms: u64,
}

impl<S: ObjectStorage> RetryableObjectStorage<S> {
    pub fn new(inner: S, max_retries: usize, base_backoff_ms: u64) -> Self {
        Self {
            inner,
            max_retries,
            base_backoff_ms,
        }
    }

    fn execute_with_retry<T, F: Fn(&S) -> Result<T>>(&self, op: F) -> Result<T> {
        let mut attempts = 0;
        let mut backoff = self.base_backoff_ms;
        loop {
            match op(&self.inner) {
                Ok(val) => return Ok(val),
                Err(err) => {
                    attempts += 1;
                    let err_str = err.to_string();
                    let is_transient = err_str.contains("503")
                        || err_str.contains("SlowDown")
                        || err_str.contains("RateLimit")
                        || err_str.contains("TooManyRequests");

                    if is_transient && attempts <= self.max_retries {
                        if backoff > 0 {
                            std::thread::sleep(std::time::Duration::from_millis(backoff));
                            backoff = (backoff * 2).min(1000);
                        }
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }
}

impl<S: ObjectStorage> ObjectStorage for RetryableObjectStorage<S> {
    fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
        self.execute_with_retry(|s| s.put_object(key, data.clone()))
    }

    fn get_object(&self, key: &str) -> Result<Bytes> {
        self.execute_with_retry(|s| s.get_object(key))
    }

    fn get_object_range(&self, key: &str, start: u64, end: u64) -> Result<Bytes> {
        self.execute_with_retry(|s| s.get_object_range(key, start, end))
    }

    fn delete_object(&self, key: &str) -> Result<()> {
        self.execute_with_retry(|s| s.delete_object(key))
    }

    fn delete_objects(&self, keys: &[String]) -> Result<()> {
        self.execute_with_retry(|s| s.delete_objects(keys))
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.execute_with_retry(|s| s.list_objects(prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_object_storage() {
        let storage = MemoryObjectStorage::new();
        let key = "streams/stream-1/00000000000000000000.data";
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        storage.put_object(key, payload.clone()).unwrap();
        assert_eq!(storage.put_count(), 1);

        // Full get
        let retrieved = storage.get_object(key).unwrap();
        assert_eq!(retrieved, payload);

        // Range get: bytes=4-9 ("efghij")
        let range = storage.get_object_range(key, 4, 9).unwrap();
        assert_eq!(range.as_ref(), b"efghij");

        // List objects
        let list = storage.list_objects("streams/stream-1").unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], key);

        // Delete
        storage.delete_object(key).unwrap();
        assert!(storage.get_object(key).is_err());
    }

    struct FailingStorage {
        failures_remaining: std::sync::atomic::AtomicUsize,
        inner: MemoryObjectStorage,
    }

    impl ObjectStorage for FailingStorage {
        fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
            let rem = self
                .failures_remaining
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            if rem > 0 {
                Err(OxideMqError::Storage(
                    "S3 API 503 SlowDown: Rate limit exceeded, back off and retry".to_string(),
                ))
            } else {
                self.inner.put_object(key, data)
            }
        }
        fn get_object(&self, key: &str) -> Result<Bytes> {
            self.inner.get_object(key)
        }
        fn get_object_range(&self, key: &str, start: u64, end: u64) -> Result<Bytes> {
            self.inner.get_object_range(key, start, end)
        }
        fn delete_object(&self, key: &str) -> Result<()> {
            self.inner.delete_object(key)
        }
        fn delete_objects(&self, keys: &[String]) -> Result<()> {
            self.inner.delete_objects(keys)
        }
        fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list_objects(prefix)
        }
    }

    #[test]
    fn test_s3_rate_limit_and_retry() {
        let failing = FailingStorage {
            failures_remaining: std::sync::atomic::AtomicUsize::new(2),
            inner: MemoryObjectStorage::new(),
        };

        // Retryable wrapper: 3 retries with 1ms backoff
        let retryable = RetryableObjectStorage::new(failing, 3, 1);
        let res = retryable.put_object("test-key", Bytes::from_static(b"resilient-payload"));
        assert!(res.is_ok(), "Expected success after retry");
        assert_eq!(
            retryable.get_object("test-key").unwrap(),
            Bytes::from_static(b"resilient-payload")
        );

        // Test other operations delegated by RetryableObjectStorage
        let range = retryable.get_object_range("test-key", 0, 8).unwrap();
        assert_eq!(range.as_ref(), b"resilient");

        let list = retryable.list_objects("test").unwrap();
        assert_eq!(list.len(), 1);

        retryable.delete_object("test-key").unwrap();
        assert!(retryable.get_object("test-key").is_err());

        // Batch delete via retryable
        let storage = MemoryObjectStorage::new();
        storage.put_object("k1", Bytes::from_static(b"v1")).unwrap();
        storage.put_object("k2", Bytes::from_static(b"v2")).unwrap();
        assert_eq!(storage.bytes_stored.load(Ordering::Relaxed), 4);
        assert_eq!(storage.get_count(), 0);

        let retryable2 = RetryableObjectStorage::new(storage, 2, 1);
        retryable2
            .delete_objects(&["k1".into(), "k2".into()])
            .unwrap();
        let remaining = retryable2.list_objects("").unwrap();
        assert!(remaining.is_empty());

        // MemoryObjectStorage clear & out of bounds range
        let mem = MemoryObjectStorage::new();
        mem.put_object("small", Bytes::from_static(b"12345"))
            .unwrap();
        let oob_range = mem.get_object_range("small", 10, 20).unwrap();
        assert!(oob_range.is_empty());
        mem.clear();
        assert_eq!(mem.bytes_stored.load(Ordering::Relaxed), 0);

        // Permanent error through retryable
        let failing_perm = FailingStorage {
            failures_remaining: std::sync::atomic::AtomicUsize::new(10),
            inner: MemoryObjectStorage::new(),
        };
        let retryable_exhaust = RetryableObjectStorage::new(failing_perm, 1, 1);
        assert!(retryable_exhaust
            .put_object("k", Bytes::from_static(b"v"))
            .is_err());
    }

    #[test]
    fn test_s3_client_storage_from_env() {
        std::env::set_var("OXIDEMQ_S3_BUCKET", "env-bucket");
        std::env::set_var("OXIDEMQ_S3_ENDPOINT", "http://127.0.0.1:4566");
        std::env::set_var("OXIDEMQ_S3_REGION", "us-west-2");

        let s3 = S3ClientStorage::from_env().unwrap();
        assert_eq!(s3.bucket(), "env-bucket");

        std::env::remove_var("OXIDEMQ_S3_BUCKET");
        std::env::remove_var("OXIDEMQ_S3_ENDPOINT");
        std::env::remove_var("OXIDEMQ_S3_REGION");
    }

    #[test]
    fn test_s3_storage_from_non_tokio_thread() {
        let handle = std::thread::spawn(|| {
            let res = run_storage_future(async { 42 });
            assert_eq!(res, 42);
        });
        handle.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s3_client_storage_with_mock_server() {
        use axum::extract::{Path, Query};
        use axum::http::{HeaderMap, StatusCode};
        use axum::response::{IntoResponse, Response};
        use axum::routing::get;
        use axum::Router;
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let store: Arc<Mutex<HashMap<String, Bytes>>> = Arc::new(Mutex::new(HashMap::new()));
        let store_put = Arc::clone(&store);
        let store_get = Arc::clone(&store);
        let store_del = Arc::clone(&store);
        let store_post = Arc::clone(&store);
        let store_list = Arc::clone(&store);

        let list_fn = {
            let store = Arc::clone(&store_list);
            move |_bucket: String, params: HashMap<String, String>| {
                let store = Arc::clone(&store);
                async move {
                    let prefix = params.get("prefix").cloned().unwrap_or_default();
                    let s = store.lock().await;
                    let mut matches = Vec::new();
                    for (k, val) in s.iter() {
                        if k.starts_with(&prefix) {
                            matches.push((k.clone(), val.len()));
                        }
                    }
                    let mut xml = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>mock-bucket</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
                        prefix, matches.len()
                    );
                    for (k, sz) in matches {
                        xml.push_str(&format!(
                            "<Contents><Key>{}</Key><Size>{}</Size><ETag>\"abc\"</ETag><LastModified>2026-09-08T00:00:00.000Z</LastModified><StorageClass>STANDARD</StorageClass></Contents>",
                            k, sz
                        ));
                    }
                    xml.push_str("</ListBucketResult>");
                    Response::builder()
                        .header("content-type", "application/xml")
                        .body(axum::body::Body::from(xml))
                        .unwrap()
                }
            }
        };

        let post_fn = {
            let store = Arc::clone(&store_post);
            move |params: HashMap<String, String>, body: axum::body::Bytes| {
                let store = Arc::clone(&store);
                async move {
                    if params.contains_key("delete") {
                        let body_str = String::from_utf8_lossy(&body);
                        let mut s = store.lock().await;
                        let mut deleted_xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
                        for segment in body_str.split("<Key>") {
                            if let Some(key) = segment.split("</Key>").next() {
                                let key = key.trim();
                                if !key.is_empty() && !key.contains('<') {
                                    s.remove(key);
                                    deleted_xml.push_str(&format!(
                                        "<Deleted><Key>{}</Key></Deleted>",
                                        key
                                    ));
                                }
                            }
                        }
                        deleted_xml.push_str("</DeleteResult>");
                        Response::builder()
                            .header("content-type", "application/xml")
                            .body(axum::body::Body::from(deleted_xml))
                            .unwrap()
                    } else {
                        StatusCode::OK.into_response()
                    }
                }
            }
        };

        let list_fn_1 = list_fn.clone();
        let list_fn_2 = list_fn.clone();
        let post_fn_1 = post_fn.clone();
        let post_fn_2 = post_fn.clone();

        let app = Router::new()
            .route(
                "/{bucket}",
                get(move |Path(b): Path<String>, Query(q): Query<HashMap<String, String>>| list_fn_1(b, q))
                    .post(move |Query(q): Query<HashMap<String, String>>, body: axum::body::Bytes| post_fn_1(q, body)),
            )
            .route(
                "/{bucket}/",
                get(move |Path(b): Path<String>, Query(q): Query<HashMap<String, String>>| list_fn_2(b, q))
                    .post(move |Query(q): Query<HashMap<String, String>>, body: axum::body::Bytes| post_fn_2(q, body)),
            )
            .route(
                "/{bucket}/{*key}",
                axum::routing::put({
                    let store = Arc::clone(&store_put);
                    move |Path((_bucket, key)): Path<(String, String)>, body: axum::body::Bytes| {
                        let store = Arc::clone(&store);
                        async move {
                            store.lock().await.insert(key, Bytes::copy_from_slice(&body));
                            StatusCode::OK.into_response()
                        }
                    }
                })
                .get({
                    let store = Arc::clone(&store_get);
                    move |Path((_bucket, key)): Path<(String, String)>, Query(params): Query<HashMap<String, String>>, headers: HeaderMap| {
                        let store = Arc::clone(&store);
                        async move {
                            if params.contains_key("list-type") || key.is_empty() {
                                let prefix = params.get("prefix").cloned().unwrap_or_default();
                                let s = store.lock().await;
                                let mut matches = Vec::new();
                                for (k, val) in s.iter() {
                                    if k.starts_with(&prefix) {
                                        matches.push((k.clone(), val.len()));
                                    }
                                }
                                let mut xml = format!(
                                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>mock-bucket</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
                                    prefix, matches.len()
                                );
                                for (k, sz) in matches {
                                    xml.push_str(&format!(
                                        "<Contents><Key>{}</Key><Size>{}</Size><ETag>\"abc\"</ETag><LastModified>2026-09-08T00:00:00.000Z</LastModified><StorageClass>STANDARD</StorageClass></Contents>",
                                        k, sz
                                    ));
                                }
                                xml.push_str("</ListBucketResult>");
                                return Response::builder()
                                    .header("content-type", "application/xml")
                                    .body(axum::body::Body::from(xml))
                                    .unwrap();
                            }

                            let s = store.lock().await;
                            match s.get(&key) {
                                Some(val) => {
                                    if let Some(range_val) = headers.get("range").and_then(|h| h.to_str().ok()) {
                                        if let Some(range_str) = range_val.strip_prefix("bytes=") {
                                            let parts: Vec<&str> = range_str.split('-').collect();
                                            let start: usize = parts[0].parse().unwrap_or(0);
                                            let end: usize = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(val.len().saturating_sub(1));
                                            let end = end.min(val.len().saturating_sub(1));
                                            let slice = &val[start..=end];
                                            return Response::builder()
                                                .status(StatusCode::PARTIAL_CONTENT)
                                                .body(axum::body::Body::from(slice.to_vec()))
                                                .unwrap();
                                        }
                                    }
                                    Response::builder()
                                        .status(StatusCode::OK)
                                        .body(axum::body::Body::from(val.to_vec()))
                                        .unwrap()
                                }
                                None => {
                                    let err_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>";
                                    Response::builder()
                                        .status(StatusCode::NOT_FOUND)
                                        .header("content-type", "application/xml")
                                        .body(axum::body::Body::from(err_xml))
                                        .unwrap()
                                }
                            }
                        }
                    }
                })
                .post({
                    let store = Arc::clone(&store_post);
                    move |Path((_bucket, _key)): Path<(String, String)>, Query(params): Query<HashMap<String, String>>, body: axum::body::Bytes| {
                        let store = Arc::clone(&store);
                        async move {
                            if params.contains_key("delete") {
                                let body_str = String::from_utf8_lossy(&body);
                                let mut s = store.lock().await;
                                let mut deleted_xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
                                for segment in body_str.split("<Key>") {
                                    if let Some(key) = segment.split("</Key>").next() {
                                        let key = key.trim();
                                        if !key.is_empty() && !key.contains('<') {
                                            s.remove(key);
                                            deleted_xml.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", key));
                                        }
                                    }
                                }
                                deleted_xml.push_str("</DeleteResult>");
                                Response::builder()
                                    .header("content-type", "application/xml")
                                    .body(axum::body::Body::from(deleted_xml))
                                    .unwrap()
                            } else {
                                StatusCode::OK.into_response()
                            }
                        }
                    }
                })

                .delete({
                    let store = Arc::clone(&store_del);
                    move |Path((_bucket, key)): Path<(String, String)>| {
                        let store = Arc::clone(&store);
                        async move {
                            store.lock().await.remove(&key);
                            StatusCode::NO_CONTENT.into_response()
                        }
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let endpoint = format!("http://127.0.0.1:{port}");
        let storage =
            S3ClientStorage::new("mock-bucket", Some(&endpoint), Some("us-east-1")).unwrap();
        assert_eq!(storage.bucket(), "mock-bucket");
        assert!(format!("{:?}", storage).contains("mock-bucket"));

        // Test PutObject
        let key = "logs/stream-100/part-0.data";
        let data = Bytes::from_static(b"HELLO-CLOUD-NATIVE-S3STREAM-TEST");
        storage.put_object(key, data.clone()).unwrap();

        // Test GetObject
        let fetched = storage.get_object(key).unwrap();
        assert_eq!(fetched, data);

        // Test GetObjectRange: bytes=6-17 -> "CLOUD-NATIVE"
        let range = storage.get_object_range(key, 6, 17).unwrap();
        assert_eq!(range.as_ref(), b"CLOUD-NATIVE");

        // Test ListObjects
        let list = storage.list_objects("logs/stream-100").unwrap();
        assert_eq!(list, vec![key.to_string()]);

        // Test DeleteObject
        storage.delete_object(key).unwrap();
        assert!(storage.get_object(key).is_err());

        // Test Batch Delete
        storage.put_object("b1", Bytes::from_static(b"1")).unwrap();
        storage.put_object("b2", Bytes::from_static(b"2")).unwrap();
        assert_eq!(storage.list_objects("b").unwrap().len(), 2);
        storage.delete_objects(&["b1".into(), "b2".into()]).unwrap();
        assert!(storage.list_objects("b").unwrap().is_empty());

        // Test Async APIs
        storage
            .put_object_async("async-key", Bytes::from_static(b"async-data"))
            .await
            .unwrap();
        let async_fetched = storage.get_object_async("async-key").await.unwrap();
        assert_eq!(async_fetched.as_ref(), b"async-data");
        let async_range = storage
            .get_object_range_async("async-key", 0, 4)
            .await
            .unwrap();
        assert_eq!(async_range.as_ref(), b"async");
        let async_list = storage.list_objects_async("async").await.unwrap();
        assert_eq!(async_list.len(), 1);
        storage
            .delete_objects_async(&["async-key".into()])
            .await
            .unwrap();
        assert!(storage.get_object_async("async-key").await.is_err());
    }
}
