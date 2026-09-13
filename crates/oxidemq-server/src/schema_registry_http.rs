use axum::extract::{Path, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use oxidemq_broker::schema_registry::{
    CompatibilityLevel, SchemaEntry, SchemaReference, SchemaRegistry, SchemaRegistryError,
    SchemaType,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

/// Request payload for registering or checking schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSchemaRequest {
    pub schema: String,
    #[serde(rename = "schemaType")]
    pub schema_type: Option<SchemaType>,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

/// Request payload for updating compatibility levels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateRequest {
    pub compatibility: Option<CompatibilityLevel>,
    #[serde(rename = "compatibilityLevel")]
    pub compatibility_level: Option<CompatibilityLevel>,
}

/// Newtype wrapper for SchemaRegistryError to implement Axum IntoResponse.
#[derive(Debug)]
pub struct SchemaError(pub SchemaRegistryError);

impl From<SchemaRegistryError> for SchemaError {
    fn from(err: SchemaRegistryError) -> Self {
        Self(err)
    }
}

impl IntoResponse for SchemaError {
    fn into_response(self) -> Response {
        let (status, code) = self.0.error_details();
        let body = Json(json!({
            "error_code": code,
            "message": self.0.to_string()
        }));
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/vnd.schemaregistry.v1+json"),
            )],
            body,
        )
            .into_response()
    }
}

/// Creates Axum router with Schema Registry core routes (/subjects, /schemas, /compatibility, /config).
pub fn create_schema_registry_routes(registry: Arc<SchemaRegistry>) -> Router {
    Router::new()
        // Subjects
        .route("/subjects", get(list_subjects_handler))
        .route(
            "/subjects/{subject}",
            post(check_schema_handler).delete(delete_subject_handler),
        )
        // Versions
        .route(
            "/subjects/{subject}/versions",
            get(list_versions_handler).post(register_schema_handler),
        )
        .route(
            "/subjects/{subject}/versions/{version}",
            get(get_subject_version_handler).delete(delete_version_handler),
        )
        .route(
            "/subjects/{subject}/versions/{version}/schema",
            get(get_subject_version_raw_schema_handler),
        )
        // Schemas by ID
        .route("/schemas/ids/{id}", get(get_schema_by_id_handler))
        .route(
            "/schemas/ids/{id}/schema",
            get(get_schema_by_id_raw_handler),
        )
        // Compatibility
        .route(
            "/compatibility/subjects/{subject}/versions/{version}",
            post(check_compatibility_handler),
        )
        // Config
        .route("/config", get(get_global_config).put(put_global_config))
        .route(
            "/config/{subject}",
            get(get_subject_config).put(put_subject_config),
        )
        .with_state(registry)
}

/// Creates standalone Axum router exposing Confluent Schema Registry HTTP API v1 on port 8081.
pub fn create_schema_registry_router(registry: Arc<SchemaRegistry>) -> Router {
    Router::new()
        .route("/", get(root_handler))
        .route("/_oxidemq/health", get(health_handler))
        .merge(create_schema_registry_routes(registry))
}

async fn root_handler() -> &'static str {
    "oxideMq Confluent-Compatible Schema Registry v1"
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn list_subjects_handler(State(reg): State<Arc<SchemaRegistry>>) -> Json<Vec<String>> {
    Json(reg.list_subjects())
}

async fn register_schema_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(subject): Path<String>,
    Json(req): Json<RegisterSchemaRequest>,
) -> Result<Json<serde_json::Value>, SchemaError> {
    let id = reg.register_schema(&subject, &req.schema, req.schema_type, req.references)?;
    Ok(Json(json!({ "id": id })))
}

async fn list_versions_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(subject): Path<String>,
) -> Result<Json<Vec<i32>>, SchemaError> {
    let vers = reg.list_versions(&subject)?;
    Ok(Json(vers))
}

async fn get_subject_version_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Json<SchemaEntry>, SchemaError> {
    let entry = reg.get_schema_by_subject_version(&subject, &version)?;
    Ok(Json(entry))
}

async fn get_subject_version_raw_schema_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<String, SchemaError> {
    let entry = reg.get_schema_by_subject_version(&subject, &version)?;
    Ok(entry.schema)
}

async fn get_schema_by_id_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(id): Path<i32>,
) -> Result<Json<serde_json::Value>, SchemaError> {
    match reg.get_schema_by_id(id) {
        Some(entry) => Ok(Json(json!({
            "schema": entry.schema,
            "schemaType": entry.schema_type,
            "references": entry.references
        }))),
        None => Err(SchemaError(SchemaRegistryError::SchemaNotFound(id))),
    }
}

async fn get_schema_by_id_raw_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(id): Path<i32>,
) -> Result<String, SchemaError> {
    match reg.get_schema_by_id(id) {
        Some(entry) => Ok(entry.schema),
        None => Err(SchemaError(SchemaRegistryError::SchemaNotFound(id))),
    }
}

async fn check_schema_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(subject): Path<String>,
    Json(req): Json<RegisterSchemaRequest>,
) -> Result<Json<SchemaEntry>, SchemaError> {
    match reg.check_schema_registered(&subject, &req.schema, req.schema_type) {
        Some(entry) => Ok(Json(entry)),
        None => Err(SchemaError(SchemaRegistryError::SchemaNotFound(-1))),
    }
}

async fn delete_subject_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(subject): Path<String>,
) -> Result<Json<Vec<i32>>, SchemaError> {
    let vers = reg.delete_subject(&subject)?;
    Ok(Json(vers))
}

async fn delete_version_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Json<i32>, SchemaError> {
    let ver = reg.delete_version(&subject, &version)?;
    Ok(Json(ver))
}

async fn check_compatibility_handler(
    State(reg): State<Arc<SchemaRegistry>>,
    Path((subject, version)): Path<(String, String)>,
    Json(req): Json<RegisterSchemaRequest>,
) -> Result<Json<serde_json::Value>, SchemaError> {
    let is_compat = reg.check_compatibility(&subject, &version, &req.schema, req.schema_type)?;
    Ok(Json(json!({ "is_compatible": is_compat })))
}

async fn get_global_config(State(reg): State<Arc<SchemaRegistry>>) -> Json<serde_json::Value> {
    let lvl = reg.get_config(None);
    Json(json!({ "compatibilityLevel": lvl }))
}

async fn put_global_config(
    State(reg): State<Arc<SchemaRegistry>>,
    Json(req): Json<ConfigUpdateRequest>,
) -> Json<serde_json::Value> {
    let lvl = req
        .compatibility_level
        .or(req.compatibility)
        .unwrap_or(CompatibilityLevel::Backward);
    reg.set_config(None, lvl);
    Json(json!({ "compatibilityLevel": lvl }))
}

async fn get_subject_config(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(subject): Path<String>,
) -> Json<serde_json::Value> {
    let lvl = reg.get_config(Some(&subject));
    Json(json!({ "compatibilityLevel": lvl }))
}

async fn put_subject_config(
    State(reg): State<Arc<SchemaRegistry>>,
    Path(subject): Path<String>,
    Json(req): Json<ConfigUpdateRequest>,
) -> Json<serde_json::Value> {
    let lvl = req
        .compatibility_level
        .or(req.compatibility)
        .unwrap_or(CompatibilityLevel::Backward);
    reg.set_config(Some(&subject), lvl);
    Json(json!({ "compatibilityLevel": lvl }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_schema_registry_http_endpoints() {
        let registry = Arc::new(SchemaRegistry::new());
        let app = create_schema_registry_router(registry);

        // 1. Health check
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/_oxidemq/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 2. Register Avro schema
        let schema_json = r#"{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}","schemaType":"AVRO"}"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/subjects/users-value/versions")
                    .header("content-type", "application/json")
                    .body(Body::from(schema_json))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(v["id"], 1);

        // 3. List subjects
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/subjects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let subs: Vec<String> = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(subs, vec!["users-value"]);

        // 4. Get version latest
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/subjects/users-value/versions/latest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 5. Get schema by ID 1
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/schemas/ids/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 6. Test compatibility check
        let new_schema = r#"{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}]}","schemaType":"AVRO"}"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compatibility/subjects/users-value/versions/latest")
                    .header("content-type", "application/json")
                    .body(Body::from(new_schema))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let res: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(res["is_compatible"], true);

        // 7. Test 404 for unknown subject
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/subjects/unknown-subject/versions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
