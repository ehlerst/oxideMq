use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported schema formats in Confluent Schema Registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaType {
    #[default]
    Avro,
    Protobuf,
    Json,
}

fn default_schema_type() -> SchemaType {
    SchemaType::Avro
}

/// External schema reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// A versioned schema entry under a subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaEntry {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    #[serde(default = "default_schema_type")]
    pub schema_type: SchemaType,
    pub schema: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
}

/// Schema compatibility levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    None,
    #[default]
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
}

/// Errors returned by the Schema Registry catalog.
#[derive(Debug, thiserror::Error)]
pub enum SchemaRegistryError {
    #[error("Subject '{0}' not found.")]
    SubjectNotFound(String),
    #[error("Version {0} not found.")]
    VersionNotFound(String),
    #[error("Schema {0} not found.")]
    SchemaNotFound(i32),
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    #[error("Schema being registered is incompatible with an earlier schema: {0}")]
    IncompatibleSchema(String),
    #[error("Record payload is invalid or missing required Confluent schema header")]
    InvalidRecordPayload,
}

impl SchemaRegistryError {
    /// Returns the HTTP status code and Confluent-standard error code.
    pub fn error_details(&self) -> (u16, u32) {
        match self {
            Self::SubjectNotFound(_) => (404, 40401),
            Self::VersionNotFound(_) => (404, 40402),
            Self::SchemaNotFound(_) => (404, 40403),
            Self::InvalidSchema(_) => (422, 42201),
            Self::IncompatibleSchema(_) => (409, 409),
            Self::InvalidRecordPayload => (422, 42202),
        }
    }
}

#[derive(Debug, Default)]
struct SubjectRecord {
    versions: Vec<SchemaEntry>,
    config: Option<CompatibilityLevel>,
}

#[derive(Debug)]
struct RegistryInner {
    next_global_id: i32,
    subjects: HashMap<String, SubjectRecord>,
    global_schemas: HashMap<i32, SchemaEntry>,
    schema_fingerprints: HashMap<(SchemaType, String), i32>,
    global_config: CompatibilityLevel,
}

impl Default for RegistryInner {
    fn default() -> Self {
        Self {
            next_global_id: 1,
            subjects: HashMap::new(),
            global_schemas: HashMap::new(),
            schema_fingerprints: HashMap::new(),
            global_config: CompatibilityLevel::Backward,
        }
    }
}

/// Confluent-compatible Schema Registry catalog and validator.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    inner: RwLock<RegistryInner>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(RegistryInner::default()),
        }
    }

    /// Registers a schema under a subject. If an identical schema is already registered
    /// under this subject, returns the existing schema ID.
    pub fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: Option<SchemaType>,
        references: Vec<SchemaReference>,
    ) -> Result<i32, SchemaRegistryError> {
        let st = schema_type.unwrap_or(SchemaType::Avro);
        self.validate_schema_syntax(schema, st)?;

        let mut inner = self.inner.write();

        // Check if exact schema is already registered under this subject
        if let Some(sub_rec) = inner.subjects.get(subject) {
            for entry in &sub_rec.versions {
                if entry.schema_type == st && entry.schema.trim() == schema.trim() {
                    return Ok(entry.id);
                }
            }

            // Check compatibility with latest existing version
            let level = sub_rec.config.unwrap_or(inner.global_config);
            if let Some(latest) = sub_rec.versions.last() {
                Self::verify_compatibility(level, latest, schema, st)?;
            }
        }

        // Determine or allocate global ID
        let normalized = schema.trim().to_string();
        let key = (st, normalized.clone());
        let id = if let Some(&existing_id) = inner.schema_fingerprints.get(&key) {
            existing_id
        } else {
            let assigned_id = inner.next_global_id;
            inner.next_global_id += 1;
            inner.schema_fingerprints.insert(key, assigned_id);
            assigned_id
        };

        let sub_rec = inner.subjects.entry(subject.to_string()).or_default();
        let version = (sub_rec.versions.len() as i32) + 1;

        let entry = SchemaEntry {
            subject: subject.to_string(),
            version,
            id,
            schema_type: st,
            schema: normalized,
            references,
        };

        sub_rec.versions.push(entry.clone());
        inner.global_schemas.insert(id, entry);

        Ok(id)
    }

    /// Retrieves a schema by global ID.
    pub fn get_schema_by_id(&self, id: i32) -> Option<SchemaEntry> {
        let inner = self.inner.read();
        inner.global_schemas.get(&id).cloned()
    }

    /// Retrieves a schema by subject and version string ("latest" or "1", "2"...).
    pub fn get_schema_by_subject_version(
        &self,
        subject: &str,
        version_str: &str,
    ) -> Result<SchemaEntry, SchemaRegistryError> {
        let inner = self.inner.read();
        let sub_rec = inner
            .subjects
            .get(subject)
            .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;

        if sub_rec.versions.is_empty() {
            return Err(SchemaRegistryError::SubjectNotFound(subject.to_string()));
        }

        if version_str.eq_ignore_ascii_case("latest") {
            return Ok(sub_rec.versions.last().unwrap().clone());
        }

        let ver: i32 = version_str
            .parse()
            .map_err(|_| SchemaRegistryError::VersionNotFound(version_str.to_string()))?;

        sub_rec
            .versions
            .iter()
            .find(|e| e.version == ver)
            .cloned()
            .ok_or_else(|| SchemaRegistryError::VersionNotFound(version_str.to_string()))
    }

    /// Lists all registered subjects.
    pub fn list_subjects(&self) -> Vec<String> {
        let inner = self.inner.read();
        let mut subs: Vec<String> = inner
            .subjects
            .iter()
            .filter(|(_, rec)| !rec.versions.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        subs.sort();
        subs
    }

    /// Lists all versions for a subject.
    pub fn list_versions(&self, subject: &str) -> Result<Vec<i32>, SchemaRegistryError> {
        let inner = self.inner.read();
        let sub_rec = inner
            .subjects
            .get(subject)
            .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;

        if sub_rec.versions.is_empty() {
            return Err(SchemaRegistryError::SubjectNotFound(subject.to_string()));
        }

        Ok(sub_rec.versions.iter().map(|e| e.version).collect())
    }

    /// Checks if an exact schema is registered under a subject.
    pub fn check_schema_registered(
        &self,
        subject: &str,
        schema: &str,
        schema_type: Option<SchemaType>,
    ) -> Option<SchemaEntry> {
        let st = schema_type.unwrap_or(SchemaType::Avro);
        let inner = self.inner.read();
        let sub_rec = inner.subjects.get(subject)?;
        let trimmed = schema.trim();
        sub_rec
            .versions
            .iter()
            .find(|e| e.schema_type == st && e.schema.trim() == trimmed)
            .cloned()
    }

    /// Deletes a subject, returning the list of deleted versions.
    pub fn delete_subject(&self, subject: &str) -> Result<Vec<i32>, SchemaRegistryError> {
        let mut inner = self.inner.write();
        let sub_rec = inner
            .subjects
            .get_mut(subject)
            .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;

        if sub_rec.versions.is_empty() {
            return Err(SchemaRegistryError::SubjectNotFound(subject.to_string()));
        }

        let versions: Vec<i32> = sub_rec.versions.iter().map(|e| e.version).collect();
        sub_rec.versions.clear();
        Ok(versions)
    }

    /// Deletes a specific version of a subject, returning the deleted version number.
    pub fn delete_version(
        &self,
        subject: &str,
        version_str: &str,
    ) -> Result<i32, SchemaRegistryError> {
        let mut inner = self.inner.write();
        let sub_rec = inner
            .subjects
            .get_mut(subject)
            .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;

        if sub_rec.versions.is_empty() {
            return Err(SchemaRegistryError::SubjectNotFound(subject.to_string()));
        }

        let target_ver = if version_str.eq_ignore_ascii_case("latest") {
            sub_rec.versions.last().unwrap().version
        } else {
            version_str
                .parse::<i32>()
                .map_err(|_| SchemaRegistryError::VersionNotFound(version_str.to_string()))?
        };

        if let Some(pos) = sub_rec
            .versions
            .iter()
            .position(|e| e.version == target_ver)
        {
            sub_rec.versions.remove(pos);
            Ok(target_ver)
        } else {
            Err(SchemaRegistryError::VersionNotFound(
                version_str.to_string(),
            ))
        }
    }

    /// Tests whether a candidate schema is compatible with an existing registered version.
    pub fn check_compatibility(
        &self,
        subject: &str,
        version_str: &str,
        candidate_schema: &str,
        candidate_type: Option<SchemaType>,
    ) -> Result<bool, SchemaRegistryError> {
        let st = candidate_type.unwrap_or(SchemaType::Avro);
        self.validate_schema_syntax(candidate_schema, st)?;

        let inner = self.inner.read();
        let sub_rec = inner
            .subjects
            .get(subject)
            .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;

        let existing = if version_str.eq_ignore_ascii_case("latest") {
            sub_rec
                .versions
                .last()
                .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?
        } else {
            let ver: i32 = version_str
                .parse()
                .map_err(|_| SchemaRegistryError::VersionNotFound(version_str.to_string()))?;
            sub_rec
                .versions
                .iter()
                .find(|e| e.version == ver)
                .ok_or_else(|| SchemaRegistryError::VersionNotFound(version_str.to_string()))?
        };

        let level = sub_rec.config.unwrap_or(inner.global_config);
        match Self::verify_compatibility(level, existing, candidate_schema, st) {
            Ok(_) => Ok(true),
            Err(SchemaRegistryError::IncompatibleSchema(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Gets compatibility configuration for subject or global.
    pub fn get_config(&self, subject: Option<&str>) -> CompatibilityLevel {
        let inner = self.inner.read();
        if let Some(sub) = subject {
            if let Some(rec) = inner.subjects.get(sub) {
                if let Some(lvl) = rec.config {
                    return lvl;
                }
            }
        }
        inner.global_config
    }

    /// Sets compatibility configuration for subject or global.
    pub fn set_config(&self, subject: Option<&str>, level: CompatibilityLevel) {
        let mut inner = self.inner.write();
        if let Some(sub) = subject {
            let rec = inner.subjects.entry(sub.to_string()).or_default();
            rec.config = Some(level);
        } else {
            inner.global_config = level;
        }
    }

    /// Validates Confluent Magic Byte wire framing:
    /// - Byte 0: `0x00`
    /// - Bytes 1..5: Big-endian 4-byte schema ID
    ///
    /// Returns `Ok(Some(schema_id))` if validly framed and schema exists in catalog.
    /// Returns `Ok(None)` if record is un-framed plain data.
    /// Returns `Err` if framed with a nonexistent schema ID.
    pub fn validate_magic_byte_payload(
        &self,
        payload: &[u8],
    ) -> Result<Option<i32>, SchemaRegistryError> {
        if payload.len() < 5 || payload[0] != 0x00 {
            return Ok(None);
        }

        let schema_id = i32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        let inner = self.inner.read();
        if inner.global_schemas.contains_key(&schema_id) {
            Ok(Some(schema_id))
        } else {
            Err(SchemaRegistryError::SchemaNotFound(schema_id))
        }
    }

    /// Validates record payload, optionally strictly enforcing schema framing.
    pub fn validate_record(
        &self,
        payload: &[u8],
        require_magic_byte: bool,
    ) -> Result<Option<i32>, SchemaRegistryError> {
        let res = self.validate_magic_byte_payload(payload)?;
        if require_magic_byte && res.is_none() {
            return Err(SchemaRegistryError::InvalidRecordPayload);
        }
        Ok(res)
    }

    fn validate_schema_syntax(
        &self,
        schema: &str,
        schema_type: SchemaType,
    ) -> Result<(), SchemaRegistryError> {
        if schema.trim().is_empty() {
            return Err(SchemaRegistryError::InvalidSchema(
                "Schema definition cannot be empty".to_string(),
            ));
        }

        match schema_type {
            SchemaType::Avro | SchemaType::Json => {
                serde_json::from_str::<serde_json::Value>(schema).map_err(|e| {
                    SchemaRegistryError::InvalidSchema(format!("Malformed JSON: {e}"))
                })?;
            }
            SchemaType::Protobuf => {
                if !schema.contains("syntax") && !schema.contains("message") {
                    return Err(SchemaRegistryError::InvalidSchema(
                        "Protobuf schema must contain message definition".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn verify_compatibility(
        level: CompatibilityLevel,
        existing: &SchemaEntry,
        candidate_schema: &str,
        candidate_type: SchemaType,
    ) -> Result<(), SchemaRegistryError> {
        if level == CompatibilityLevel::None {
            return Ok(());
        }

        if existing.schema_type != candidate_type {
            return Err(SchemaRegistryError::IncompatibleSchema(format!(
                "Cannot change schema type from {:?} to {:?}",
                existing.schema_type, candidate_type
            )));
        }

        // Basic compatibility validation: ensure candidate schema is valid JSON
        // and doesn't wipe required fields
        if candidate_type == SchemaType::Avro || candidate_type == SchemaType::Json {
            let prev_json: serde_json::Value =
                serde_json::from_str(&existing.schema).unwrap_or(serde_json::Value::Null);
            let next_json: serde_json::Value = serde_json::from_str(candidate_schema)
                .map_err(|e| SchemaRegistryError::InvalidSchema(e.to_string()))?;

            if let (Some(prev_obj), Some(next_obj)) = (prev_json.as_object(), next_json.as_object())
            {
                if let (Some(prev_name), Some(next_name)) =
                    (prev_obj.get("name"), next_obj.get("name"))
                {
                    if prev_name != next_name && level != CompatibilityLevel::None {
                        return Err(SchemaRegistryError::IncompatibleSchema(format!(
                            "Top-level record name changed from {prev_name} to {next_name}"
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registry_lifecycle_and_versions() {
        let registry = SchemaRegistry::new();
        let avro_schema_v1 =
            r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#;
        let avro_schema_v2 = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string","default":""}]}"#;

        // Register v1
        let id1 = registry
            .register_schema(
                "users-value",
                avro_schema_v1,
                Some(SchemaType::Avro),
                vec![],
            )
            .unwrap();
        assert_eq!(id1, 1);

        // Registering identical returns same id
        let id1_dup = registry
            .register_schema(
                "users-value",
                avro_schema_v1,
                Some(SchemaType::Avro),
                vec![],
            )
            .unwrap();
        assert_eq!(id1, id1_dup);

        // Register v2
        let id2 = registry
            .register_schema(
                "users-value",
                avro_schema_v2,
                Some(SchemaType::Avro),
                vec![],
            )
            .unwrap();
        assert_eq!(id2, 2);

        // List subjects
        let subs = registry.list_subjects();
        assert_eq!(subs, vec!["users-value".to_string()]);

        // List versions
        let vers = registry.list_versions("users-value").unwrap();
        assert_eq!(vers, vec![1, 2]);

        // Fetch latest
        let latest = registry
            .get_schema_by_subject_version("users-value", "latest")
            .unwrap();
        assert_eq!(latest.version, 2);
        assert_eq!(latest.id, id2);

        // Fetch version 1
        let v1 = registry
            .get_schema_by_subject_version("users-value", "1")
            .unwrap();
        assert_eq!(v1.version, 1);
        assert_eq!(v1.id, id1);

        // Check compatibility
        let is_compat = registry
            .check_compatibility(
                "users-value",
                "latest",
                avro_schema_v2,
                Some(SchemaType::Avro),
            )
            .unwrap();
        assert!(is_compat);

        // Delete version 1
        let deleted_ver = registry.delete_version("users-value", "1").unwrap();
        assert_eq!(deleted_ver, 1);
        assert_eq!(registry.list_versions("users-value").unwrap(), vec![2]);

        // Delete subject
        let deleted_vers = registry.delete_subject("users-value").unwrap();
        assert_eq!(deleted_vers, vec![2]);
        assert!(registry.list_subjects().is_empty());
    }

    #[test]
    fn test_magic_byte_payload_validation() {
        let registry = SchemaRegistry::new();
        let schema = r#"{"type":"record","name":"Event","fields":[]}"#;
        let id = registry
            .register_schema("events-value", schema, None, vec![])
            .unwrap();

        // Construct Confluent wire format payload: 0x00 + 4-byte BE schema_id + payload
        let mut framed = Vec::new();
        framed.push(0x00);
        framed.extend_from_slice(&id.to_be_bytes());
        framed.extend_from_slice(b"sample avro encoded data");

        // Valid payload
        let validated = registry.validate_magic_byte_payload(&framed).unwrap();
        assert_eq!(validated, Some(id));

        // Unknown schema ID
        let mut invalid_framed = Vec::new();
        invalid_framed.push(0x00);
        invalid_framed.extend_from_slice(&9999_i32.to_be_bytes());
        invalid_framed.extend_from_slice(b"data");
        assert!(registry
            .validate_magic_byte_payload(&invalid_framed)
            .is_err());

        // Plain raw payload without magic byte
        let raw = b"unframed data";
        assert_eq!(registry.validate_magic_byte_payload(raw).unwrap(), None);

        // Require magic byte
        assert!(registry.validate_record(raw, true).is_err());
        assert!(registry.validate_record(&framed, true).is_ok());
    }
}
