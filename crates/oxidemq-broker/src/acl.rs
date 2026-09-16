use oxidemq_protocol::error_code::KafkaErrorCode;
use oxidemq_protocol::messages::{
    AclCreation, AclCreationResult, AclDescription, AclOperation, AclPermissionType,
    AclResourcePatternType, AclResourceType, DeleteAclsFilter, DeleteAclsFilterResult,
    DeleteAclsMatchingAcl, DescribeAclsRequest, DescribeAclsResource,
};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::info;

/// A single Access Control Entry binding principal, host, operation, and permission to a resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclBinding {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclResourcePatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: AclPermissionType,
}

/// Thread-safe in-memory Kafka ACL authorization engine.
#[derive(Debug, Clone)]
pub struct AclAuthorizer {
    enabled: bool,
    super_users: HashSet<String>,
    allow_everyone_if_no_acl_found: bool,
    bindings: Arc<RwLock<Vec<AclBinding>>>,
}

impl Default for AclAuthorizer {
    fn default() -> Self {
        let mut super_users = HashSet::new();
        super_users.insert("User:admin".to_string());
        Self::new(false, super_users, true)
    }
}

impl AclAuthorizer {
    /// Creates a new AclAuthorizer instance.
    pub fn new(
        enabled: bool,
        super_users: HashSet<String>,
        allow_everyone_if_no_acl_found: bool,
    ) -> Self {
        let normalized_super_users = super_users
            .into_iter()
            .map(|u| {
                if !u.contains(':') {
                    format!("User:{}", u)
                } else {
                    u
                }
            })
            .collect();

        Self {
            enabled,
            super_users: normalized_super_users,
            allow_everyone_if_no_acl_found,
            bindings: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Evaluates whether a principal has permission to perform an operation on a resource from a client host.
    pub fn authorize(
        &self,
        principal: &str,
        host: &str,
        resource_type: AclResourceType,
        resource_name: &str,
        operation: AclOperation,
    ) -> bool {
        if !self.enabled {
            return true;
        }

        let norm_principal = if !principal.contains(':') {
            format!("User:{}", principal)
        } else {
            principal.to_string()
        };

        // Super users bypass all ACL checks
        if self.super_users.contains(&norm_principal) || self.super_users.contains(principal) {
            return true;
        }

        let bindings = self.bindings.read();

        // Check if there are any ACLs defined for this resource at all
        let has_any_rules_for_resource = bindings.iter().any(|b| {
            if b.resource_type != resource_type {
                return false;
            }
            match b.pattern_type {
                AclResourcePatternType::Literal => {
                    b.resource_name == resource_name || b.resource_name == "*"
                }
                AclResourcePatternType::Prefixed => resource_name.starts_with(&b.resource_name),
                _ => b.resource_name == resource_name || b.resource_name == "*",
            }
        });

        if !has_any_rules_for_resource {
            return self.allow_everyone_if_no_acl_found;
        }

        // Find matching rules for this (principal, host, operation)
        let mut matched_allows = false;
        for b in bindings.iter() {
            if b.resource_type != resource_type {
                continue;
            }

            // Resource pattern match
            let resource_matches = match b.pattern_type {
                AclResourcePatternType::Literal => {
                    b.resource_name == resource_name || b.resource_name == "*"
                }
                AclResourcePatternType::Prefixed => resource_name.starts_with(&b.resource_name),
                _ => b.resource_name == resource_name || b.resource_name == "*",
            };
            if !resource_matches {
                continue;
            }

            // Principal match
            let principal_matches =
                b.principal == norm_principal || b.principal == "User:*" || b.principal == "*";
            if !principal_matches {
                continue;
            }

            // Host match
            let host_matches = b.host == host || b.host == "*";
            if !host_matches {
                continue;
            }

            // Operation match
            let op_matches = b.operation == operation || b.operation == AclOperation::All;
            if !op_matches {
                continue;
            }

            // Deny takes absolute precedence
            if b.permission_type == AclPermissionType::Deny {
                return false;
            }

            if b.permission_type == AclPermissionType::Allow {
                matched_allows = true;
            }
        }

        matched_allows
    }

    /// Creates ACL bindings on the broker.
    pub fn create_acls(&self, creations: &[AclCreation]) -> Vec<AclCreationResult> {
        let mut results = Vec::with_capacity(creations.len());
        let mut bindings = self.bindings.write();

        for c in creations {
            let res_type = AclResourceType::from_i8(c.resource_type);
            let pattern_type = AclResourcePatternType::from_i8(c.resource_pattern_type);
            let operation = AclOperation::from_i8(c.operation);
            let permission = AclPermissionType::from_i8(c.permission_type);

            if res_type == AclResourceType::Unknown
                || pattern_type == AclResourcePatternType::Unknown
                || operation == AclOperation::Unknown
                || permission == AclPermissionType::Unknown
            {
                results.push(AclCreationResult {
                    error_code: KafkaErrorCode::InvalidRequest,
                    error_message: Some("Invalid ACL creation parameters".to_string()),
                });
                continue;
            }

            let principal = if !c.principal.contains(':') {
                format!("User:{}", c.principal)
            } else {
                c.principal.clone()
            };

            let binding = AclBinding {
                resource_type: res_type,
                resource_name: c.resource_name.clone(),
                pattern_type,
                principal,
                host: if c.host.is_empty() {
                    "*".to_string()
                } else {
                    c.host.clone()
                },
                operation,
                permission_type: permission,
            };

            if !bindings.contains(&binding) {
                info!(
                    "Created ACL: {:?} on {:?} '{}' for {} from {} (perm: {:?})",
                    binding.operation,
                    binding.resource_type,
                    binding.resource_name,
                    binding.principal,
                    binding.host,
                    binding.permission_type
                );
                bindings.push(binding);
            }

            results.push(AclCreationResult {
                error_code: KafkaErrorCode::None,
                error_message: None,
            });
        }

        results
    }

    /// Describes ACL bindings matching the filter.
    pub fn describe_acls(&self, req: &DescribeAclsRequest) -> Vec<DescribeAclsResource> {
        let bindings = self.bindings.read();
        let mut grouped: HashMap<(i8, String, i8), Vec<AclDescription>> = HashMap::new();

        for b in bindings.iter() {
            // Filter resource type
            if req.resource_type_filter != 1 && b.resource_type as i8 != req.resource_type_filter {
                continue;
            }

            // Filter resource name
            if let Some(ref name_filter) = req.resource_name_filter {
                let matches_name = match req.resource_pattern_type_filter {
                    1 | 2 => {
                        // Any or Match
                        b.resource_name == *name_filter
                            || name_filter.starts_with(&b.resource_name)
                            || b.resource_name == "*"
                    }
                    3 => b.resource_name == *name_filter, // Literal
                    4 => name_filter.starts_with(&b.resource_name), // Prefixed
                    _ => b.resource_name == *name_filter,
                };
                if !matches_name {
                    continue;
                }
            }

            // Filter principal
            if let Some(ref principal_filter) = req.principal_filter {
                let norm = if !principal_filter.contains(':') && principal_filter != "*" {
                    format!("User:{}", principal_filter)
                } else {
                    principal_filter.clone()
                };
                if principal_filter != "*"
                    && b.principal != norm
                    && b.principal != *principal_filter
                {
                    continue;
                }
            }

            // Filter host
            if let Some(ref host_filter) = req.host_filter {
                if host_filter != "*" && b.host != *host_filter {
                    continue;
                }
            }

            // Filter operation
            if req.operation != 1 && b.operation as i8 != req.operation {
                continue;
            }

            // Filter permission type
            if req.permission_type != 1 && b.permission_type as i8 != req.permission_type {
                continue;
            }

            let key = (
                b.resource_type as i8,
                b.resource_name.clone(),
                b.pattern_type as i8,
            );
            grouped.entry(key).or_default().push(AclDescription {
                principal: b.principal.clone(),
                host: b.host.clone(),
                operation: b.operation as i8,
                permission_type: b.permission_type as i8,
            });
        }

        grouped
            .into_iter()
            .map(
                |((resource_type, resource_name, resource_pattern_type), acls)| {
                    DescribeAclsResource {
                        resource_type,
                        resource_name,
                        resource_pattern_type,
                        acls,
                    }
                },
            )
            .collect()
    }

    /// Deletes ACL bindings matching the filters.
    pub fn delete_acls(&self, filters: &[DeleteAclsFilter]) -> Vec<DeleteAclsFilterResult> {
        let mut results = Vec::with_capacity(filters.len());
        let mut bindings = self.bindings.write();

        for filter in filters {
            let mut matching_acls = Vec::new();
            bindings.retain(|b| {
                // Check if binding matches filter
                if filter.resource_type_filter != 1
                    && b.resource_type as i8 != filter.resource_type_filter
                {
                    return true;
                }

                if let Some(ref name_filter) = filter.resource_name_filter {
                    let matches_name = match filter.resource_pattern_type_filter {
                        1 | 2 => {
                            b.resource_name == *name_filter
                                || name_filter.starts_with(&b.resource_name)
                                || b.resource_name == "*"
                        }
                        3 => b.resource_name == *name_filter,
                        4 => name_filter.starts_with(&b.resource_name),
                        _ => b.resource_name == *name_filter,
                    };
                    if !matches_name {
                        return true;
                    }
                }

                if let Some(ref principal_filter) = filter.principal_filter {
                    let norm = if !principal_filter.contains(':') && principal_filter != "*" {
                        format!("User:{}", principal_filter)
                    } else {
                        principal_filter.clone()
                    };
                    if principal_filter != "*"
                        && b.principal != norm
                        && b.principal != *principal_filter
                    {
                        return true;
                    }
                }

                if let Some(ref host_filter) = filter.host_filter {
                    if host_filter != "*" && b.host != *host_filter {
                        return true;
                    }
                }

                if filter.operation != 1 && b.operation as i8 != filter.operation {
                    return true;
                }

                if filter.permission_type != 1 && b.permission_type as i8 != filter.permission_type
                {
                    return true;
                }

                // If all matched, remove it and record in matching_acls
                matching_acls.push(DeleteAclsMatchingAcl {
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                    resource_type: b.resource_type as i8,
                    resource_name: b.resource_name.clone(),
                    resource_pattern_type: b.pattern_type as i8,
                    principal: b.principal.clone(),
                    host: b.host.clone(),
                    operation: b.operation as i8,
                    permission_type: b.permission_type as i8,
                });
                false
            });

            results.push(DeleteAclsFilterResult {
                error_code: KafkaErrorCode::None,
                error_message: None,
                matching_acls,
            });
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_authorizer_disabled_allows_all() {
        let authorizer = AclAuthorizer::default(); // enabled = false
        assert!(authorizer.authorize(
            "User:alice",
            "127.0.0.1",
            AclResourceType::Topic,
            "secret-topic",
            AclOperation::Write
        ));
    }

    #[test]
    fn test_acl_authorizer_super_user_bypass() {
        let mut super_users = HashSet::new();
        super_users.insert("User:admin".to_string());
        let authorizer = AclAuthorizer::new(true, super_users, false);

        assert!(authorizer.authorize(
            "admin",
            "127.0.0.1",
            AclResourceType::Topic,
            "secret-topic",
            AclOperation::Write
        ));
        assert!(authorizer.authorize(
            "User:admin",
            "127.0.0.1",
            AclResourceType::Cluster,
            "cluster",
            AclOperation::All
        ));
    }

    #[test]
    fn test_acl_authorizer_allow_and_deny_precedence() {
        let authorizer = AclAuthorizer::new(true, HashSet::new(), false);

        // Allow Alice to read topic "orders"
        let creations = vec![AclCreation {
            resource_type: AclResourceType::Topic as i8,
            resource_name: "orders".into(),
            resource_pattern_type: AclResourcePatternType::Literal as i8,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read as i8,
            permission_type: AclPermissionType::Allow as i8,
        }];
        let res = authorizer.create_acls(&creations);
        assert_eq!(res[0].error_code, KafkaErrorCode::None);

        // Alice can Read orders
        assert!(authorizer.authorize(
            "User:alice",
            "127.0.0.1",
            AclResourceType::Topic,
            "orders",
            AclOperation::Read
        ));
        // Alice cannot Write orders
        assert!(!authorizer.authorize(
            "User:alice",
            "127.0.0.1",
            AclResourceType::Topic,
            "orders",
            AclOperation::Write
        ));
        // Bob cannot Read orders
        assert!(!authorizer.authorize(
            "User:bob",
            "127.0.0.1",
            AclResourceType::Topic,
            "orders",
            AclOperation::Read
        ));

        // Now add a Deny for Alice on orders
        let creations = vec![AclCreation {
            resource_type: AclResourceType::Topic as i8,
            resource_name: "orders".into(),
            resource_pattern_type: AclResourcePatternType::Literal as i8,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read as i8,
            permission_type: AclPermissionType::Deny as i8,
        }];
        authorizer.create_acls(&creations);

        // Deny takes precedence: Alice is now rejected
        assert!(!authorizer.authorize(
            "User:alice",
            "127.0.0.1",
            AclResourceType::Topic,
            "orders",
            AclOperation::Read
        ));
    }

    #[test]
    fn test_acl_prefixed_and_wildcard_matching() {
        let authorizer = AclAuthorizer::new(true, HashSet::new(), false);

        // Allow any user to read topics prefixed with "public-"
        let creations = vec![AclCreation {
            resource_type: AclResourceType::Topic as i8,
            resource_name: "public-".into(),
            resource_pattern_type: AclResourcePatternType::Prefixed as i8,
            principal: "User:*".into(),
            host: "*".into(),
            operation: AclOperation::Read as i8,
            permission_type: AclPermissionType::Allow as i8,
        }];
        authorizer.create_acls(&creations);

        assert!(authorizer.authorize(
            "User:anyone",
            "127.0.0.1",
            AclResourceType::Topic,
            "public-events",
            AclOperation::Read
        ));
        assert!(authorizer.authorize(
            "User:bob",
            "127.0.0.1",
            AclResourceType::Topic,
            "public-metrics",
            AclOperation::Read
        ));
        // Non-matching prefix fails
        assert!(!authorizer.authorize(
            "User:anyone",
            "127.0.0.1",
            AclResourceType::Topic,
            "private-events",
            AclOperation::Read
        ));
    }

    #[test]
    fn test_acl_describe_and_delete() {
        let authorizer = AclAuthorizer::new(true, HashSet::new(), false);
        authorizer.create_acls(&[
            AclCreation {
                resource_type: AclResourceType::Topic as i8,
                resource_name: "events".into(),
                resource_pattern_type: AclResourcePatternType::Literal as i8,
                principal: "User:charlie".into(),
                host: "*".into(),
                operation: AclOperation::All as i8,
                permission_type: AclPermissionType::Allow as i8,
            },
            AclCreation {
                resource_type: AclResourceType::Group as i8,
                resource_name: "analytics-group".into(),
                resource_pattern_type: AclResourcePatternType::Literal as i8,
                principal: "User:charlie".into(),
                host: "*".into(),
                operation: AclOperation::Read as i8,
                permission_type: AclPermissionType::Allow as i8,
            },
        ]);

        // Describe topic ACLs
        let desc = authorizer.describe_acls(&DescribeAclsRequest {
            resource_type_filter: AclResourceType::Topic as i8,
            resource_name_filter: None,
            resource_pattern_type_filter: 1, // Any
            principal_filter: None,
            host_filter: None,
            operation: 1,       // Any
            permission_type: 1, // Any
        });
        assert_eq!(desc.len(), 1);
        assert_eq!(desc[0].resource_name, "events");

        // Delete group ACL
        let del = authorizer.delete_acls(&[DeleteAclsFilter {
            resource_type_filter: AclResourceType::Group as i8,
            resource_name_filter: Some("analytics-group".into()),
            resource_pattern_type_filter: 1,
            principal_filter: None,
            host_filter: None,
            operation: 1,
            permission_type: 1,
        }]);
        assert_eq!(del.len(), 1);
        assert_eq!(del[0].matching_acls.len(), 1);
        assert_eq!(del[0].matching_acls[0].resource_name, "analytics-group");

        // Confirm group ACL is gone
        let desc_group = authorizer.describe_acls(&DescribeAclsRequest {
            resource_type_filter: AclResourceType::Group as i8,
            resource_name_filter: None,
            resource_pattern_type_filter: 1,
            principal_filter: None,
            host_filter: None,
            operation: 1,
            permission_type: 1,
        });
        assert_eq!(desc_group.len(), 0);
    }
}
