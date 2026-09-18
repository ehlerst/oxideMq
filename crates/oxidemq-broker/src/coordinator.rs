use bytes::Bytes;
use oxidemq_core::types::TopicPartition;
use oxidemq_protocol::{DescribedGroup, DescribedGroupMember, KafkaErrorCode};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub protocol_metadata: Vec<u8>,
    pub assignment: Vec<u8>,
}

pub struct ConsumerGroup {
    pub group_id: String,
    pub state: GroupState,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub protocol_type: String,
    pub protocol_name: Option<String>,
    pub members: HashMap<String, GroupMember>,
    pub offsets: HashMap<TopicPartition, i64>,
}

impl ConsumerGroup {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            state: GroupState::Empty,
            generation_id: 0,
            leader_id: None,
            protocol_type: String::new(),
            protocol_name: None,
            members: HashMap::new(),
            offsets: HashMap::new(),
        }
    }
}

/// Pure-Rust consumer group coordinator handling rebalances, heartbeats, and committed offsets.
#[derive(Default)]
pub struct GroupCoordinator {
    groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    next_member_counter: AtomicI32,
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            next_member_counter: AtomicI32::new(1),
        }
    }

    /// Handles JoinGroup requests. Assigns member ID, determines group leader, and transitions state.
    pub fn handle_join_group(
        &self,
        group_id: &str,
        member_id: &str,
        client_id: &str,
        protocol_type: &str,
    ) -> (KafkaErrorCode, i32, String, String) {
        let mut groups = self.groups.write();
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id));

        let actual_member_id = if member_id.is_empty() {
            let id_num = self.next_member_counter.fetch_add(1, Ordering::Relaxed);
            format!("{}-{:08}", client_id, id_num)
        } else {
            member_id.to_string()
        };

        if group.members.is_empty() {
            group.leader_id = Some(actual_member_id.clone());
            group.protocol_type = protocol_type.to_string();
        }

        group.members.insert(
            actual_member_id.clone(),
            GroupMember {
                member_id: actual_member_id.clone(),
                client_id: client_id.to_string(),
                client_host: "127.0.0.1".to_string(),
                protocol_metadata: Vec::new(),
                assignment: Vec::new(),
            },
        );

        group.generation_id += 1;
        group.state = GroupState::CompletingRebalance;

        let leader_id = group.leader_id.clone().unwrap_or_default();
        let gen_id = group.generation_id;

        (KafkaErrorCode::None, gen_id, actual_member_id, leader_id)
    }

    /// Handles SyncGroup requests. Distributes partition assignments computed by group leader.
    pub fn handle_sync_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        assignments: HashMap<String, Vec<u8>>,
    ) -> (KafkaErrorCode, Vec<u8>) {
        let mut groups = self.groups.write();
        let group = match groups.get_mut(group_id) {
            Some(g) => g,
            None => return (KafkaErrorCode::InvalidGroupId, Vec::new()),
        };

        if group.generation_id != generation_id {
            return (KafkaErrorCode::IllegalGeneration, Vec::new());
        }

        if !assignments.is_empty() {
            for (m_id, assign) in assignments {
                if let Some(m) = group.members.get_mut(&m_id) {
                    m.assignment = assign;
                }
            }
            group.state = GroupState::Stable;
        }

        let member_assign = group
            .members
            .get(member_id)
            .map(|m| m.assignment.clone())
            .unwrap_or_default();

        (KafkaErrorCode::None, member_assign)
    }

    /// Handles Heartbeat requests.
    pub fn handle_heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
    ) -> KafkaErrorCode {
        let groups = self.groups.read();
        let group = match groups.get(group_id) {
            Some(g) => g,
            None => return KafkaErrorCode::InvalidGroupId,
        };

        if group.generation_id != generation_id {
            return KafkaErrorCode::IllegalGeneration;
        }

        if !group.members.contains_key(member_id) {
            return KafkaErrorCode::UnknownMemberId;
        }

        KafkaErrorCode::None
    }

    /// Handles LeaveGroup requests.
    pub fn handle_leave_group(&self, group_id: &str, member_id: &str) -> KafkaErrorCode {
        let mut groups = self.groups.write();
        if let Some(group) = groups.get_mut(group_id) {
            group.members.remove(member_id);
            if group.members.is_empty() {
                group.state = GroupState::Empty;
                group.leader_id = None;
            } else if group.leader_id.as_deref() == Some(member_id) {
                // Elect new leader
                group.leader_id = group.members.keys().next().cloned();
                group.state = GroupState::PreparingRebalance;
            }
            KafkaErrorCode::None
        } else {
            KafkaErrorCode::InvalidGroupId
        }
    }

    /// Commits consumer offset for a topic partition.
    pub fn commit_offset(&self, group_id: &str, tp: TopicPartition, offset: i64) {
        let mut groups = self.groups.write();
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id));
        group.offsets.insert(tp, offset);
    }

    /// Retrieves committed consumer offset for a topic partition.
    pub fn fetch_offset(&self, group_id: &str, tp: &TopicPartition) -> Option<i64> {
        let groups = self.groups.read();
        groups
            .get(group_id)
            .and_then(|g| g.offsets.get(tp).copied())
    }

    pub fn reset(&self) {
        self.groups.write().clear();
    }

    pub fn group_count(&self) -> usize {
        self.groups.read().len()
    }

    pub fn dump_group_snapshots(&self) -> Vec<crate::state::ConsumerGroupSnapshot> {
        let groups = self.groups.read();
        let mut snapshots = Vec::with_capacity(groups.len());
        for (gid, g) in groups.iter() {
            let mut offsets = HashMap::new();
            for (tp, off) in &g.offsets {
                offsets.insert(format!("{}:{}", tp.topic, tp.partition), *off);
            }
            snapshots.push(crate::state::ConsumerGroupSnapshot {
                group_id: gid.clone(),
                state: format!("{:?}", g.state),
                generation_id: g.generation_id,
                leader_id: g.leader_id.clone(),
                offsets,
            });
        }
        snapshots
    }

    /// Lists all active consumer groups and their protocol types.
    pub fn list_groups(&self) -> Vec<(String, String)> {
        let groups = self.groups.read();
        let mut list = Vec::with_capacity(groups.len());
        for (gid, g) in groups.iter() {
            list.push((gid.clone(), g.protocol_type.clone()));
        }
        list
    }

    /// Describes metadata, state, and members for the requested groups.
    pub fn describe_groups(&self, group_ids: &[String]) -> Vec<DescribedGroup> {
        let groups = self.groups.read();
        let mut result = Vec::with_capacity(group_ids.len());

        for gid in group_ids {
            if let Some(g) = groups.get(gid) {
                let state_str = match g.state {
                    GroupState::Empty => "Empty",
                    GroupState::PreparingRebalance => "PreparingRebalance",
                    GroupState::CompletingRebalance => "CompletingRebalance",
                    GroupState::Stable => "Stable",
                    GroupState::Dead => "Dead",
                };
                let members = g
                    .members
                    .values()
                    .map(|m| DescribedGroupMember {
                        member_id: m.member_id.clone(),
                        client_id: m.client_id.clone(),
                        client_host: m.client_host.clone(),
                        member_metadata: Bytes::copy_from_slice(&m.protocol_metadata),
                        member_assignment: Bytes::copy_from_slice(&m.assignment),
                    })
                    .collect();

                result.push(DescribedGroup {
                    error_code: KafkaErrorCode::None,
                    group_id: gid.clone(),
                    group_state: state_str.to_string(),
                    protocol_type: g.protocol_type.clone(),
                    protocol_data: g.protocol_name.clone().unwrap_or_default(),
                    members,
                });
            } else {
                result.push(DescribedGroup {
                    error_code: KafkaErrorCode::None,
                    group_id: gid.clone(),
                    group_state: "Dead".to_string(),
                    protocol_type: String::new(),
                    protocol_data: String::new(),
                    members: Vec::new(),
                });
            }
        }
        result
    }

    /// Deletes consumer groups. Only groups in Empty or Dead state can be deleted.
    pub fn delete_groups(&self, group_ids: &[String]) -> Vec<(String, KafkaErrorCode)> {
        let mut groups = self.groups.write();
        let mut results = Vec::with_capacity(group_ids.len());

        for gid in group_ids {
            match groups.get(gid) {
                None => {
                    results.push((gid.clone(), KafkaErrorCode::GroupIdNotFound));
                }
                Some(group) => {
                    if !group.members.is_empty()
                        && group.state != GroupState::Empty
                        && group.state != GroupState::Dead
                    {
                        results.push((gid.clone(), KafkaErrorCode::NonEmptyGroup));
                    } else {
                        groups.remove(gid);
                        results.push((gid.clone(), KafkaErrorCode::None));
                    }
                }
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_lifecycle() {
        let coord = GroupCoordinator::new();

        // 1. JoinGroup
        let (err, gen, member_id, leader_id) =
            coord.handle_join_group("analytics-group", "", "client-1", "consumer");
        assert_eq!(err, KafkaErrorCode::None);
        assert_eq!(gen, 1);
        assert_eq!(member_id, leader_id);

        // 2. SyncGroup with assignment
        let mut assignments = HashMap::new();
        assignments.insert(member_id.clone(), vec![0x10, 0x20]);
        let (s_err, assign) =
            coord.handle_sync_group("analytics-group", gen, &member_id, assignments);
        assert_eq!(s_err, KafkaErrorCode::None);
        assert_eq!(assign, vec![0x10, 0x20]);

        // 3. Heartbeat
        let hb_err = coord.handle_heartbeat("analytics-group", gen, &member_id);
        assert_eq!(hb_err, KafkaErrorCode::None);

        // 4. Commit & Fetch offset
        let tp = TopicPartition::new("events", 0);
        coord.commit_offset("analytics-group", tp.clone(), 150);
        assert_eq!(coord.fetch_offset("analytics-group", &tp), Some(150));

        // 5. Leave group
        let leave_err = coord.handle_leave_group("analytics-group", &member_id);
        assert_eq!(leave_err, KafkaErrorCode::None);

        // Error code checks
        assert_eq!(
            coord.handle_heartbeat("unknown-group", gen, &member_id),
            KafkaErrorCode::InvalidGroupId
        );
        assert_eq!(
            coord.handle_heartbeat("analytics-group", 999, &member_id),
            KafkaErrorCode::IllegalGeneration
        );
        assert_eq!(
            coord.handle_heartbeat("analytics-group", gen, "non-member"),
            KafkaErrorCode::UnknownMemberId
        );
        assert_eq!(
            coord.handle_leave_group("unknown-group", &member_id),
            KafkaErrorCode::InvalidGroupId
        );
        let (sync_err, _) =
            coord.handle_sync_group("unknown-group", gen, &member_id, HashMap::new());
        assert_eq!(sync_err, KafkaErrorCode::InvalidGroupId);
        assert_eq!(coord.fetch_offset("unknown-group", &tp), None);

        // Group snapshots and count
        assert_eq!(coord.group_count(), 1);
        let snaps = coord.dump_group_snapshots();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].group_id, "analytics-group");

        coord.reset();
        assert_eq!(coord.group_count(), 0);
    }

    #[test]
    fn test_groups_admin_coordinator() {
        let coord = GroupCoordinator::new();

        // Initially no groups
        assert_eq!(coord.list_groups().len(), 0);

        // Describe non-existent group returns Dead
        let desc_non = coord.describe_groups(&["ghost-group".to_string()]);
        assert_eq!(desc_non.len(), 1);
        assert_eq!(desc_non[0].group_id, "ghost-group");
        assert_eq!(desc_non[0].group_state, "Dead");
        assert_eq!(desc_non[0].members.len(), 0);

        // Join group
        let (_, gen, member_id, _) =
            coord.handle_join_group("admin-test-group", "", "client-x", "consumer");
        let mut assignments = HashMap::new();
        assignments.insert(member_id.clone(), vec![1, 2, 3]);
        let _ = coord.handle_sync_group("admin-test-group", gen, &member_id, assignments);

        // List groups contains admin-test-group
        let list = coord.list_groups();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "admin-test-group");
        assert_eq!(list[0].1, "consumer");

        // Describe groups
        let desc = coord.describe_groups(&["admin-test-group".to_string()]);
        assert_eq!(desc.len(), 1);
        assert_eq!(desc[0].group_id, "admin-test-group");
        assert_eq!(desc[0].group_state, "Stable");
        assert_eq!(desc[0].members.len(), 1);
        assert_eq!(desc[0].members[0].member_id, member_id);
        assert_eq!(desc[0].members[0].member_assignment, vec![1, 2, 3]);

        // Attempt delete while group has active members -> NonEmptyGroup
        let del_res = coord.delete_groups(&["admin-test-group".to_string()]);
        assert_eq!(del_res.len(), 1);
        assert_eq!(del_res[0].1, KafkaErrorCode::NonEmptyGroup);

        // Member leaves group -> becomes Empty
        let _ = coord.handle_leave_group("admin-test-group", &member_id);
        let desc_empty = coord.describe_groups(&["admin-test-group".to_string()]);
        assert_eq!(desc_empty[0].group_state, "Empty");

        // Delete empty group -> None (success)
        let del_res2 = coord.delete_groups(&["admin-test-group".to_string()]);
        assert_eq!(del_res2.len(), 1);
        assert_eq!(del_res2[0].1, KafkaErrorCode::None);

        // Subsequent delete -> GroupIdNotFound
        let del_res3 = coord.delete_groups(&["admin-test-group".to_string()]);
        assert_eq!(del_res3.len(), 1);
        assert_eq!(del_res3[0].1, KafkaErrorCode::GroupIdNotFound);
    }
}
