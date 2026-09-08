use crate::ui::UI_HTML;
use axum::extract::State;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use oxidemq_broker::chaos::{ChaosEngine, ChaosRule};
use oxidemq_broker::coordinator::GroupCoordinator;
use oxidemq_broker::router::ClusterState;
use oxidemq_broker::state::ClusterStateSnapshot;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;

/// Shared application state for HTTP admin and web console routes.
#[derive(Clone)]
pub struct AppState {
    pub cluster_state: Arc<ClusterState>,
    pub coordinator: Arc<GroupCoordinator>,
    pub chaos: Arc<ChaosEngine>,
    pub start_time: Instant,
}

pub fn create_admin_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui_handler))
        .route("/ui", get(ui_handler))
        .route("/_oxidemq/health", get(health_handler))
        .route("/_oxidemq/version", get(version_handler))
        .route("/_oxidemq/status", get(status_handler))
        .route("/_oxidemq/state/dump", get(dump_state_handler))
        .route("/_oxidemq/state/reset", post(reset_state_handler))
        .route("/_oxidemq/state/load", post(load_state_handler))
        .route(
            "/_oxidemq/chaos/rules",
            get(list_chaos_handler).post(add_chaos_handler),
        )
        .route("/_oxidemq/chaos/clear", post(clear_chaos_handler))
        .route("/_oxidemq/advertised", post(update_advertised_handler))
        .with_state(state)
}

async fn ui_handler() -> Html<&'static str> {
    Html(UI_HTML)
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn version_handler() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

async fn status_handler(State(state): State<AppState>) -> Json<Value> {
    let snapshot = ClusterStateSnapshot::capture(&state.cluster_state, &state.coordinator);
    let chaos_rules = state.chaos.list_rules();
    let uptime_secs = state.start_time.elapsed().as_secs();

    Json(json!({
        "cluster_id": state.cluster_state.cluster_id(),
        "node_id": state.cluster_state.node_id(),
        "partitions": snapshot.partitions,
        "consumer_groups": snapshot.consumer_groups,
        "active_chaos_rules": chaos_rules.len(),
        "uptime_secs": uptime_secs,
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn dump_state_handler(State(state): State<AppState>) -> Json<ClusterStateSnapshot> {
    let snapshot = ClusterStateSnapshot::capture(&state.cluster_state, &state.coordinator);
    Json(snapshot)
}

async fn reset_state_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.cluster_state.reset();
    state.coordinator.reset();
    Json(json!({ "status": "ok", "message": "Cluster state reset to clean initial state" }))
}

async fn load_state_handler(
    State(state): State<AppState>,
    Json(snapshot): Json<ClusterStateSnapshot>,
) -> impl IntoResponse {
    match snapshot.apply(&state.cluster_state, &state.coordinator) {
        Ok(_) => Json(json!({ "status": "ok", "message": "State restored successfully" })),
        Err(e) => Json(json!({ "status": "error", "message": e.to_string() })),
    }
}

async fn list_chaos_handler(State(state): State<AppState>) -> Json<Vec<ChaosRule>> {
    Json(state.chaos.list_rules())
}

async fn add_chaos_handler(
    State(state): State<AppState>,
    Json(rule): Json<ChaosRule>,
) -> impl IntoResponse {
    state.chaos.add_rule(rule);
    Json(json!({ "status": "ok", "message": "Chaos rule registered" }))
}

async fn clear_chaos_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.chaos.clear_rules();
    Json(json!({ "status": "ok", "message": "All chaos rules cleared" }))
}

#[derive(serde::Deserialize)]
pub struct AdvertisedConfigRequest {
    pub host: Option<String>,
    pub port: Option<i32>,
}

async fn update_advertised_handler(
    State(state): State<AppState>,
    Json(payload): Json<AdvertisedConfigRequest>,
) -> impl IntoResponse {
    if let Some(h) = payload.host {
        state.cluster_state.set_advertised_host(h);
    }
    if let Some(p) = payload.port {
        state.cluster_state.set_advertised_port(p);
    }
    Json(json!({
        "status": "updated",
        "advertised_host": state.cluster_state.host(),
        "advertised_port": state.cluster_state.port(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidemq_s3stream::block_cache::BlockCache;
    use oxidemq_s3stream::client::MemoryObjectStorage;
    use oxidemq_s3stream::log_cache::LogCache;
    use oxidemq_wal::memory::MemoryWal;

    #[tokio::test]
    async fn test_admin_router_routes() {
        let wal = Arc::new(MemoryWal::new());
        let storage = Arc::new(MemoryObjectStorage::new());
        let log_cache = Arc::new(LogCache::new(1024 * 1024));
        let block_cache = Arc::new(BlockCache::new(1024 * 1024));
        let cluster_state = Arc::new(ClusterState::new(
            1,
            "127.0.0.1",
            9092,
            "test-cluster",
            wal,
            storage,
            log_cache,
            block_cache,
        ));
        let coordinator = Arc::new(GroupCoordinator::new());
        let chaos = Arc::new(ChaosEngine::new());
        let app_state = AppState {
            cluster_state,
            coordinator,
            chaos,
            start_time: Instant::now(),
        };

        let _router = create_admin_router(app_state);
        // Router created successfully with all routes registered
    }
}
