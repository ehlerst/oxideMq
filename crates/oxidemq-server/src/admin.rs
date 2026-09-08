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
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

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

        let app = create_admin_router(app_state.clone());

        // GET /
        let res = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /ui
        let res = app
            .clone()
            .oneshot(Request::builder().uri("/ui").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /_oxidemq/health
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/_oxidemq/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /_oxidemq/version
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/_oxidemq/version")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /_oxidemq/status
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/_oxidemq/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /_oxidemq/state/dump
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/_oxidemq/state/dump")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // POST /_oxidemq/state/reset
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_oxidemq/state/reset")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // POST /_oxidemq/state/load
        let snapshot = ClusterStateSnapshot {
            cluster_id: "test-cluster".into(),
            node_id: 1,
            partitions: vec![],
            consumer_groups: vec![],
            timestamp_ms: 123456789,
        };
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_oxidemq/state/load")
                    .header("content-type", "application/json")
                    .body(Body::from(snapshot_json))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // POST /_oxidemq/chaos/rules
        let rule = json!({
            "id": "chaos-1",
            "target": "Produce",
            "latency_ms": 10,
            "error_probability": 0.0,
            "error_message": null
        });
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_oxidemq/chaos/rules")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&rule).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /_oxidemq/chaos/rules
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/_oxidemq/chaos/rules")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // POST /_oxidemq/chaos/clear
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_oxidemq/chaos/clear")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // POST /_oxidemq/advertised
        let adv = json!({
            "host": "broker.example.com",
            "port": 9094
        });
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_oxidemq/advertised")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&adv).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(app_state.cluster_state.host(), "broker.example.com");
        assert_eq!(app_state.cluster_state.port(), 9094);
    }
}
