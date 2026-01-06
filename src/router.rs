use crate::agent_proxy::AgentProxyManager;
use crate::auth::AgentAuthenticator;
use crate::holochain::AppCall;
use crate::kitsune_proxy::GatewayKitsune;
use crate::temp_op_store::TempOpStoreHandle;
use crate::{
    config::Configuration,
    routes::{
        auth_challenge, auth_verify, dht_count_links, dht_get_details, dht_get_links,
        dht_get_record, dht_publish, health_check, test_signal, ws_handler, zome_call,
    },
    service::AppState,
    AdminCall,
};
use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// Create the HTTP gateway router.
pub fn hc_http_gateway_router(
    configuration: Configuration,
    admin_call: Arc<dyn AdminCall>,
    app_call: Arc<dyn AppCall>,
) -> Router {
    hc_http_gateway_router_with_auth(configuration, admin_call, app_call, None, None)
}

/// Create the HTTP gateway router with optional authentication and kitsune2.
pub fn hc_http_gateway_router_with_auth(
    configuration: Configuration,
    admin_call: Arc<dyn AdminCall>,
    app_call: Arc<dyn AppCall>,
    authenticator: Option<Arc<dyn AgentAuthenticator>>,
    agent_proxy: Option<AgentProxyManager>,
) -> Router {
    hc_http_gateway_router_full(
        configuration,
        admin_call,
        app_call,
        authenticator,
        agent_proxy,
        None,
        None,
    )
}

/// Create the HTTP gateway router with all optional features.
pub fn hc_http_gateway_router_full(
    configuration: Configuration,
    admin_call: Arc<dyn AdminCall>,
    app_call: Arc<dyn AppCall>,
    authenticator: Option<Arc<dyn AgentAuthenticator>>,
    agent_proxy: Option<AgentProxyManager>,
    gateway_kitsune: Option<GatewayKitsune>,
    temp_op_store: Option<TempOpStoreHandle>,
) -> Router {
    let state = AppState {
        configuration,
        admin_call,
        app_call,
        app_info_cache: Default::default(),
        authenticator,
        agent_proxy: agent_proxy.unwrap_or_else(AgentProxyManager::new),
        gateway_kitsune,
        temp_op_store,
    };

    let ws_enabled = state.configuration.websocket.enabled;

    let mut router = Router::new()
        .route("/health", get(health_check))
        // Auth endpoints
        .route("/auth/challenge", post(auth_challenge))
        .route("/auth/verify", post(auth_verify))
        // DHT endpoints (require session if authenticator configured)
        .route("/dht/{dna_hash}/record/{hash}", get(dht_get_record))
        .route("/dht/{dna_hash}/details/{hash}", get(dht_get_details))
        .route("/dht/{dna_hash}/links", get(dht_get_links))
        .route("/dht/{dna_hash}/links/count", get(dht_count_links))
        // DHT publish endpoint for zero-arc browser extension agents
        .route("/dht/{dna_hash}/publish", post(dht_publish))
        // Test endpoint for signal testing (development only)
        .route("/test/signal", post(test_signal))
        // Zome call endpoint
        .route(
            "/{dna_hash}/{coordinator_identifier}/{zome_name}/{fn_name}",
            get(zome_call),
        );

    // WebSocket endpoint for browser extension connections
    if ws_enabled {
        router = router.route("/ws", get(ws_handler));
    }

    // Add CORS layer to allow cross-origin requests (needed for browser extension testing)
    // Use permissive() which handles preflight OPTIONS requests automatically
    let cors = CorsLayer::permissive();

    router
        .layer(cors)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use crate::test::router::TestRouter;
    use axum::{body::Body, http::Request};
    use reqwest::StatusCode;
    use tower::ServiceExt;

    #[tokio::test]
    async fn get_request_to_root_fails() {
        let router = TestRouter::new();
        let (status_code, _) = router.request("/").await;
        assert_eq!(status_code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn post_method_to_health_fails() {
        let router = TestRouter::new();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn post_method_to_zome_call_fails() {
        let router = TestRouter::new();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/dna_hash/coodinator/zome_name/fn_name")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
