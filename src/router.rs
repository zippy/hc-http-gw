use crate::auth::AgentAuthenticator;
use crate::holochain::AppCall;
use crate::{
    config::Configuration,
    routes::{
        auth_challenge, auth_verify, dht_count_links, dht_get_details, dht_get_links,
        dht_get_record, health_check, ws_handler, zome_call,
    },
    service::AppState,
    AdminCall,
};
use axum::{http::StatusCode, routing::{get, post}, Router};
use std::sync::Arc;

/// Create the HTTP gateway router.
pub fn hc_http_gateway_router(
    configuration: Configuration,
    admin_call: Arc<dyn AdminCall>,
    app_call: Arc<dyn AppCall>,
) -> Router {
    hc_http_gateway_router_with_auth(configuration, admin_call, app_call, None)
}

/// Create the HTTP gateway router with optional authentication.
pub fn hc_http_gateway_router_with_auth(
    configuration: Configuration,
    admin_call: Arc<dyn AdminCall>,
    app_call: Arc<dyn AppCall>,
    authenticator: Option<Arc<dyn AgentAuthenticator>>,
) -> Router {
    let state = AppState {
        configuration,
        admin_call,
        app_call,
        app_info_cache: Default::default(),
        authenticator,
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
        // Zome call endpoint
        .route(
            "/{dna_hash}/{coordinator_identifier}/{zome_name}/{fn_name}",
            get(zome_call),
        );

    // WebSocket endpoint for browser extension connections
    if ws_enabled {
        router = router.route("/ws", get(ws_handler));
    }

    router
        .method_not_allowed_fallback(|| async { (StatusCode::METHOD_NOT_ALLOWED, ()) })
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
