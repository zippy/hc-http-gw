//! HTTP gateway service for Holochain

use crate::agent_proxy::AgentProxyManager;
use crate::app_selection::AppInfoCache;
use crate::auth::AgentAuthenticator;
use crate::holochain::{AdminCall, AppCall};
use crate::kitsune_proxy::GatewayKitsune;
use crate::temp_op_store::TempOpStoreHandle;
use crate::{
    config::Configuration,
    router::{hc_http_gateway_router, hc_http_gateway_router_full},
};
use axum::Router;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;

/// Core Holochain HTTP gateway service
#[derive(Debug)]
pub struct HcHttpGatewayService {
    listener: TcpListener,
    router: Router,
}

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    /// Gateway configuration.
    pub configuration: Configuration,
    /// Admin call handler.
    pub admin_call: Arc<dyn AdminCall>,
    /// App call handler.
    pub app_call: Arc<dyn AppCall>,
    /// App info cache.
    pub app_info_cache: AppInfoCache,
    /// Optional authenticator for browser extension agents.
    pub authenticator: Option<Arc<dyn AgentAuthenticator>>,
    /// Agent proxy manager for WebSocket connections.
    pub agent_proxy: AgentProxyManager,
    /// Optional kitsune2 network manager for remote signal forwarding.
    pub gateway_kitsune: Option<GatewayKitsune>,
    /// Optional TempOpStore handle for browser extension publishing.
    pub temp_op_store: Option<TempOpStoreHandle>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("configuration", &self.configuration)
            .field("app_info_cache", &self.app_info_cache)
            .field("has_authenticator", &self.authenticator.is_some())
            .field("agent_proxy", &self.agent_proxy)
            .field("has_gateway_kitsune", &self.gateway_kitsune.is_some())
            .field("has_temp_op_store", &self.temp_op_store.is_some())
            .finish()
    }
}

impl HcHttpGatewayService {
    /// Create a new service instance bound to the given address and port
    pub async fn new(
        address: impl Into<IpAddr>,
        port: u16,
        configuration: Configuration,
        admin_call: Arc<dyn AdminCall>,
        app_call: Arc<dyn AppCall>,
    ) -> std::io::Result<Self> {
        tracing::info!("Configuration: {:?}", configuration);

        let router = hc_http_gateway_router(configuration, admin_call, app_call);

        let address = SocketAddr::new(address.into(), port);
        let listener = TcpListener::bind(address).await?;

        Ok(HcHttpGatewayService { router, listener })
    }

    /// Create a new service instance with optional authentication, agent proxy, kitsune2, and op store.
    ///
    /// This constructor allows configuring:
    /// - Signal forwarding via `AgentProxyManager` shared with the `AppConnPool`
    /// - Remote signal delivery via `GatewayKitsune` for kitsune2 network participation
    /// - DHT publishing via `TempOpStoreHandle` for browser extension agents
    pub async fn with_auth(
        address: impl Into<IpAddr>,
        port: u16,
        configuration: Configuration,
        admin_call: Arc<dyn AdminCall>,
        app_call: Arc<dyn AppCall>,
        authenticator: Option<Arc<dyn AgentAuthenticator>>,
        agent_proxy: Option<AgentProxyManager>,
        gateway_kitsune: Option<GatewayKitsune>,
        temp_op_store: Option<TempOpStoreHandle>,
    ) -> std::io::Result<Self> {
        tracing::info!("Configuration: {:?}", configuration);

        let router = hc_http_gateway_router_full(
            configuration,
            admin_call,
            app_call,
            authenticator,
            agent_proxy,
            gateway_kitsune,
            temp_op_store,
        );

        let address = SocketAddr::new(address.into(), port);
        let listener = TcpListener::bind(address).await?;

        Ok(HcHttpGatewayService { router, listener })
    }

    /// Get the socket address the service is configured to use
    pub fn address(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Start the HTTP server and run until terminated
    pub async fn run(self) -> std::io::Result<()> {
        let address = self.address()?;

        tracing::info!("Starting server on {}", address);
        axum::serve(self.listener, self.router).await?;

        Ok(())
    }
}
