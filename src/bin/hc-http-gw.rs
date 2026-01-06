use anyhow::Context;
use clap::Parser;
use holochain_http_gateway::{
    resolve_address_from_url, AdminConn, AgentProxyManager, AllowedAppIds, AllowedFns,
    AppConnPool, Configuration, GatewayKitsune, HcHttpGatewayService, KitsuneProxy,
    KitsuneProxyBuilder, TempOpStoreFactory, TempOpStoreHandle,
};
use std::net::IpAddr;
use std::sync::Arc;
use std::{collections::HashMap, env, str::FromStr};
use tracing_subscriber::{
    fmt::{self, format::FmtSpan, time::UtcTime},
    layer::SubscriberExt,
    EnvFilter, Registry,
};

const DEFAULT_LOG_LEVEL: &str = "info";

/// Command line arguments and environment variables for configuring the Gateway Service
#[derive(clap::Parser, Debug)]
pub struct HcHttpGatewayArgs {
    /// The address to use
    #[arg(short, long, env = "HC_GW_ADDRESS", default_value = "127.0.0.1")]
    pub address: IpAddr,

    /// The port to bind to
    #[arg(short, long, env = "HC_GW_PORT", default_value = "8090")]
    pub port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install the default rustls crypto provider (required for TLS connections)
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    initialize_tracing_subscriber()?;

    let configuration = load_config_from_env().await?;

    let args = HcHttpGatewayArgs::parse();

    // Create AgentProxyManager for signal forwarding to browser extensions
    let agent_proxy = AgentProxyManager::new();

    let admin_call = Arc::new(AdminConn::new(configuration.admin_socket_addr));

    // Create AppConnPool with signal forwarding enabled
    let app_call = Arc::new(AppConnPool::with_signal_forwarding(
        configuration.clone(),
        admin_call.clone(),
        agent_proxy.clone(),
    ));

    // Build GatewayKitsune and TempOpStore if kitsune2 is enabled
    let (gateway_kitsune, temp_op_store) = build_gateway_kitsune(&agent_proxy).await?;

    // Create service with agent proxy for WebSocket-based signal delivery
    let service = HcHttpGatewayService::with_auth(
        args.address,
        args.port,
        configuration,
        admin_call,
        app_call,
        None, // No authenticator for now
        Some(agent_proxy),
        gateway_kitsune,
        temp_op_store,
    )
    .await?;

    service.run().await?;

    Ok(())
}

async fn load_config_from_env() -> anyhow::Result<Configuration> {
    let admin_ws_url = env::var("HC_GW_ADMIN_WS_URL").context("HC_GW_ADMIN_WS_URL is not set")?;
    let admin_socket_addr = resolve_address_from_url(&admin_ws_url)
        .await
        .context("Failed to extract socket address from the admin websocket URL")?;
    tracing::info!("Resolved admin socket address: {}", admin_socket_addr);

    let payload_limit_bytes = env::var("HC_GW_PAYLOAD_LIMIT_BYTES").unwrap_or_default();

    let allowed_app_ids = env::var("HC_GW_ALLOWED_APP_IDS").unwrap_or_default();

    let mut allowed_fns = HashMap::new();

    let app_ids = AllowedAppIds::from_str(&allowed_app_ids)?;
    for app_id in app_ids.iter() {
        let fns = env::var(format!("HC_GW_ALLOWED_FNS_{app_id}"))
            .context(format!("Missing HC_GW_ALLOWED_FNS_{app_id} env var"))?;
        let fns = AllowedFns::from_str(&fns)?;
        allowed_fns.insert(app_id.to_owned(), fns);
    }

    let max_app_connections = env::var("HC_GW_MAX_APP_CONNECTIONS").unwrap_or_default();

    let zome_call_timeout = env::var("HC_GW_ZOME_CALL_TIMEOUT_MS").unwrap_or_default();

    let config = Configuration::try_new(
        admin_socket_addr,
        &payload_limit_bytes,
        &allowed_app_ids,
        allowed_fns,
        &max_app_connections,
        &zome_call_timeout,
    )?;

    Ok(config)
}

/// Build GatewayKitsune and TempOpStore if kitsune2 is enabled via environment variables.
///
/// Environment variables:
/// - `HC_GW_KITSUNE2_ENABLED`: Set to "true" or "1" to enable kitsune2
/// - `HC_GW_BOOTSTRAP_URL`: Bootstrap server URL (required if enabled)
/// - `HC_GW_SIGNAL_URL`: WebRTC signal server URL (required if enabled)
async fn build_gateway_kitsune(
    agent_proxy: &AgentProxyManager,
) -> anyhow::Result<(Option<GatewayKitsune>, Option<TempOpStoreHandle>)> {
    let enabled = env::var("HC_GW_KITSUNE2_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if !enabled {
        tracing::info!("Kitsune2 disabled (set HC_GW_KITSUNE2_ENABLED=true to enable)");
        return Ok((None, None));
    }

    let bootstrap_url = env::var("HC_GW_BOOTSTRAP_URL")
        .context("HC_GW_BOOTSTRAP_URL required when kitsune2 is enabled")?;
    let signal_url = env::var("HC_GW_SIGNAL_URL")
        .context("HC_GW_SIGNAL_URL required when kitsune2 is enabled")?;

    tracing::info!(
        %bootstrap_url,
        %signal_url,
        "Initializing kitsune2 for remote signal forwarding with TempOpStore"
    );

    // Create TempOpStore for browser extension publishing
    let (op_store_factory, temp_op_store_handle) = TempOpStoreFactory::create();
    op_store_factory.start_cleanup_task();
    tracing::info!("TempOpStore initialized with 60-second TTL");

    let handler = KitsuneProxy::new(agent_proxy.clone());
    let kitsune = KitsuneProxyBuilder::new(handler)
        .with_bootstrap_url(&bootstrap_url)
        .with_signal_url(&signal_url)
        .with_op_store(op_store_factory.into_dyn())
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to build kitsune2 instance: {}", e))?;

    let gateway_kitsune = GatewayKitsune::new(kitsune, agent_proxy.clone());
    tracing::info!("Kitsune2 initialized successfully");

    Ok((Some(gateway_kitsune), Some(temp_op_store_handle)))
}

/// Initialize a global tracing subscriber
pub fn initialize_tracing_subscriber() -> Result<(), tracing::subscriber::SetGlobalDefaultError> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_LEVEL));
    let formatting_layer = fmt::layer()
        .with_timer(UtcTime::rfc_3339())
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_file(true)
        .with_line_number(true);

    let subscriber = Registry::default().with(env_filter).with(formatting_layer);

    tracing::subscriber::set_global_default(subscriber)
}
