#![deny(missing_docs)]
//! # Holochain HTTP gateway
#![doc = include_str!("../spec.md")]

/// Agent proxy manager for browser extension connections.
pub mod agent_proxy;
/// Kitsune2 proxy for browser extension agents.
pub mod kitsune_proxy;
/// Proxy agent implementation for browser agents.
pub mod proxy_agent;
mod app_selection;
/// Authentication module for agent verification.
pub mod auth;
mod config;
mod error;
mod holochain;
mod resolve;
mod router;
/// HTTP routes for the gateway.
pub mod routes;
mod service;
mod transcode;

#[cfg(any(test, feature = "test-utils"))]
pub mod test;

pub use agent_proxy::AgentProxyManager;
pub use config::*;
pub use error::{ErrorResponse, HcHttpGatewayError, HcHttpGatewayResult};
pub use holochain::*;
pub use resolve::resolve_address_from_url;
pub use kitsune_proxy::{GatewayKitsune, KitsuneProxy, KitsuneProxyBuilder};
pub use proxy_agent::ProxyAgent;
pub use service::HcHttpGatewayService;
