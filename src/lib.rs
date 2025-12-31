#![deny(missing_docs)]
//! # Holochain HTTP gateway
#![doc = include_str!("../spec.md")]

/// Agent proxy manager for browser extension connections.
pub mod agent_proxy;
mod app_selection;
/// Authentication module for agent verification.
pub mod auth;
mod config;
mod error;
mod holochain;
mod resolve;
mod router;
mod routes;
mod service;
mod transcode;

#[cfg(any(test, feature = "test-utils"))]
pub mod test;

pub use agent_proxy::AgentProxyManager;
pub use config::*;
pub use error::{ErrorResponse, HcHttpGatewayError, HcHttpGatewayResult};
pub use holochain::*;
pub use resolve::resolve_address_from_url;
pub use service::HcHttpGatewayService;
