//! Authentication module for the HTTP Gateway.
//!
//! Provides agent authentication using Ed25519 signatures and session tokens.

mod authenticator;
mod config_list;
mod session;
/// Authentication types.
pub mod types;

#[cfg(test)]
mod tests;

pub use authenticator::AgentAuthenticator;
pub use config_list::ConfigListAuthenticator;
pub use session::{SessionManager, SessionToken};
pub use types::{AuthChallenge, AuthError, AuthVerifyRequest, ParsedAuthRequest};
