//! Agent authenticator trait.

use super::types::{AuthError, ParsedAuthRequest};
use super::SessionToken;
use async_trait::async_trait;
use holochain_types::prelude::AgentPubKey;

/// Trait for authenticating agents.
///
/// Implementations can verify agents against different sources:
/// - Configuration file (ConfigListAuthenticator)
/// - Database
/// - Capability grants
/// - Trust-on-first-use with rate limiting
#[async_trait]
pub trait AgentAuthenticator: Send + Sync {
    /// Verify a signed challenge and return a session token if valid.
    ///
    /// The implementation should:
    /// 1. Verify the signature matches the agent's public key
    /// 2. Check that the agent is authorized (e.g., in allowed list)
    /// 3. Return a session token on success
    async fn authenticate(&self, request: ParsedAuthRequest) -> Result<SessionToken, AuthError>;

    /// Verify a session token is still valid.
    ///
    /// Returns the agent's public key if the session is valid.
    async fn verify_session(&self, token: &str) -> Result<AgentPubKey, AuthError>;

    /// Check if an agent is authorized (without requiring authentication).
    ///
    /// Used to check if an agent would be allowed before starting auth flow.
    async fn is_agent_authorized(&self, agent_pub_key: &AgentPubKey) -> bool;
}
