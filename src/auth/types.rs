//! Authentication types.

use holochain_types::prelude::AgentPubKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Authentication errors.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Agent is not in the allowed list.
    #[error("Agent not authorized: {0}")]
    AgentNotAuthorized(String),

    /// Invalid signature.
    #[error("Invalid signature")]
    InvalidSignature,

    /// Session token is invalid or expired.
    #[error("Invalid or expired session")]
    InvalidSession,

    /// Nonce not found or already used.
    #[error("Invalid nonce")]
    InvalidNonce,

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Challenge sent to client for authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthChallenge {
    /// Random nonce to be signed.
    pub nonce: String,
    /// When the challenge expires (Unix timestamp in seconds).
    pub expires_at: u64,
}

/// Request to verify an authentication challenge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthVerifyRequest {
    /// Agent public key (base64 encoded).
    pub agent_pub_key: String,
    /// Signature of the nonce (base64 encoded).
    pub signature: String,
    /// The nonce that was signed.
    pub nonce: String,
}

/// Successful authentication response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResponse {
    /// Session token for subsequent requests.
    pub session_token: String,
    /// When the session expires (Unix timestamp in seconds).
    pub expires_at: u64,
}

/// Parsed agent public key with signature for verification.
pub struct ParsedAuthRequest {
    /// The agent's public key.
    pub agent_pub_key: AgentPubKey,
    /// The signature bytes.
    pub signature: Vec<u8>,
    /// The nonce bytes.
    pub nonce: Vec<u8>,
}
