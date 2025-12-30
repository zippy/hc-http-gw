//! Configuration-based agent authenticator.
//!
//! Checks agents against a list of allowed agent public keys from configuration.

use super::authenticator::AgentAuthenticator;
use super::session::SessionManager;
use super::types::{AuthError, ParsedAuthRequest};
use super::SessionToken;
use async_trait::async_trait;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use holochain_types::prelude::AgentPubKey;
use std::collections::HashSet;
use std::time::Duration;

/// Authenticator that checks agents against a configured list.
pub struct ConfigListAuthenticator {
    /// Set of allowed agent public keys.
    allowed_agents: HashSet<AgentPubKey>,
    /// Session manager for token creation/verification.
    session_manager: SessionManager,
}

impl ConfigListAuthenticator {
    /// Create a new authenticator with allowed agents and session TTL.
    pub fn new(allowed_agents: HashSet<AgentPubKey>, session_ttl: Duration) -> Self {
        Self {
            allowed_agents,
            session_manager: SessionManager::new(session_ttl),
        }
    }

    /// Create from a list of base64-encoded agent public keys.
    pub fn from_base64_list(
        agent_keys: &[String],
        session_ttl: Duration,
    ) -> Result<Self, AuthError> {
        let mut allowed_agents = HashSet::new();

        for key_str in agent_keys {
            let agent = AgentPubKey::try_from(key_str.clone()).map_err(|e| {
                AuthError::Internal(format!("Invalid agent public key '{}': {}", key_str, e))
            })?;
            allowed_agents.insert(agent);
        }

        Ok(Self::new(allowed_agents, session_ttl))
    }

    /// Verify an Ed25519 signature.
    fn verify_signature(
        &self,
        agent_pub_key: &AgentPubKey,
        signature: &[u8],
        message: &[u8],
    ) -> Result<(), AuthError> {
        // Extract the 32-byte public key from the AgentPubKey
        let pub_key_bytes: [u8; 32] = agent_pub_key
            .get_raw_32()
            .try_into()
            .map_err(|_| AuthError::InvalidSignature)?;

        let verifying_key =
            VerifyingKey::from_bytes(&pub_key_bytes).map_err(|_| AuthError::InvalidSignature)?;

        let sig_bytes: [u8; 64] = signature
            .try_into()
            .map_err(|_| AuthError::InvalidSignature)?;

        let signature = Signature::from_bytes(&sig_bytes);

        verifying_key
            .verify(message, &signature)
            .map_err(|_| AuthError::InvalidSignature)
    }
}

#[async_trait]
impl AgentAuthenticator for ConfigListAuthenticator {
    async fn authenticate(&self, request: ParsedAuthRequest) -> Result<SessionToken, AuthError> {
        // Check if agent is in allowed list
        if !self.allowed_agents.contains(&request.agent_pub_key) {
            return Err(AuthError::AgentNotAuthorized(
                request.agent_pub_key.to_string(),
            ));
        }

        // Verify signature
        self.verify_signature(&request.agent_pub_key, &request.signature, &request.nonce)?;

        // Create session
        Ok(self.session_manager.create_session(request.agent_pub_key))
    }

    async fn verify_session(&self, token: &str) -> Result<AgentPubKey, AuthError> {
        self.session_manager.verify(token)
    }

    async fn is_agent_authorized(&self, agent_pub_key: &AgentPubKey) -> bool {
        self.allowed_agents.contains(agent_pub_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_agent_pub_key() -> AgentPubKey {
        AgentPubKey::from_raw_32(vec![1u8; 32])
    }

    #[tokio::test]
    async fn test_unauthorized_agent() {
        let authenticator =
            ConfigListAuthenticator::new(HashSet::new(), Duration::from_secs(3600));

        let request = ParsedAuthRequest {
            agent_pub_key: test_agent_pub_key(),
            signature: vec![0u8; 64],
            nonce: vec![1, 2, 3, 4],
        };

        let result = authenticator.authenticate(request).await;
        assert!(matches!(result, Err(AuthError::AgentNotAuthorized(_))));
    }

    #[tokio::test]
    async fn test_is_agent_authorized() {
        let agent = test_agent_pub_key();
        let mut allowed = HashSet::new();
        allowed.insert(agent.clone());

        let authenticator = ConfigListAuthenticator::new(allowed, Duration::from_secs(3600));

        assert!(authenticator.is_agent_authorized(&agent).await);

        let other_agent = AgentPubKey::from_raw_32(vec![2u8; 32]);
        assert!(!authenticator.is_agent_authorized(&other_agent).await);
    }
}
