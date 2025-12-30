//! Session management.

use super::types::AuthError;
use holochain_types::prelude::AgentPubKey;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Session token with metadata.
#[derive(Debug, Clone)]
pub struct SessionToken {
    /// The token string.
    pub token: String,
    /// The agent this session belongs to.
    pub agent_pub_key: AgentPubKey,
    /// When the session expires (Unix timestamp in seconds).
    pub expires_at: u64,
}

/// Manages session tokens and their expiration.
pub struct SessionManager {
    /// Active sessions keyed by token.
    sessions: RwLock<HashMap<String, SessionToken>>,
    /// Session time-to-live.
    ttl: Duration,
}

impl SessionManager {
    /// Create a new session manager with the given TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Create a new session for an agent.
    pub fn create_session(&self, agent_pub_key: AgentPubKey) -> SessionToken {
        let token = generate_token();
        let expires_at = current_timestamp() + self.ttl.as_secs();

        let session = SessionToken {
            token: token.clone(),
            agent_pub_key,
            expires_at,
        };

        self.sessions
            .write()
            .unwrap()
            .insert(token.clone(), session.clone());

        session
    }

    /// Verify a session token and return the associated agent.
    pub fn verify(&self, token: &str) -> Result<AgentPubKey, AuthError> {
        let sessions = self.sessions.read().unwrap();
        let session = sessions.get(token).ok_or(AuthError::InvalidSession)?;

        if session.expires_at < current_timestamp() {
            drop(sessions);
            self.sessions.write().unwrap().remove(token);
            return Err(AuthError::InvalidSession);
        }

        Ok(session.agent_pub_key.clone())
    }

    /// Remove expired sessions.
    pub fn cleanup_expired(&self) {
        let now = current_timestamp();
        self.sessions
            .write()
            .unwrap()
            .retain(|_, session| session.expires_at > now);
    }

    /// Invalidate a specific session.
    pub fn invalidate(&self, token: &str) {
        self.sessions.write().unwrap().remove(token);
    }
}

/// Generate a random session token.
fn generate_token() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let bytes: [u8; 32] = rng.random();
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_agent_pub_key() -> AgentPubKey {
        AgentPubKey::from_raw_32(vec![0u8; 32])
    }

    #[test]
    fn test_create_and_verify_session() {
        let manager = SessionManager::new(Duration::from_secs(3600));
        let agent = test_agent_pub_key();

        let session = manager.create_session(agent.clone());
        assert!(!session.token.is_empty());
        assert!(session.expires_at > current_timestamp());

        let verified = manager.verify(&session.token).unwrap();
        assert_eq!(verified, agent);
    }

    #[test]
    fn test_invalid_token() {
        let manager = SessionManager::new(Duration::from_secs(3600));
        let result = manager.verify("invalid-token");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalidate_session() {
        let manager = SessionManager::new(Duration::from_secs(3600));
        let agent = test_agent_pub_key();

        let session = manager.create_session(agent);
        manager.invalidate(&session.token);

        let result = manager.verify(&session.token);
        assert!(result.is_err());
    }
}
