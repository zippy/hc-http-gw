//! Proxy agent for browser extension agents.
//!
//! This module implements `LocalAgent` for browser agents whose private keys
//! live in the browser extension. The gateway acts as a proxy, registering
//! the agent with the kitsune2 network and forwarding signals.
//!
//! # Signing Strategy
//!
//! Browser agents can't sign locally since their private keys are in the browser.
//! We use a "pre-signed agent info" approach:
//!
//! 1. Browser creates AgentInfo (with gateway's URL as listening address)
//! 2. Browser signs it with the agent's private key
//! 3. Browser sends the encoded AgentInfoSigned to the gateway during registration
//! 4. Gateway stores and uses this for bootstrap registration
//!
//! For any other signing needs, the gateway would need to communicate with
//! the browser via WebSocket (not yet implemented).

use bytes::Bytes;
use kitsune2_api::{AgentId, AgentInfo, BoxFut, DhtArc, K2Error, K2Result, LocalAgent, Signer};
use std::sync::{Arc, Mutex};
use tracing::warn;

/// Inner mutable state for ProxyAgent.
struct ProxyAgentInner {
    /// Callback registered by kitsune2 for state changes.
    cb: Option<Arc<dyn Fn() + 'static + Send + Sync>>,
    /// Current storage arc (always Empty for zero-arc browser agents).
    cur_arc: DhtArc,
    /// Target storage arc (always Empty for zero-arc browser agents).
    tgt_arc: DhtArc,
}

/// A proxy agent that represents a browser extension agent in the gateway.
///
/// This implements `LocalAgent` for agents whose private keys live in the
/// browser extension. Since we can't sign locally, any signing requests
/// must be delegated to the browser via WebSocket.
///
/// # Zero-Arc Agents
///
/// Browser agents are "zero-arc" - they don't store DHT data locally.
/// They rely on the network (via the gateway) for all data retrieval.
pub struct ProxyAgent {
    /// The agent's public key (32 bytes for Ed25519).
    agent_id: AgentId,
    /// Mutable state.
    inner: Mutex<ProxyAgentInner>,
}

impl std::fmt::Debug for ProxyAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyAgent")
            .field("agent_id", &self.agent_id)
            .finish()
    }
}

impl ProxyAgent {
    /// Create a new ProxyAgent with the given agent public key.
    ///
    /// The `agent_pubkey` should be the raw 32-byte Ed25519 public key.
    pub fn new(agent_pubkey: impl Into<Bytes>) -> Self {
        Self {
            agent_id: AgentId::from(agent_pubkey.into()),
            inner: Mutex::new(ProxyAgentInner {
                cb: None,
                cur_arc: DhtArc::Empty,
                tgt_arc: DhtArc::Empty,
            }),
        }
    }

    /// Create a ProxyAgent from a base64-encoded agent public key.
    ///
    /// This is useful when receiving the agent key from a WebSocket message.
    pub fn from_base64(agent_b64: &str) -> Result<Self, base64::DecodeError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD.decode(agent_b64)?;
        Ok(Self::new(bytes))
    }
}

impl Signer for ProxyAgent {
    fn sign<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        _agent_info: &'b AgentInfo,
        _message: &'c [u8],
    ) -> BoxFut<'a, K2Result<Bytes>> {
        // Browser agents can't sign locally - their private keys are in the browser.
        // For now, we return an error. In the future, this could delegate to the
        // browser via WebSocket, but that adds significant latency.
        //
        // The recommended approach is to use pre-signed AgentInfo when registering
        // agents, avoiding the need for runtime signing in most cases.
        Box::pin(async move {
            warn!("ProxyAgent::sign called but browser signing is not yet implemented");
            Err(K2Error::other(
                "ProxyAgent cannot sign locally - private key is in browser extension",
            ))
        })
    }
}

impl LocalAgent for ProxyAgent {
    fn agent(&self) -> &AgentId {
        &self.agent_id
    }

    fn register_cb(&self, cb: Arc<dyn Fn() + 'static + Send + Sync>) {
        self.inner.lock().unwrap().cb = Some(cb);
    }

    fn invoke_cb(&self) {
        let cb = self.inner.lock().unwrap().cb.clone();
        if let Some(cb) = cb {
            cb();
        }
    }

    fn get_cur_storage_arc(&self) -> DhtArc {
        self.inner.lock().unwrap().cur_arc
    }

    fn set_cur_storage_arc(&self, arc: DhtArc) {
        self.inner.lock().unwrap().cur_arc = arc;
    }

    fn get_tgt_storage_arc(&self) -> DhtArc {
        self.inner.lock().unwrap().tgt_arc
    }

    fn set_tgt_storage_arc_hint(&self, arc: DhtArc) {
        self.inner.lock().unwrap().tgt_arc = arc;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_agent_creation() {
        let pubkey = vec![0xab; 32];
        let agent = ProxyAgent::new(pubkey.clone());

        assert_eq!(agent.agent().as_ref(), &pubkey[..]);
        assert_eq!(agent.get_cur_storage_arc(), DhtArc::Empty);
        assert_eq!(agent.get_tgt_storage_arc(), DhtArc::Empty);
    }

    #[test]
    fn test_proxy_agent_from_base64() {
        use base64::Engine;
        let pubkey = vec![0xcd; 32];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&pubkey);

        let agent = ProxyAgent::from_base64(&b64).expect("decode base64");
        assert_eq!(agent.agent().as_ref(), &pubkey[..]);
    }

    #[test]
    fn test_proxy_agent_storage_arcs() {
        let agent = ProxyAgent::new(vec![0x12; 32]);

        // Browser agents are zero-arc, but kitsune2 may try to set arcs
        agent.set_cur_storage_arc(DhtArc::Empty);
        agent.set_tgt_storage_arc_hint(DhtArc::Empty);

        assert_eq!(agent.get_cur_storage_arc(), DhtArc::Empty);
        assert_eq!(agent.get_tgt_storage_arc(), DhtArc::Empty);
    }

    #[test]
    fn test_proxy_agent_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let agent = ProxyAgent::new(vec![0x34; 32]);
        let called = Arc::new(AtomicBool::new(false));

        let called_clone = called.clone();
        agent.register_cb(Arc::new(move || {
            called_clone.store(true, Ordering::SeqCst);
        }));

        assert!(!called.load(Ordering::SeqCst));
        agent.invoke_cb();
        assert!(called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_proxy_agent_sign_returns_error() {
        let agent = ProxyAgent::new(vec![0x56; 32]);

        // Create a minimal AgentInfo for testing
        let agent_info = AgentInfo {
            agent: agent.agent().clone(),
            space: kitsune2_api::SpaceId::from(Bytes::from(vec![0u8; 32])),
            created_at: kitsune2_api::Timestamp::now(),
            expires_at: kitsune2_api::Timestamp::now(),
            is_tombstone: false,
            url: None,
            storage_arc: DhtArc::Empty,
        };

        let result = agent.sign(&agent_info, b"test message").await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.to_string().contains("private key is in browser"));
    }

    #[test]
    fn test_proxy_agent_debug() {
        let agent = ProxyAgent::new(vec![0x78; 32]);
        let debug = format!("{:?}", agent);
        assert!(debug.contains("ProxyAgent"));
        assert!(debug.contains("agent_id"));
    }
}
