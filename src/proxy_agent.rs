//! Proxy agent for browser extension agents.
//!
//! This module implements `LocalAgent` for browser agents whose private keys
//! live in the browser extension. The gateway acts as a proxy, registering
//! the agent with the kitsune2 network and forwarding signals.
//!
//! # Signing Strategy
//!
//! Browser agents can't sign locally since their private keys are in the browser.
//! We use a remote signing protocol:
//!
//! 1. Kitsune2 calls `sign()` on the ProxyAgent
//! 2. ProxyAgent sends a sign request to the browser via WebSocket
//! 3. Browser signs with its local Lair keystore
//! 4. Browser sends the signature back via WebSocket
//! 5. ProxyAgent returns the signature to kitsune2
//!
//! This allows the gateway to participate fully in kitsune2 while keeping
//! private keys secure in the browser extension.

use crate::agent_proxy::AgentProxyManager;
use bytes::Bytes;
use holochain_types::prelude::AgentPubKey;
use kitsune2_api::{AgentId, AgentInfo, BoxFut, DhtArc, K2Error, K2Result, LocalAgent, Signer};
use std::sync::{Arc, Mutex};
use tracing::{debug, warn};

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
/// browser extension. Signing requests are delegated to the browser via
/// WebSocket using the remote signing protocol.
///
/// # Zero-Arc Agents
///
/// Browser agents are "zero-arc" - they don't store DHT data locally.
/// They rely on the network (via the gateway) for all data retrieval.
pub struct ProxyAgent {
    /// The agent's public key for kitsune2.
    agent_id: AgentId,
    /// The agent's public key (Holochain type for type-safe lookups).
    agent_pubkey: AgentPubKey,
    /// Reference to the agent proxy manager for remote signing.
    agent_proxy: AgentProxyManager,
    /// Mutable state.
    inner: Mutex<ProxyAgentInner>,
}

impl std::fmt::Debug for ProxyAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyAgent")
            .field("agent_id", &self.agent_id)
            .field("agent_pubkey", &self.agent_pubkey)
            .finish()
    }
}

impl ProxyAgent {
    /// Create a new ProxyAgent with the given agent public key.
    ///
    /// Uses the proper Holochain AgentPubKey type for type-safe registration lookups.
    pub fn new(agent_pubkey: AgentPubKey, agent_proxy: AgentProxyManager) -> Self {
        // Convert to kitsune2 AgentId using the 32-byte key
        let agent_id = AgentId::from(Bytes::copy_from_slice(agent_pubkey.get_raw_32()));
        Self {
            agent_id,
            agent_pubkey,
            agent_proxy,
            inner: Mutex::new(ProxyAgentInner {
                cb: None,
                cur_arc: DhtArc::Empty,
                tgt_arc: DhtArc::Empty,
            }),
        }
    }

    /// Get the agent public key.
    pub fn agent_pubkey(&self) -> &AgentPubKey {
        &self.agent_pubkey
    }
}

impl Signer for ProxyAgent {
    fn sign<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        _agent_info: &'b AgentInfo,
        message: &'c [u8],
    ) -> BoxFut<'a, K2Result<Bytes>> {
        // Delegate signing to the browser via WebSocket.
        // The browser will sign with its local Lair keystore and return the signature.
        let agent_proxy = self.agent_proxy.clone();
        let agent_pubkey = self.agent_pubkey.clone();
        let message = message.to_vec();

        Box::pin(async move {
            debug!(
                agent = %agent_pubkey,
                message_len = message.len(),
                "Requesting remote signature from browser"
            );

            match agent_proxy.request_signature(&agent_pubkey, &message).await {
                Ok(signature) => {
                    debug!(
                        agent = %agent_pubkey,
                        signature_len = signature.len(),
                        "Received remote signature from browser"
                    );
                    Ok(signature)
                }
                Err(e) => {
                    warn!(
                        agent = %agent_pubkey,
                        error = %e,
                        "Remote signing failed"
                    );
                    Err(K2Error::other(format!("Remote signing failed: {}", e)))
                }
            }
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

    fn test_agent_proxy() -> AgentProxyManager {
        AgentProxyManager::new()
    }

    fn test_agent(id: u8) -> AgentPubKey {
        AgentPubKey::from_raw_36(vec![id; 36])
    }

    #[test]
    fn test_proxy_agent_creation() {
        let agent_pubkey = test_agent(0xab);
        let agent = ProxyAgent::new(agent_pubkey.clone(), test_agent_proxy());

        // The kitsune2 AgentId uses the 32-byte key
        assert_eq!(agent.agent().as_ref(), agent_pubkey.get_raw_32());
        assert_eq!(agent.get_cur_storage_arc(), DhtArc::Empty);
        assert_eq!(agent.get_tgt_storage_arc(), DhtArc::Empty);
    }

    #[test]
    fn test_proxy_agent_storage_arcs() {
        let agent = ProxyAgent::new(test_agent(0x12), test_agent_proxy());

        // Browser agents are zero-arc, but kitsune2 may try to set arcs
        agent.set_cur_storage_arc(DhtArc::Empty);
        agent.set_tgt_storage_arc_hint(DhtArc::Empty);

        assert_eq!(agent.get_cur_storage_arc(), DhtArc::Empty);
        assert_eq!(agent.get_tgt_storage_arc(), DhtArc::Empty);
    }

    #[test]
    fn test_proxy_agent_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let agent = ProxyAgent::new(test_agent(0x34), test_agent_proxy());
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
    async fn test_proxy_agent_sign_without_registration_returns_error() {
        // Agent is not registered, so signing should fail
        let agent = ProxyAgent::new(test_agent(0x56), test_agent_proxy());

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
        assert!(err.to_string().contains("not registered"));
    }

    #[test]
    fn test_proxy_agent_debug() {
        let agent = ProxyAgent::new(test_agent(0x78), test_agent_proxy());
        let debug = format!("{:?}", agent);
        assert!(debug.contains("ProxyAgent"));
        assert!(debug.contains("agent_id"));
    }
}
