//! Agent Proxy Manager for browser extension connections.
//!
//! This module manages browser agents that connect via WebSocket.
//! It tracks which agents are registered for which DNAs and provides
//! a way to route signals to the correct WebSocket connections.
//!
//! It also handles remote signing requests, allowing the gateway to
//! request signatures from browser agents whose private keys are stored
//! in the browser extension.

use crate::routes::websocket::ServerMessage;
use base64::Engine;
use bytes::Bytes;
use holochain_types::prelude::{AgentPubKey, DnaHash};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};

/// Key for identifying a registered agent.
/// Uses actual Holochain types to ensure type safety and prevent encoding mismatches.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AgentRegistration {
    /// DNA hash.
    pub dna_hash: DnaHash,
    /// Agent public key.
    pub agent_pubkey: AgentPubKey,
}

/// Sender handle for a WebSocket connection.
pub type WsSender = mpsc::Sender<ServerMessage>;

/// Result type for signing operations.
pub type SignResult = Result<Bytes, String>;

/// Pending sign request waiting for browser response.
type PendingSign = oneshot::Sender<SignResult>;

/// Manages browser agent registrations and signal routing.
#[derive(Clone)]
pub struct AgentProxyManager {
    /// Map of registered agents to their WebSocket senders.
    /// Multiple agents can share the same sender (same WebSocket connection).
    registrations: Arc<RwLock<HashMap<AgentRegistration, WsSender>>>,
    /// Counter for generating unique request IDs.
    request_counter: Arc<AtomicU64>,
    /// Pending sign requests awaiting browser response.
    pending_signs: Arc<RwLock<HashMap<String, PendingSign>>>,
    /// Timeout for sign requests.
    sign_timeout: Duration,
}

impl std::fmt::Debug for AgentProxyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentProxyManager")
            .field("registrations", &self.registrations)
            .field("request_counter", &self.request_counter)
            .field("pending_signs_count", &"<pending>")
            .field("sign_timeout", &self.sign_timeout)
            .finish()
    }
}

impl Default for AgentProxyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentProxyManager {
    /// Create a new agent proxy manager.
    pub fn new() -> Self {
        Self {
            registrations: Arc::new(RwLock::new(HashMap::new())),
            request_counter: Arc::new(AtomicU64::new(0)),
            pending_signs: Arc::new(RwLock::new(HashMap::new())),
            sign_timeout: Duration::from_secs(30),
        }
    }

    /// Create a new agent proxy manager with custom sign timeout.
    pub fn with_sign_timeout(sign_timeout: Duration) -> Self {
        Self {
            registrations: Arc::new(RwLock::new(HashMap::new())),
            request_counter: Arc::new(AtomicU64::new(0)),
            pending_signs: Arc::new(RwLock::new(HashMap::new())),
            sign_timeout,
        }
    }

    /// Register an agent for a DNA.
    ///
    /// The sender is used to forward signals to the WebSocket connection.
    pub async fn register(
        &self,
        dna_hash: DnaHash,
        agent_pubkey: AgentPubKey,
        sender: WsSender,
    ) {
        let key = AgentRegistration {
            dna_hash: dna_hash.clone(),
            agent_pubkey: agent_pubkey.clone(),
        };

        let mut registrations = self.registrations.write().await;
        registrations.insert(key, sender);

        tracing::info!(
            "Agent {:?} registered for DNA {:?} (total registrations: {})",
            agent_pubkey,
            dna_hash,
            registrations.len()
        );
    }

    /// Unregister an agent from a DNA.
    pub async fn unregister(&self, dna_hash: &DnaHash, agent_pubkey: &AgentPubKey) {
        let key = AgentRegistration {
            dna_hash: dna_hash.clone(),
            agent_pubkey: agent_pubkey.clone(),
        };

        let mut registrations = self.registrations.write().await;
        if registrations.remove(&key).is_some() {
            tracing::info!(
                "Agent {:?} unregistered from DNA {:?} (total registrations: {})",
                agent_pubkey,
                dna_hash,
                registrations.len()
            );
        }
    }

    /// Unregister all agents associated with a given sender.
    ///
    /// This is called when a WebSocket connection closes.
    pub async fn unregister_all(&self, sender: &WsSender) {
        let mut registrations = self.registrations.write().await;
        let before = registrations.len();

        // Remove all entries where the sender matches
        registrations.retain(|_, s| !s.same_channel(sender));

        let removed = before - registrations.len();
        if removed > 0 {
            tracing::info!(
                "Unregistered {} agents on connection close (total registrations: {})",
                removed,
                registrations.len()
            );
        }
    }

    /// Send a signal to a specific agent.
    ///
    /// Returns true if the signal was queued for delivery, false if the agent
    /// is not registered or the channel is full/closed.
    pub async fn send_signal(
        &self,
        dna_hash: &DnaHash,
        agent_pubkey: &AgentPubKey,
        signal: ServerMessage,
    ) -> bool {
        let key = AgentRegistration {
            dna_hash: dna_hash.clone(),
            agent_pubkey: agent_pubkey.clone(),
        };

        let registrations = self.registrations.read().await;
        if let Some(sender) = registrations.get(&key) {
            match sender.try_send(signal) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        "Signal dropped for agent {:?} on DNA {:?}: channel full",
                        agent_pubkey,
                        dna_hash
                    );
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(
                        "Signal dropped for agent {:?} on DNA {:?}: channel closed",
                        agent_pubkey,
                        dna_hash
                    );
                    false
                }
            }
        } else {
            tracing::debug!(
                "Signal dropped for agent {:?} on DNA {:?}: not registered",
                agent_pubkey,
                dna_hash
            );
            false
        }
    }

    /// Get the number of registered agents.
    pub async fn registration_count(&self) -> usize {
        self.registrations.read().await.len()
    }

    /// Check if an agent is registered for a DNA.
    pub async fn is_registered(&self, dna_hash: &DnaHash, agent_pubkey: &AgentPubKey) -> bool {
        let key = AgentRegistration {
            dna_hash: dna_hash.clone(),
            agent_pubkey: agent_pubkey.clone(),
        };
        self.registrations.read().await.contains_key(&key)
    }

    /// Find a WebSocket sender for an agent (any DNA).
    ///
    /// Returns the first sender found for this agent, regardless of which DNA
    /// the registration is for. This is used for signing requests since the
    /// agent's private key is the same across all DNAs.
    async fn find_sender_for_agent(&self, agent_pubkey: &AgentPubKey) -> Option<WsSender> {
        let registrations = self.registrations.read().await;
        for (key, sender) in registrations.iter() {
            if &key.agent_pubkey == agent_pubkey {
                return Some(sender.clone());
            }
        }
        None
    }

    /// Request a signature from a browser agent.
    ///
    /// This sends a sign request to the browser via WebSocket and waits for
    /// the response. The browser will use its local Lair keystore to sign
    /// the message with the agent's private key.
    ///
    /// # Arguments
    ///
    /// * `agent_pubkey` - The agent's public key
    /// * `message` - The bytes to sign
    ///
    /// # Returns
    ///
    /// The signature bytes, or an error if signing failed.
    pub async fn request_signature(
        &self,
        agent_pubkey: &AgentPubKey,
        message: &[u8],
    ) -> SignResult {
        // Find a sender for this agent
        let sender = match self.find_sender_for_agent(agent_pubkey).await {
            Some(s) => s,
            None => {
                return Err(format!(
                    "Agent {:?} is not registered - cannot request signature",
                    agent_pubkey
                ));
            }
        };

        // Generate unique request ID
        let request_id = format!(
            "sign-{}-{}",
            self.request_counter.fetch_add(1, Ordering::SeqCst),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );

        // Create oneshot channel for response
        let (tx, rx) = oneshot::channel();

        // Store pending request
        {
            let mut pending = self.pending_signs.write().await;
            pending.insert(request_id.clone(), tx);
        }

        // Send sign request to browser - use HoloHash's built-in string encoding
        let sign_request = ServerMessage::SignRequest {
            request_id: request_id.clone(),
            agent_pubkey: agent_pubkey.to_string(),
            message: base64::engine::general_purpose::STANDARD.encode(message),
        };

        if let Err(e) = sender.send(sign_request).await {
            // Remove pending request on send failure
            self.pending_signs.write().await.remove(&request_id);
            return Err(format!("Failed to send sign request: {}", e));
        }

        tracing::debug!(
            request_id = %request_id,
            agent = %agent_pubkey,
            message_len = message.len(),
            "Sent sign request to browser"
        );

        // Wait for response with timeout
        let result = tokio::time::timeout(self.sign_timeout, rx).await;

        // Clean up pending request (if not already removed by deliver_signature)
        self.pending_signs.write().await.remove(&request_id);

        match result {
            Ok(Ok(signature)) => signature,
            Ok(Err(_)) => Err("Sign request cancelled".to_string()),
            Err(_) => Err(format!(
                "Sign request timed out after {:?}",
                self.sign_timeout
            )),
        }
    }

    /// Deliver a signature response from the browser.
    ///
    /// This is called by the WebSocket handler when a sign_response message
    /// is received from the browser.
    ///
    /// # Arguments
    ///
    /// * `request_id` - The request ID from the original sign request
    /// * `result` - The signature result (Ok with signature bytes, or Err with message)
    pub async fn deliver_signature(&self, request_id: &str, result: SignResult) {
        let sender = {
            let mut pending = self.pending_signs.write().await;
            pending.remove(request_id)
        };

        if let Some(sender) = sender {
            if sender.send(result).is_err() {
                tracing::warn!(
                    request_id = %request_id,
                    "Sign request receiver dropped before response delivered"
                );
            } else {
                tracing::debug!(
                    request_id = %request_id,
                    "Delivered sign response"
                );
            }
        } else {
            tracing::warn!(
                request_id = %request_id,
                "Received sign response for unknown request (may have timed out)"
            );
        }
    }

    /// Get the number of pending sign requests.
    pub async fn pending_sign_count(&self) -> usize {
        self.pending_signs.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dna(id: u8) -> DnaHash {
        DnaHash::from_raw_36(vec![id; 36])
    }

    fn test_agent(id: u8) -> AgentPubKey {
        AgentPubKey::from_raw_36(vec![id; 36])
    }

    #[tokio::test]
    async fn test_register_and_unregister() {
        let manager = AgentProxyManager::new();
        let (tx, _rx) = mpsc::channel(32);

        let dna1 = test_dna(1);
        let dna2 = test_dna(2);
        let agent1 = test_agent(1);
        let agent2 = test_agent(2);

        // Register an agent
        manager.register(dna1.clone(), agent1.clone(), tx.clone()).await;
        assert!(manager.is_registered(&dna1, &agent1).await);
        assert_eq!(manager.registration_count().await, 1);

        // Register another agent
        manager.register(dna2.clone(), agent2.clone(), tx.clone()).await;
        assert!(manager.is_registered(&dna2, &agent2).await);
        assert_eq!(manager.registration_count().await, 2);

        // Unregister first agent
        manager.unregister(&dna1, &agent1).await;
        assert!(!manager.is_registered(&dna1, &agent1).await);
        assert_eq!(manager.registration_count().await, 1);

        // Unregister second agent
        manager.unregister(&dna2, &agent2).await;
        assert!(!manager.is_registered(&dna2, &agent2).await);
        assert_eq!(manager.registration_count().await, 0);
    }

    #[tokio::test]
    async fn test_unregister_nonexistent() {
        let manager = AgentProxyManager::new();

        let dna1 = test_dna(1);
        let agent1 = test_agent(1);

        // Unregistering a non-existent agent should not panic
        manager.unregister(&dna1, &agent1).await;
        assert_eq!(manager.registration_count().await, 0);
    }

    #[tokio::test]
    async fn test_duplicate_registration() {
        let manager = AgentProxyManager::new();
        let (tx1, _rx1) = mpsc::channel(32);
        let (tx2, _rx2) = mpsc::channel(32);

        let dna1 = test_dna(1);
        let agent1 = test_agent(1);

        // Register same agent twice with different senders
        manager.register(dna1.clone(), agent1.clone(), tx1).await;
        manager.register(dna1.clone(), agent1.clone(), tx2).await;

        // Should still only have one registration (replaced)
        assert_eq!(manager.registration_count().await, 1);
    }

    #[tokio::test]
    async fn test_unregister_all() {
        let manager = AgentProxyManager::new();
        let (tx1, _rx1) = mpsc::channel(32);
        let (tx2, _rx2) = mpsc::channel(32);

        let dna1 = test_dna(1);
        let dna2 = test_dna(2);
        let agent1 = test_agent(1);
        let agent2 = test_agent(2);

        // Register multiple agents with same sender
        manager.register(dna1.clone(), agent1.clone(), tx1.clone()).await;
        manager.register(dna2.clone(), agent1.clone(), tx1.clone()).await;

        // Register one agent with different sender
        manager.register(dna1.clone(), agent2.clone(), tx2.clone()).await;

        assert_eq!(manager.registration_count().await, 3);

        // Unregister all for tx1
        manager.unregister_all(&tx1).await;

        // Only tx2's agent should remain
        assert_eq!(manager.registration_count().await, 1);
        assert!(!manager.is_registered(&dna1, &agent1).await);
        assert!(!manager.is_registered(&dna2, &agent1).await);
        assert!(manager.is_registered(&dna1, &agent2).await);
    }

    #[tokio::test]
    async fn test_send_signal_to_registered_agent() {
        let manager = AgentProxyManager::new();
        let (tx, mut rx) = mpsc::channel(32);

        let dna1 = test_dna(1);
        let agent1 = test_agent(1);

        manager.register(dna1.clone(), agent1.clone(), tx).await;

        let signal = ServerMessage::Signal {
            dna_hash: dna1.to_string(),
            from_agent: "sender".to_string(),
            zome_name: "test_zome".to_string(),
            signal: "test_signal".to_string(),
        };

        let sent = manager.send_signal(&dna1, &agent1, signal).await;
        assert!(sent);

        // Verify signal was received
        let received = rx.recv().await.unwrap();
        match received {
            ServerMessage::Signal { dna_hash, .. } => {
                assert_eq!(dna_hash, dna1.to_string());
            }
            _ => panic!("Expected Signal message"),
        }
    }

    #[tokio::test]
    async fn test_send_signal_to_unregistered_agent() {
        let manager = AgentProxyManager::new();

        let dna1 = test_dna(1);
        let agent1 = test_agent(1);

        let signal = ServerMessage::Signal {
            dna_hash: dna1.to_string(),
            from_agent: "sender".to_string(),
            zome_name: "test_zome".to_string(),
            signal: "test_signal".to_string(),
        };

        let sent = manager.send_signal(&dna1, &agent1, signal).await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn test_send_signal_to_closed_channel() {
        let manager = AgentProxyManager::new();
        let (tx, rx) = mpsc::channel(32);

        let dna1 = test_dna(1);
        let agent1 = test_agent(1);

        manager.register(dna1.clone(), agent1.clone(), tx).await;

        // Drop the receiver to close the channel
        drop(rx);

        let signal = ServerMessage::Signal {
            dna_hash: dna1.to_string(),
            from_agent: "sender".to_string(),
            zome_name: "test_zome".to_string(),
            signal: "test_signal".to_string(),
        };

        let sent = manager.send_signal(&dna1, &agent1, signal).await;
        assert!(!sent);
    }
}
