//! Agent Proxy Manager for browser extension connections.
//!
//! This module manages browser agents that connect via WebSocket.
//! It tracks which agents are registered for which DNAs and provides
//! a way to route signals to the correct WebSocket connections.

use crate::routes::websocket::ServerMessage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// Key for identifying a registered agent.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AgentRegistration {
    /// DNA hash (base64 encoded).
    pub dna_hash: String,
    /// Agent public key (base64 encoded).
    pub agent_pubkey: String,
}

/// Sender handle for a WebSocket connection.
pub type WsSender = mpsc::Sender<ServerMessage>;

/// Manages browser agent registrations and signal routing.
#[derive(Debug, Clone)]
pub struct AgentProxyManager {
    /// Map of registered agents to their WebSocket senders.
    /// Multiple agents can share the same sender (same WebSocket connection).
    registrations: Arc<RwLock<HashMap<AgentRegistration, WsSender>>>,
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
        }
    }

    /// Register an agent for a DNA.
    ///
    /// The sender is used to forward signals to the WebSocket connection.
    pub async fn register(
        &self,
        dna_hash: String,
        agent_pubkey: String,
        sender: WsSender,
    ) {
        let key = AgentRegistration {
            dna_hash: dna_hash.clone(),
            agent_pubkey: agent_pubkey.clone(),
        };

        let mut registrations = self.registrations.write().await;
        registrations.insert(key, sender);

        tracing::info!(
            "Agent {} registered for DNA {} (total registrations: {})",
            agent_pubkey,
            dna_hash,
            registrations.len()
        );
    }

    /// Unregister an agent from a DNA.
    pub async fn unregister(&self, dna_hash: &str, agent_pubkey: &str) {
        let key = AgentRegistration {
            dna_hash: dna_hash.to_string(),
            agent_pubkey: agent_pubkey.to_string(),
        };

        let mut registrations = self.registrations.write().await;
        if registrations.remove(&key).is_some() {
            tracing::info!(
                "Agent {} unregistered from DNA {} (total registrations: {})",
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
        dna_hash: &str,
        agent_pubkey: &str,
        signal: ServerMessage,
    ) -> bool {
        let key = AgentRegistration {
            dna_hash: dna_hash.to_string(),
            agent_pubkey: agent_pubkey.to_string(),
        };

        let registrations = self.registrations.read().await;
        if let Some(sender) = registrations.get(&key) {
            match sender.try_send(signal) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        "Signal dropped for agent {} on DNA {}: channel full",
                        agent_pubkey,
                        dna_hash
                    );
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(
                        "Signal dropped for agent {} on DNA {}: channel closed",
                        agent_pubkey,
                        dna_hash
                    );
                    false
                }
            }
        } else {
            tracing::debug!(
                "Signal dropped for agent {} on DNA {}: not registered",
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
    pub async fn is_registered(&self, dna_hash: &str, agent_pubkey: &str) -> bool {
        let key = AgentRegistration {
            dna_hash: dna_hash.to_string(),
            agent_pubkey: agent_pubkey.to_string(),
        };
        self.registrations.read().await.contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_unregister() {
        let manager = AgentProxyManager::new();
        let (tx, _rx) = mpsc::channel(32);

        // Register an agent
        manager.register("dna1".to_string(), "agent1".to_string(), tx.clone()).await;
        assert!(manager.is_registered("dna1", "agent1").await);
        assert_eq!(manager.registration_count().await, 1);

        // Register another agent
        manager.register("dna2".to_string(), "agent2".to_string(), tx.clone()).await;
        assert!(manager.is_registered("dna2", "agent2").await);
        assert_eq!(manager.registration_count().await, 2);

        // Unregister first agent
        manager.unregister("dna1", "agent1").await;
        assert!(!manager.is_registered("dna1", "agent1").await);
        assert_eq!(manager.registration_count().await, 1);

        // Unregister second agent
        manager.unregister("dna2", "agent2").await;
        assert!(!manager.is_registered("dna2", "agent2").await);
        assert_eq!(manager.registration_count().await, 0);
    }

    #[tokio::test]
    async fn test_unregister_nonexistent() {
        let manager = AgentProxyManager::new();

        // Unregistering a non-existent agent should not panic
        manager.unregister("dna1", "agent1").await;
        assert_eq!(manager.registration_count().await, 0);
    }

    #[tokio::test]
    async fn test_duplicate_registration() {
        let manager = AgentProxyManager::new();
        let (tx1, _rx1) = mpsc::channel(32);
        let (tx2, _rx2) = mpsc::channel(32);

        // Register same agent twice with different senders
        manager.register("dna1".to_string(), "agent1".to_string(), tx1).await;
        manager.register("dna1".to_string(), "agent1".to_string(), tx2).await;

        // Should still only have one registration (replaced)
        assert_eq!(manager.registration_count().await, 1);
    }

    #[tokio::test]
    async fn test_unregister_all() {
        let manager = AgentProxyManager::new();
        let (tx1, _rx1) = mpsc::channel(32);
        let (tx2, _rx2) = mpsc::channel(32);

        // Register multiple agents with same sender
        manager.register("dna1".to_string(), "agent1".to_string(), tx1.clone()).await;
        manager.register("dna2".to_string(), "agent1".to_string(), tx1.clone()).await;

        // Register one agent with different sender
        manager.register("dna1".to_string(), "agent2".to_string(), tx2.clone()).await;

        assert_eq!(manager.registration_count().await, 3);

        // Unregister all for tx1
        manager.unregister_all(&tx1).await;

        // Only tx2's agent should remain
        assert_eq!(manager.registration_count().await, 1);
        assert!(!manager.is_registered("dna1", "agent1").await);
        assert!(!manager.is_registered("dna2", "agent1").await);
        assert!(manager.is_registered("dna1", "agent2").await);
    }

    #[tokio::test]
    async fn test_send_signal_to_registered_agent() {
        let manager = AgentProxyManager::new();
        let (tx, mut rx) = mpsc::channel(32);

        manager.register("dna1".to_string(), "agent1".to_string(), tx).await;

        let signal = ServerMessage::Signal {
            dna_hash: "dna1".to_string(),
            from_agent: "sender".to_string(),
            zome_name: "test_zome".to_string(),
            signal: "test_signal".to_string(),
        };

        let sent = manager.send_signal("dna1", "agent1", signal).await;
        assert!(sent);

        // Verify signal was received
        let received = rx.recv().await.unwrap();
        match received {
            ServerMessage::Signal { dna_hash, .. } => {
                assert_eq!(dna_hash, "dna1");
            }
            _ => panic!("Expected Signal message"),
        }
    }

    #[tokio::test]
    async fn test_send_signal_to_unregistered_agent() {
        let manager = AgentProxyManager::new();

        let signal = ServerMessage::Signal {
            dna_hash: "dna1".to_string(),
            from_agent: "sender".to_string(),
            zome_name: "test_zome".to_string(),
            signal: "test_signal".to_string(),
        };

        let sent = manager.send_signal("dna1", "agent1", signal).await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn test_send_signal_to_closed_channel() {
        let manager = AgentProxyManager::new();
        let (tx, rx) = mpsc::channel(32);

        manager.register("dna1".to_string(), "agent1".to_string(), tx).await;

        // Drop the receiver to close the channel
        drop(rx);

        let signal = ServerMessage::Signal {
            dna_hash: "dna1".to_string(),
            from_agent: "sender".to_string(),
            zome_name: "test_zome".to_string(),
            signal: "test_signal".to_string(),
        };

        let sent = manager.send_signal("dna1", "agent1", signal).await;
        assert!(!sent);
    }
}
