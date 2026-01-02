//! Kitsune2 proxy for browser extension agents
//!
//! This module allows the gateway to participate in kitsune2
//! on behalf of zero-arc browser agents whose private keys live
//! in the browser extension.
//!
//! # Architecture
//!
//! The gateway runs its own kitsune2 instance that:
//! 1. Joins spaces (DNAs) on behalf of browser agents
//! 2. Receives `RemoteSignalEvt` messages via `recv_notify`
//! 3. Forwards signals to browser via WebSocket
//!
//! # Signal Flow
//!
//! ```text
//! Conductor Agent A ──send_remote_signal──► kitsune2 network
//!                                                │
//!                                                ▼
//! Gateway ◄── recv_notify (RemoteSignalEvt) ◄────┘
//!    │
//!    └── decode WireMessage
//!    └── forward to AgentProxyManager
//!    └── WebSocket to browser
//! ```

use crate::agent_proxy::AgentProxyManager;
use crate::routes::websocket::ServerMessage;
use base64::Engine;
use bytes::Bytes;
use holochain_p2p::WireMessage;
use holochain_types::prelude::{AgentPubKey, ExternIO};
use kitsune2_api::{
    BoxFut, DynKitsune, DynSpaceHandler, K2Result, KitsuneHandler, SpaceHandler, SpaceId, Url,
};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Convert a SpaceId to a base64-encoded DnaHash string.
///
/// SpaceId and DnaHash are the same bytes (32-byte hash + 4-byte type prefix + 3-byte DHT location).
fn space_id_to_dna_b64(space_id: &SpaceId) -> String {
    base64::engine::general_purpose::STANDARD.encode(space_id.as_ref())
}

/// Convert an AgentPubKey to a base64-encoded string.
fn agent_to_b64(agent: &AgentPubKey) -> String {
    base64::engine::general_purpose::STANDARD.encode(agent.get_raw_39())
}

/// Convert signal payload (ExternIO) to a base64-encoded string.
fn signal_to_b64(signal: &ExternIO) -> String {
    base64::engine::general_purpose::STANDARD.encode(&signal.0)
}

/// Top-level kitsune2 handler for the gateway.
///
/// This implements `KitsuneHandler` and creates `ProxySpaceHandler`
/// instances for each space (DNA) that has registered browser agents.
#[derive(Debug)]
pub struct KitsuneProxy {
    agent_proxy: AgentProxyManager,
}

impl KitsuneProxy {
    /// Create a new KitsuneProxy with the given agent proxy manager.
    pub fn new(agent_proxy: AgentProxyManager) -> Self {
        Self { agent_proxy }
    }
}

impl KitsuneHandler for KitsuneProxy {
    fn create_space(&self, space_id: SpaceId) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
        let agent_proxy = self.agent_proxy.clone();
        Box::pin(async move {
            info!(?space_id, "Creating proxy space handler");
            let handler: DynSpaceHandler = Arc::new(ProxySpaceHandler {
                space_id,
                agent_proxy,
            });
            Ok(handler)
        })
    }

    fn new_listening_address(&self, this_url: Url) -> BoxFut<'static, ()> {
        info!(%this_url, "Gateway kitsune2 listening on new address");
        Box::pin(async move {})
    }

    fn peer_disconnect(&self, peer: Url, reason: Option<String>) {
        debug!(%peer, ?reason, "Peer disconnected from gateway");
    }
}

/// Per-space handler that receives notifications from kitsune2.
///
/// When a `RemoteSignalEvt` is received, it decodes the wire message
/// and forwards the signal to the appropriate browser agent via WebSocket.
#[derive(Debug)]
struct ProxySpaceHandler {
    space_id: SpaceId,
    agent_proxy: AgentProxyManager,
}

impl SpaceHandler for ProxySpaceHandler {
    fn recv_notify(
        &self,
        from_peer: Url,
        space_id: SpaceId,
        data: Bytes,
    ) -> K2Result<()> {
        debug!(
            %from_peer,
            ?space_id,
            data_len = data.len(),
            "Received notification in proxy space"
        );

        // Decode the wire messages
        match WireMessage::decode_batch(&data) {
            Ok(messages) => {
                for msg in messages {
                    self.handle_wire_message(msg, &from_peer);
                }
            }
            Err(e) => {
                warn!(%e, "Failed to decode wire messages");
            }
        }

        Ok(())
    }
}

impl ProxySpaceHandler {
    fn handle_wire_message(&self, msg: WireMessage, from_peer: &Url) {
        match msg {
            WireMessage::RemoteSignalEvt {
                to_agent,
                zome_call_params_serialized,
                signature: _,
            } => {
                info!(
                    ?to_agent,
                    %from_peer,
                    payload_len = zome_call_params_serialized.0.len(),
                    "Received RemoteSignalEvt for browser agent"
                );

                // Convert to base64 strings for the WebSocket message
                let dna_hash = space_id_to_dna_b64(&self.space_id);
                let agent_pubkey = agent_to_b64(&to_agent);
                let signal_data = signal_to_b64(&zome_call_params_serialized);

                // Create the server message
                // Note: from_agent is "remote" since we don't have the sender's
                // agent key in RemoteSignalEvt (it's embedded in zome_call_params)
                let server_msg = ServerMessage::Signal {
                    dna_hash: dna_hash.clone(),
                    from_agent: "remote".to_string(),
                    zome_name: "recv_remote_signal".to_string(),
                    signal: signal_data,
                };

                // Forward to the registered browser agent via AgentProxyManager
                // spawn a task since send_signal is async and recv_notify is sync
                let agent_proxy = self.agent_proxy.clone();
                tokio::spawn(async move {
                    let sent = agent_proxy
                        .send_signal(&dna_hash, &agent_pubkey, server_msg)
                        .await;
                    if sent {
                        debug!(
                            dna = %dna_hash,
                            agent = %agent_pubkey,
                            "Remote signal forwarded to browser agent"
                        );
                    }
                });
            }
            other => {
                debug!(
                    msg_type = ?std::mem::discriminant(&other),
                    "Ignoring non-signal wire message"
                );
            }
        }
    }
}

/// Builder for creating a gateway kitsune2 instance.
///
/// # Example
///
/// ```ignore
/// let proxy = KitsuneProxy::new(agent_proxy_manager);
/// let kitsune = KitsuneProxyBuilder::new(proxy)
///     .with_bootstrap_url("https://bootstrap.example.com")
///     .with_signal_url("wss://signal.example.com")
///     .build()
///     .await?;
/// ```
pub struct KitsuneProxyBuilder {
    handler: Arc<KitsuneProxy>,
    bootstrap_url: Option<String>,
    signal_url: Option<String>,
}

impl KitsuneProxyBuilder {
    /// Create a new builder with the given handler.
    pub fn new(handler: KitsuneProxy) -> Self {
        Self {
            handler: Arc::new(handler),
            bootstrap_url: None,
            signal_url: None,
        }
    }

    /// Set the bootstrap server URL.
    pub fn with_bootstrap_url(mut self, url: impl Into<String>) -> Self {
        self.bootstrap_url = Some(url.into());
        self
    }

    /// Set the signal server URL (for tx5 transport).
    pub fn with_signal_url(mut self, url: impl Into<String>) -> Self {
        self.signal_url = Some(url.into());
        self
    }

    /// Build the kitsune2 instance.
    pub async fn build(self) -> Result<DynKitsune, Box<dyn std::error::Error + Send + Sync>> {
        use kitsune2::default_builder;
        use kitsune2_core::factories::config::{CoreBootstrapConfig, CoreBootstrapModConfig};
        use kitsune2_transport_tx5::config::{Tx5TransportConfig, Tx5TransportModConfig};

        let builder = default_builder().with_default_config()?;

        // Configure bootstrap server
        if let Some(bootstrap_url) = self.bootstrap_url {
            builder.config.set_module_config(&CoreBootstrapModConfig {
                core_bootstrap: CoreBootstrapConfig {
                    server_url: bootstrap_url,
                    ..Default::default()
                },
            })?;
        }

        // Configure signal server
        if let Some(signal_url) = self.signal_url {
            builder.config.set_module_config(&Tx5TransportModConfig {
                tx5_transport: Tx5TransportConfig {
                    server_url: signal_url,
                    signal_allow_plain_text: true, // TODO: configure for production
                    ..Default::default()
                },
            })?;
        }

        // Build and register handler
        let kitsune = builder.build().await?;
        kitsune.register_handler(self.handler).await?;

        info!("Gateway kitsune2 instance created");
        Ok(kitsune)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holochain_types::prelude::{AgentPubKey, ExternIO, Signature};

    fn test_space_id() -> SpaceId {
        SpaceId::from(Bytes::from(vec![0u8; 32]))
    }

    fn test_signature() -> Signature {
        let bytes: [u8; 64] = [0xaa; 64];
        Signature::from(bytes)
    }

    #[test]
    fn test_kitsune_proxy_creation() {
        let agent_proxy = AgentProxyManager::new();
        let proxy = KitsuneProxy::new(agent_proxy);
        assert!(format!("{:?}", proxy).contains("KitsuneProxy"));
    }

    #[test]
    fn test_space_handler_creation() {
        let agent_proxy = AgentProxyManager::new();
        let handler = ProxySpaceHandler {
            space_id: test_space_id(),
            agent_proxy,
        };
        assert!(format!("{:?}", handler).contains("ProxySpaceHandler"));
    }

    #[test]
    fn test_decode_remote_signal_evt() {
        // Create a RemoteSignalEvt wire message
        let to_agent = AgentPubKey::from_raw_36(vec![0xdb; 36]);
        let zome_call_params = ExternIO::encode(b"test signal payload").unwrap();
        let signature = test_signature();

        let wire_msg = WireMessage::remote_signal_evt(
            to_agent.clone(),
            zome_call_params.clone(),
            signature.clone(),
        );

        // Encode as batch using the proper holochain encoding (write_named)
        let batch: Vec<&WireMessage> = vec![&wire_msg];
        let encoded = WireMessage::encode_batch(&batch).expect("encode");

        // Decode using WireMessage::decode_batch
        let decoded = WireMessage::decode_batch(&encoded).expect("decode");
        assert_eq!(decoded.len(), 1);

        match &decoded[0] {
            WireMessage::RemoteSignalEvt {
                to_agent: decoded_agent,
                zome_call_params_serialized,
                signature: decoded_sig,
            } => {
                assert_eq!(decoded_agent.get_raw_36(), to_agent.get_raw_36());
                assert_eq!(zome_call_params_serialized.0, zome_call_params.0);
                assert_eq!(decoded_sig.0.len(), 64);
            }
            other => panic!("Expected RemoteSignalEvt, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_space_handler_recv_notify() {
        // Create handler
        let agent_proxy = AgentProxyManager::new();
        let handler = ProxySpaceHandler {
            space_id: test_space_id(),
            agent_proxy,
        };

        // Create a RemoteSignalEvt
        let to_agent = AgentPubKey::from_raw_36(vec![0xdb; 36]);
        let zome_call_params = ExternIO::encode(b"hello from conductor").unwrap();
        let signature = test_signature();

        let wire_msg = WireMessage::remote_signal_evt(to_agent, zome_call_params, signature);
        let batch: Vec<&WireMessage> = vec![&wire_msg];
        let encoded = WireMessage::encode_batch(&batch).expect("encode");

        // Call recv_notify
        let from_peer = Url::from_str("ws://localhost:5000").unwrap();
        let space_id = test_space_id();

        let result = handler.recv_notify(from_peer, space_id, encoded);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_signal_forwarding_to_registered_agent() {
        use tokio::sync::mpsc;

        // Create handler with agent proxy
        let agent_proxy = AgentProxyManager::new();
        let (tx, mut rx) = mpsc::channel(32);

        // Calculate what the base64 values will be
        let space_bytes = vec![0u8; 32];
        let dna_hash_b64 =
            base64::engine::general_purpose::STANDARD.encode(&space_bytes);
        let agent_bytes = vec![0xdb; 36];
        // AgentPubKey adds 3 bytes (type prefix) to make 39 bytes
        let to_agent = AgentPubKey::from_raw_36(agent_bytes.clone());
        let agent_pubkey_b64 =
            base64::engine::general_purpose::STANDARD.encode(to_agent.get_raw_39());

        // Register the agent
        agent_proxy
            .register(dna_hash_b64.clone(), agent_pubkey_b64.clone(), tx)
            .await;

        // Create handler
        let handler = ProxySpaceHandler {
            space_id: SpaceId::from(Bytes::from(space_bytes)),
            agent_proxy: agent_proxy.clone(),
        };

        // Create a RemoteSignalEvt
        let zome_call_params = ExternIO::encode(b"test signal data").unwrap();
        let signature = test_signature();

        let wire_msg =
            WireMessage::remote_signal_evt(to_agent, zome_call_params.clone(), signature);
        let batch: Vec<&WireMessage> = vec![&wire_msg];
        let encoded = WireMessage::encode_batch(&batch).expect("encode");

        // Call recv_notify
        let from_peer = Url::from_str("ws://localhost:5000").unwrap();
        let space_id = SpaceId::from(Bytes::from(vec![0u8; 32]));

        let result = handler.recv_notify(from_peer, space_id, encoded);
        assert!(result.is_ok());

        // Give the spawned task time to run
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify the signal was forwarded
        let received = rx.try_recv().expect("Expected to receive forwarded signal");
        match received {
            crate::routes::websocket::ServerMessage::Signal {
                dna_hash,
                from_agent,
                zome_name,
                signal,
            } => {
                assert_eq!(dna_hash, dna_hash_b64);
                assert_eq!(from_agent, "remote");
                assert_eq!(zome_name, "recv_remote_signal");
                // Signal should be base64-encoded version of the zome_call_params
                let expected_signal =
                    base64::engine::general_purpose::STANDARD.encode(&zome_call_params.0);
                assert_eq!(signal, expected_signal);
            }
            other => panic!("Expected Signal message, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_signal_not_forwarded_to_unregistered_agent() {
        // Create handler with agent proxy (no agents registered)
        let agent_proxy = AgentProxyManager::new();

        // Create handler
        let handler = ProxySpaceHandler {
            space_id: test_space_id(),
            agent_proxy: agent_proxy.clone(),
        };

        // Create a RemoteSignalEvt for an unregistered agent
        let to_agent = AgentPubKey::from_raw_36(vec![0xdb; 36]);
        let zome_call_params = ExternIO::encode(b"signal for nobody").unwrap();
        let signature = test_signature();

        let wire_msg = WireMessage::remote_signal_evt(to_agent, zome_call_params, signature);
        let batch: Vec<&WireMessage> = vec![&wire_msg];
        let encoded = WireMessage::encode_batch(&batch).expect("encode");

        // Call recv_notify - should succeed (doesn't fail on unregistered agent)
        let from_peer = Url::from_str("ws://localhost:5000").unwrap();
        let space_id = test_space_id();

        let result = handler.recv_notify(from_peer, space_id, encoded);
        assert!(result.is_ok());

        // Verify no crash - the signal is just dropped for unregistered agents
        assert_eq!(agent_proxy.registration_count().await, 0);
    }
}
