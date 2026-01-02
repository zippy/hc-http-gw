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
use bytes::Bytes;
use holochain_p2p::WireMessage;
use kitsune2_api::{
    BoxFut, DynKitsune, DynSpaceHandler, K2Result, KitsuneHandler, SpaceHandler, SpaceId, Url,
};
use std::sync::Arc;
use tracing::{debug, info, warn};

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
                signature,
            } => {
                info!(
                    ?to_agent,
                    %from_peer,
                    payload_len = zome_call_params_serialized.0.len(),
                    "Received RemoteSignalEvt for browser agent"
                );

                // TODO: Forward to AgentProxyManager
                // The space_id maps to a DnaHash
                // The to_agent is the target AgentPubKey
                //
                // We need to:
                // 1. Convert space_id to DnaHash (they're the same bytes)
                // 2. Check if to_agent is registered in our proxy
                // 3. Forward the signal via WebSocket
                //
                // For now, just log that we received it
                debug!(
                    ?signature,
                    "Signal signature present (for verification)"
                );
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

    #[test]
    fn test_space_handler_recv_notify() {
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

        // The signal should be logged (but not forwarded yet - that's TODO)
    }
}
