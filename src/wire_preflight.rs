//! Wire format for kitsune2 preflight messages.
//!
//! This module defines the preflight message format that must match
//! `holochain_p2p::types::wire::WirePreflightMessage` for compatibility
//! with Holochain conductors.

use bytes::{BufMut, BytesMut};

/// Network compatibility parameters.
///
/// Must match `holochain_p2p::NetworkCompatParams` exactly.
/// The protocol version is used during preflight to ensure compatible nodes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NetworkCompatParams {
    /// The current protocol version.
    /// Must match HCP2P_PROTO_VER (currently 2) from holochain_p2p.
    pub proto_ver: u32,
}

/// Current protocol version matching holochain_p2p::HCP2P_PROTO_VER
pub const HCP2P_PROTO_VER: u32 = 2;

impl Default for NetworkCompatParams {
    fn default() -> Self {
        Self {
            proto_ver: HCP2P_PROTO_VER,
        }
    }
}

/// Preflight message exchanged during kitsune2 peer handshake.
///
/// Must match `holochain_p2p::types::wire::WirePreflightMessage` format exactly.
/// Uses `rmp_serde::encode::write_named` for serialization (msgpack with field names).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WirePreflightMessage {
    /// Compatibility parameters for protocol version checking.
    pub compat: NetworkCompatParams,
    /// Local agent infos as base64-encoded strings.
    /// Each string is a base64-encoded `AgentInfoSigned`.
    pub agents: Vec<String>,
}

impl WirePreflightMessage {
    /// Create a new preflight message with default compat params and no agents.
    pub fn new() -> Self {
        Self {
            compat: NetworkCompatParams::default(),
            agents: vec![],
        }
    }

    /// Create a new preflight message with the given agents.
    pub fn with_agents(agents: Vec<String>) -> Self {
        Self {
            compat: NetworkCompatParams::default(),
            agents,
        }
    }

    /// Encode the preflight message to bytes.
    ///
    /// Uses `rmp_serde::encode::write_named` to match holochain_p2p encoding.
    pub fn encode(&self) -> Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let mut b = BufMut::writer(BytesMut::new());
        rmp_serde::encode::write_named(&mut b, self)?;
        Ok(b.into_inner().freeze())
    }

    /// Decode a preflight message from bytes.
    pub fn decode(data: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(rmp_serde::decode::from_slice(data)?)
    }
}

impl Default for WirePreflightMessage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_compat_params_default() {
        let params = NetworkCompatParams::default();
        assert_eq!(params.proto_ver, 2);
    }

    #[test]
    fn test_preflight_encode_decode_roundtrip() {
        let msg = WirePreflightMessage::new();
        let encoded = msg.encode().expect("encode");
        let decoded = WirePreflightMessage::decode(&encoded).expect("decode");

        assert_eq!(decoded.compat.proto_ver, 2);
        assert!(decoded.agents.is_empty());
    }

    #[test]
    fn test_preflight_with_agents() {
        let msg = WirePreflightMessage::with_agents(vec!["agent1".to_string(), "agent2".to_string()]);
        let encoded = msg.encode().expect("encode");
        let decoded = WirePreflightMessage::decode(&encoded).expect("decode");

        assert_eq!(decoded.agents.len(), 2);
        assert_eq!(decoded.agents[0], "agent1");
        assert_eq!(decoded.agents[1], "agent2");
    }
}
