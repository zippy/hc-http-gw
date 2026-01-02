//! Integration tests for kitsune2 remote signal forwarding.
//!
//! These tests verify that the gateway can:
//! 1. Create a kitsune2 instance and receive signals
//! 2. Forward signals to registered WebSocket clients
//! 3. Properly manage agent lifecycle (join/leave spaces)

use base64::Engine;
use bytes::Bytes;
use holochain_http_gateway::test::test_tracing::initialize_testing_tracing_subscriber;
use holochain_http_gateway::{
    AgentProxyManager, GatewayKitsune, KitsuneProxy, KitsuneProxyBuilder,
};
use holochain_p2p::WireMessage;
use holochain_types::prelude::{AgentPubKey, ExternIO, Signature};
use kitsune2_api::SpaceId;
use tokio::sync::mpsc;

/// Create a test SpaceId (DNA hash) from bytes.
fn test_space_id() -> SpaceId {
    SpaceId::from(Bytes::from(vec![0xaa; 32]))
}

/// Create a test agent pubkey.
fn test_agent_pubkey() -> AgentPubKey {
    AgentPubKey::from_raw_36(vec![0xbb; 36])
}

/// Create a test signature.
fn test_signature() -> Signature {
    Signature::from([0xcc; 64])
}

/// Convert SpaceId to base64 for WebSocket registration.
fn space_id_to_b64(space_id: &SpaceId) -> String {
    base64::engine::general_purpose::STANDARD.encode(space_id.as_ref())
}

/// Convert AgentPubKey to base64 for WebSocket registration.
fn agent_to_b64(agent: &AgentPubKey) -> String {
    base64::engine::general_purpose::STANDARD.encode(agent.get_raw_39())
}

/// Test that GatewayKitsune properly manages agent join/leave lifecycle.
#[tokio::test]
async fn test_gateway_kitsune_agent_lifecycle() {
    initialize_testing_tracing_subscriber();

    let agent_proxy = AgentProxyManager::new();
    let proxy = KitsuneProxy::new(agent_proxy.clone());

    // Build a minimal kitsune2 instance (without real network)
    // Note: This will fail to connect to bootstrap/signal servers, but
    // the GatewayKitsune structure can still be tested.
    let builder = KitsuneProxyBuilder::new(proxy)
        .with_bootstrap_url("https://localhost:12345")
        .with_signal_url("wss://localhost:12346");

    // Try to build - this may fail due to network, but we can test the builder
    let result = builder.build().await;

    // The build will likely fail in CI due to network, but we can verify
    // the builder was configured correctly by checking the error type
    if let Err(e) = result {
        // Expected: network-related error since we're using fake URLs
        let err_str = e.to_string();
        assert!(
            err_str.contains("connect")
                || err_str.contains("network")
                || err_str.contains("dns")
                || err_str.contains("resolve")
                || err_str.contains("transport")
                || err_str.contains("url")
                || err_str.contains("bootstrap"),
            "Unexpected error type: {}",
            err_str
        );
        return;
    }

    // If we somehow connected, test the full lifecycle
    let kitsune = result.unwrap();
    let gateway_kitsune = GatewayKitsune::new(kitsune);

    // Initially no agents or spaces
    assert_eq!(gateway_kitsune.agent_count().await, 0);
    assert_eq!(gateway_kitsune.space_count().await, 0);

    // Join an agent to a space
    let dna_b64 = space_id_to_b64(&test_space_id());
    let agent = test_agent_pubkey();
    let agent_bytes = agent.get_raw_39().to_vec();

    gateway_kitsune
        .agent_join(&dna_b64, agent_bytes.clone())
        .await
        .expect("agent join failed");

    assert_eq!(gateway_kitsune.agent_count().await, 1);
    assert_eq!(gateway_kitsune.space_count().await, 1);
    assert!(gateway_kitsune.is_agent_joined(&dna_b64, &agent_to_b64(&agent)).await);

    // Leave the agent
    gateway_kitsune.agent_leave(&dna_b64, agent_bytes).await;

    assert_eq!(gateway_kitsune.agent_count().await, 0);
    // Space should be cleaned up since no agents remain
    assert_eq!(gateway_kitsune.space_count().await, 0);
}

/// Test that signals received via recv_notify are forwarded to registered agents.
#[tokio::test]
async fn test_signal_forwarding_integration() {
    initialize_testing_tracing_subscriber();

    let agent_proxy = AgentProxyManager::new();

    // Create a channel to receive forwarded signals
    let (tx, mut rx) = mpsc::channel(32);

    // Register an agent
    let space_id = test_space_id();
    let dna_b64 = space_id_to_b64(&space_id);
    let agent = test_agent_pubkey();
    let agent_b64 = agent_to_b64(&agent);

    agent_proxy
        .register(dna_b64.clone(), agent_b64.clone(), tx)
        .await;

    // Create the proxy with the agent proxy manager
    let proxy = KitsuneProxy::new(agent_proxy.clone());

    // Simulate receiving a RemoteSignalEvt by calling create_space and recv_notify directly
    use kitsune2_api::KitsuneHandler;

    let space_handler = proxy.create_space(space_id.clone()).await.unwrap();

    // Create a wire message
    let zome_call_params = ExternIO::encode(b"test signal payload").unwrap();
    let wire_msg = WireMessage::remote_signal_evt(agent.clone(), zome_call_params.clone(), test_signature());
    let batch: Vec<&WireMessage> = vec![&wire_msg];
    let encoded = WireMessage::encode_batch(&batch).expect("encode");

    // Call recv_notify
    let from_peer = kitsune2_api::Url::from_str("ws://localhost:5000").unwrap();
    space_handler
        .recv_notify(from_peer, space_id, encoded)
        .expect("recv_notify failed");

    // Give the spawned task time to run
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify the signal was forwarded
    let received = rx.try_recv().expect("Expected to receive forwarded signal");
    match received {
        holochain_http_gateway::routes::websocket::ServerMessage::Signal {
            dna_hash,
            from_agent,
            zome_name,
            signal,
        } => {
            assert_eq!(dna_hash, dna_b64);
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

/// Test that signals are not forwarded to unregistered agents.
#[tokio::test]
async fn test_signal_not_forwarded_to_unregistered_agent() {
    initialize_testing_tracing_subscriber();

    let agent_proxy = AgentProxyManager::new();
    // No agents registered

    let proxy = KitsuneProxy::new(agent_proxy.clone());

    use kitsune2_api::KitsuneHandler;

    let space_id = test_space_id();
    let space_handler = proxy.create_space(space_id.clone()).await.unwrap();

    // Create a wire message for an unregistered agent
    let agent = test_agent_pubkey();
    let zome_call_params = ExternIO::encode(b"signal for nobody").unwrap();
    let wire_msg = WireMessage::remote_signal_evt(agent, zome_call_params, test_signature());
    let batch: Vec<&WireMessage> = vec![&wire_msg];
    let encoded = WireMessage::encode_batch(&batch).expect("encode");

    // Call recv_notify - should succeed without error
    let from_peer = kitsune2_api::Url::from_str("ws://localhost:5000").unwrap();
    let result = space_handler.recv_notify(from_peer, space_id, encoded);
    assert!(result.is_ok());

    // Verify no crash and no agents registered
    assert_eq!(agent_proxy.registration_count().await, 0);
}

/// Test that multiple agents can be registered for the same DNA.
#[tokio::test]
async fn test_multiple_agents_same_dna() {
    initialize_testing_tracing_subscriber();

    let agent_proxy = AgentProxyManager::new();

    let space_id = test_space_id();
    let dna_b64 = space_id_to_b64(&space_id);

    // Create channels for two agents
    let (tx1, mut rx1) = mpsc::channel(32);
    let (tx2, mut rx2) = mpsc::channel(32);

    // Register two different agents for the same DNA
    let agent1 = AgentPubKey::from_raw_36(vec![0x11; 36]);
    let agent2 = AgentPubKey::from_raw_36(vec![0x22; 36]);
    let agent1_b64 = agent_to_b64(&agent1);
    let agent2_b64 = agent_to_b64(&agent2);

    agent_proxy.register(dna_b64.clone(), agent1_b64.clone(), tx1).await;
    agent_proxy.register(dna_b64.clone(), agent2_b64.clone(), tx2).await;

    assert_eq!(agent_proxy.registration_count().await, 2);

    // Create the proxy
    let proxy = KitsuneProxy::new(agent_proxy.clone());

    use kitsune2_api::KitsuneHandler;
    let space_handler = proxy.create_space(space_id.clone()).await.unwrap();

    // Send a signal to agent1
    let zome_call_params = ExternIO::encode(b"signal for agent1").unwrap();
    let wire_msg = WireMessage::remote_signal_evt(agent1.clone(), zome_call_params.clone(), test_signature());
    let batch: Vec<&WireMessage> = vec![&wire_msg];
    let encoded = WireMessage::encode_batch(&batch).expect("encode");

    let from_peer = kitsune2_api::Url::from_str("ws://localhost:5000").unwrap();
    space_handler
        .recv_notify(from_peer.clone(), space_id.clone(), encoded)
        .expect("recv_notify failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Agent1 should receive the signal
    let received1 = rx1.try_recv().expect("Agent1 should receive signal");
    assert!(matches!(
        received1,
        holochain_http_gateway::routes::websocket::ServerMessage::Signal { .. }
    ));

    // Agent2 should NOT receive the signal (it was addressed to agent1)
    assert!(rx2.try_recv().is_err());

    // Now send a signal to agent2
    let zome_call_params2 = ExternIO::encode(b"signal for agent2").unwrap();
    let wire_msg2 = WireMessage::remote_signal_evt(agent2.clone(), zome_call_params2, test_signature());
    let batch2: Vec<&WireMessage> = vec![&wire_msg2];
    let encoded2 = WireMessage::encode_batch(&batch2).expect("encode");

    space_handler
        .recv_notify(from_peer, space_id, encoded2)
        .expect("recv_notify failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Agent2 should receive this signal
    let received2 = rx2.try_recv().expect("Agent2 should receive signal");
    assert!(matches!(
        received2,
        holochain_http_gateway::routes::websocket::ServerMessage::Signal { .. }
    ));

    // Agent1 should NOT receive this one
    assert!(rx1.try_recv().is_err());
}

/// Test that the WireMessage decoding handles batches correctly.
#[tokio::test]
async fn test_wire_message_batch_decoding() {
    initialize_testing_tracing_subscriber();

    let agent_proxy = AgentProxyManager::new();
    let (tx, mut rx) = mpsc::channel(32);

    let space_id = test_space_id();
    let dna_b64 = space_id_to_b64(&space_id);
    let agent = test_agent_pubkey();
    let agent_b64 = agent_to_b64(&agent);

    agent_proxy.register(dna_b64, agent_b64, tx).await;

    let proxy = KitsuneProxy::new(agent_proxy);

    use kitsune2_api::KitsuneHandler;
    let space_handler = proxy.create_space(space_id.clone()).await.unwrap();

    // Create a batch with multiple signals
    let zome_call_params1 = ExternIO::encode(b"signal 1").unwrap();
    let zome_call_params2 = ExternIO::encode(b"signal 2").unwrap();
    let wire_msg1 = WireMessage::remote_signal_evt(agent.clone(), zome_call_params1, test_signature());
    let wire_msg2 = WireMessage::remote_signal_evt(agent.clone(), zome_call_params2, test_signature());
    let batch: Vec<&WireMessage> = vec![&wire_msg1, &wire_msg2];
    let encoded = WireMessage::encode_batch(&batch).expect("encode");

    let from_peer = kitsune2_api::Url::from_str("ws://localhost:5000").unwrap();
    space_handler
        .recv_notify(from_peer, space_id, encoded)
        .expect("recv_notify failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Should receive both signals
    let received1 = rx.try_recv().expect("Should receive first signal");
    let received2 = rx.try_recv().expect("Should receive second signal");

    assert!(matches!(
        received1,
        holochain_http_gateway::routes::websocket::ServerMessage::Signal { .. }
    ));
    assert!(matches!(
        received2,
        holochain_http_gateway::routes::websocket::ServerMessage::Signal { .. }
    ));
}
