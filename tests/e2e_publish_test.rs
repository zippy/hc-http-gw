//! End-to-end test for the DHT publish endpoint with TempOpStore.
//!
//! This test is designed to run against the production gateway started
//! by the e2e-test-setup.sh script, which has TempOpStore configured.
//!
//! Run with:
//! ```
//! cargo test --test e2e_publish_test -- --nocapture --ignored
//! ```

use base64::Engine;
use holochain_types::dht_op::{ChainOp, DhtOp};
use holochain_types::prelude::*;
use serde::{Deserialize, Serialize};

/// Request body for the publish endpoint
#[derive(Debug, Serialize)]
struct PublishRequest {
    ops: Vec<SignedDhtOp>,
}

/// A signed DhtOp for publishing
#[derive(Debug, Serialize)]
struct SignedDhtOp {
    op_data: String,
    signature: String,
}

/// Response from the publish endpoint
#[derive(Debug, Deserialize)]
struct PublishResponse {
    success: bool,
    queued: usize,
    failed: usize,
    results: Vec<OpResult>,
}

#[derive(Debug, Deserialize)]
struct OpResult {
    success: bool,
    error: Option<String>,
}

fn make_test_chain_op() -> DhtOp {
    let author = AgentPubKey::from_raw_32(vec![0x11; 32]);
    let entry_hash = EntryHash::from_raw_32(vec![0x22; 32]);
    let prev_action = ActionHash::from_raw_32(vec![0x33; 32]);

    let action = Action::Create(Create {
        author,
        timestamp: Timestamp::now(),
        action_seq: 5,
        prev_action,
        entry_type: EntryType::App(AppEntryDef {
            entry_index: 0.into(),
            zome_index: 0.into(),
            visibility: EntryVisibility::Public,
        }),
        entry_hash,
        weight: EntryRateWeight::default(),
    });

    let entry_bytes = UnsafeBytes::from(vec![1u8, 2, 3]);
    let entry = Entry::App(AppEntryBytes(SerializedBytes::from(entry_bytes)));
    let signature = Signature::from([0xaa; 64]);

    DhtOp::ChainOp(Box::new(ChainOp::StoreRecord(
        signature,
        action,
        RecordEntry::Present(entry),
    )))
}

/// Test publish endpoint against the production gateway (with TempOpStore).
///
/// This test requires the gateway to be running with:
/// - HC_GW_KITSUNE2_ENABLED=true
/// - Kitsune2 bootstrap and signal URLs configured
///
/// Use `./scripts/e2e-test-setup.sh start` to start the gateway.
#[tokio::test]
#[ignore] // Run manually with: cargo test --test e2e_publish_test -- --ignored --nocapture
async fn test_publish_with_temp_op_store() {
    let gateway_url =
        std::env::var("GATEWAY_URL").unwrap_or_else(|_| "http://localhost:8090".to_string());
    let dna_hash = std::env::var("DNA_HASH").unwrap_or_else(|_| {
        "uhC0k2J3h4yJ17fbOaKJ8muCcpi9r58tqRFVVKFa6PeFqwy84A3ii".to_string()
    });

    println!("Testing against gateway: {}", gateway_url);
    println!("DNA hash: {}", dna_hash);

    // Check gateway is running
    let client = reqwest::Client::new();
    let health = client
        .get(format!("{}/health", gateway_url))
        .send()
        .await
        .expect("Gateway not reachable - run ./scripts/e2e-test-setup.sh start");
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    println!("Gateway health: OK");

    // Create a test DhtOp
    let op = make_test_chain_op();
    println!("Created test DhtOp");

    // Encode the op
    let op_bytes = holochain_serialized_bytes::encode(&op).expect("Failed to encode DhtOp");
    let op_data = base64::engine::general_purpose::STANDARD.encode(&op_bytes);
    let signature = base64::engine::general_purpose::STANDARD.encode(&[0xbb; 64]);

    println!("Op data length: {} bytes", op_bytes.len());
    println!("Base64 length: {} chars", op_data.len());

    // Build request
    let request = PublishRequest {
        ops: vec![SignedDhtOp { op_data, signature }],
    };

    // Send to publish endpoint
    let url = format!("{}/dht/{}/publish", gateway_url, dna_hash);
    println!("Sending to: {}", url);

    let response = client
        .post(&url)
        .json(&request)
        .send()
        .await
        .expect("Failed to send request");

    let status = response.status();
    let body = response.text().await.unwrap();

    println!("Response status: {}", status);
    println!("Response body: {}", body);

    assert_eq!(status, reqwest::StatusCode::OK);

    let publish_response: PublishResponse = serde_json::from_str(&body).unwrap();

    println!(
        "Result: success={}, queued={}, failed={}",
        publish_response.success, publish_response.queued, publish_response.failed
    );

    // With TempOpStore configured, the op should be queued
    assert!(
        publish_response.success,
        "Expected success=true, got results: {:?}",
        publish_response.results
    );
    assert_eq!(
        publish_response.queued, 1,
        "Expected 1 op to be queued, got {}",
        publish_response.queued
    );
    assert_eq!(publish_response.failed, 0);

    println!("SUCCESS: Op was stored in TempOpStore and kitsune2 publish was triggered");

    // Give time for async publish to complete and check logs manually
    println!("");
    println!("Check gateway logs for:");
    println!("  - 'Stored op in TempOpStore'");
    println!("  - 'Publishing ops to peers'");
}

/// Test that print the base64-encoded DhtOp for manual testing
#[test]
fn print_test_op_for_curl() {
    let op = make_test_chain_op();
    let op_bytes = holochain_serialized_bytes::encode(&op).expect("Failed to encode DhtOp");
    let op_data = base64::engine::general_purpose::STANDARD.encode(&op_bytes);
    let signature = base64::engine::general_purpose::STANDARD.encode(&[0xbb; 64]);

    println!("Copy this JSON for curl testing:");
    println!("");
    println!(
        r#"curl -X POST http://localhost:8090/dht/uhC0k2J3h4yJ17fbOaKJ8muCcpi9r58tqRFVVKFa6PeFqwy84A3ii/publish \
  -H "Content-Type: application/json" \
  -d '{{"ops": [{{"op_data": "{}", "signature": "{}"}}]}}'"#,
        op_data, signature
    );
    println!("");
}
