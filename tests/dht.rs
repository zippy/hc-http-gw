//! Integration tests for DHT endpoints.
//!
//! Tests the /dht/* endpoints that call the dht_util zome.
//!
//! NOTE: These tests must be run serially to avoid init() callback conflicts:
//! ```
//! cargo test --test dht -- --test-threads=1
//! ```

mod setup;
mod sweet;

use holochain::sweettest::SweetConductor;
use holochain_types::prelude::{ActionHash, ActionHashB64};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use setup::TestGateway;
use std::sync::Arc;
use sweet::install_fixture1;

/// Response from create_1 zome function
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateResponse {
    pub created: ActionHashB64,
}

/// Test that GET /dht/{dna_hash}/record/{hash} returns a record that was created
#[tokio::test(flavor = "multi_thread")]
async fn dht_get_record_found() {
    let conductor = SweetConductor::from_standard_config().await;
    let app = install_fixture1(conductor.clone().into(), Some("fixture1".into()))
        .await
        .unwrap();

    let conductor_arc: Arc<holochain::conductor::Conductor> = conductor.clone().into();
    let gateway = TestGateway::spawn(conductor_arc.clone()).await;

    // Get the cell info
    let cell_id = app.all_cells().next().unwrap();
    let dna_hash = cell_id.dna_hash().clone();

    // Create an entry using create_1 zome function
    let create_response: CreateResponse = conductor_arc
        .easy_call_zome(
            &app.agent_key,
            None,
            cell_id,
            "coordinator1".to_string(),
            "create_1".to_string(),
            (),
        )
        .await
        .unwrap();

    println!("Created entry with action hash: {}", create_response.created);

    // Now fetch it via the DHT endpoint
    let url = format!(
        "http://{}/dht/{}/record/{}",
        gateway.address, dna_hash, create_response.created
    );

    println!("Fetching URL: {}", url);
    let response = gateway.client.get(&url).send().await.unwrap();

    println!("Response status: {}", response.status());
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected OK status for existing record"
    );

    let body = response.text().await.unwrap();
    println!("Response body: {}", body);

    // Should contain the record with signed_action
    assert!(
        body.contains("signed_action") || body.contains("action"),
        "Expected record in response, got: {}",
        body
    );
}

/// Test that GET /dht/{dna_hash}/record/{hash} returns null for non-existent record
#[tokio::test(flavor = "multi_thread")]
async fn dht_get_record_not_found() {
    let conductor = SweetConductor::from_standard_config().await;
    let app = install_fixture1(conductor.clone().into(), Some("fixture1".into()))
        .await
        .unwrap();

    let gateway = TestGateway::spawn(conductor.clone().into()).await;

    // Get the DNA hash
    let cell_id = app.all_cells().next().unwrap();
    let dna_hash = cell_id.dna_hash();

    // Create a properly formatted ActionHash (from 32 zero bytes)
    let fake_hash = ActionHash::from_raw_32(vec![0u8; 32]);

    // Request a non-existent record
    let url = format!(
        "http://{}/dht/{}/record/{}",
        gateway.address, dna_hash, fake_hash
    );

    let response = gateway.client.get(&url).send().await.unwrap();

    // Should return OK with null body (record not found)
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected OK status, got {}",
        response.status()
    );

    let body = response.text().await.unwrap();
    assert!(
        body == "null" || body.is_empty(),
        "Expected null for non-existent record, got: {}",
        body
    );
}

/// Test that GET /dht/{dna_hash}/links returns empty array for base with no links
#[tokio::test(flavor = "multi_thread")]
async fn dht_get_links_empty() {
    let conductor = SweetConductor::from_standard_config().await;
    let app = install_fixture1(conductor.clone().into(), Some("fixture1".into()))
        .await
        .unwrap();

    let gateway = TestGateway::spawn(conductor.clone().into()).await;

    // Get the DNA hash
    let cell_id = app.all_cells().next().unwrap();
    let dna_hash = cell_id.dna_hash();

    // Create a base address using agent pubkey's proper string format
    // AnyLinkableHash expects the hash string format (like "uhCAk...")
    let base = app.agent_key.to_string();

    // Request links for this base
    let url = format!(
        "http://{}/dht/{}/links?base={}",
        gateway.address, dna_hash, base
    );

    let response = gateway.client.get(&url).send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();

    println!("Response status: {:?}", status);
    println!("Response body: {:?}", body);

    // Should return OK with empty array
    assert_eq!(status, StatusCode::OK, "Expected OK, got {} - body: {}", status, body);

    // Should be an empty array
    assert!(
        body == "[]" || body.contains("[]"),
        "Expected empty array, got: {}",
        body
    );
}

/// Test that GET /dht/{dna_hash}/links/count returns 0 for base with no links
#[tokio::test(flavor = "multi_thread")]
async fn dht_count_links_zero() {
    let conductor = SweetConductor::from_standard_config().await;
    let app = install_fixture1(conductor.clone().into(), Some("fixture1".into()))
        .await
        .unwrap();

    let gateway = TestGateway::spawn(conductor.clone().into()).await;

    // Get the DNA hash
    let cell_id = app.all_cells().next().unwrap();
    let dna_hash = cell_id.dna_hash();

    // Create a base address using agent pubkey's proper string format
    let base = app.agent_key.to_string();

    // Request link count for this base
    let url = format!(
        "http://{}/dht/{}/links/count?base={}",
        gateway.address, dna_hash, base
    );

    let response = gateway.client.get(&url).send().await.unwrap();

    // Should return OK with count of 0
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected OK, got {}",
        response.status()
    );

    let body = response.text().await.unwrap();
    // Should be 0
    assert_eq!(body, "0", "Expected 0 links, got: {}", body);
}
