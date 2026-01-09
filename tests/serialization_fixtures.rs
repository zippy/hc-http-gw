//! Serialization Fixture Tests for DhtOp Compatibility
//!
//! These tests serve two purposes:
//! 1. Generate known-good byte fixtures for all ChainOp variants
//! 2. Validate that external byte fixtures (from fishy extension) decode correctly
//!
//! Run with: cargo test --test serialization_fixtures -- --nocapture
//! to see the hex output of each fixture.

use base64::Engine;
use holochain_serialized_bytes::prelude::{decode, encode, SerializedBytes, UnsafeBytes};
use holochain_types::dht_op::{ChainOp, DhtOp};
use holochain_types::prelude::*;

/// Helper to create deterministic test hashes
fn make_hash_32(seed: u8) -> Vec<u8> {
    vec![seed; 32]
}

fn make_agent_pub_key(seed: u8) -> AgentPubKey {
    AgentPubKey::from_raw_32(make_hash_32(seed))
}

fn make_action_hash(seed: u8) -> ActionHash {
    ActionHash::from_raw_32(make_hash_32(seed))
}

fn make_entry_hash(seed: u8) -> EntryHash {
    EntryHash::from_raw_32(make_hash_32(seed))
}

fn make_any_linkable_hash(seed: u8) -> AnyLinkableHash {
    AnyLinkableHash::from(make_entry_hash(seed))
}

fn make_signature() -> Signature {
    Signature::from([0xaa; 64])
}

fn make_timestamp() -> Timestamp {
    // Fixed timestamp for deterministic output
    Timestamp::from_micros(1704067200000000) // 2024-01-01 00:00:00 UTC
}

fn make_entry_rate_weight() -> EntryRateWeight {
    EntryRateWeight {
        bucket_id: 0,
        units: 0,
        rate_bytes: 0,
    }
}

fn make_rate_weight() -> RateWeight {
    RateWeight {
        bucket_id: 0,
        units: 0,
    }
}

fn make_app_entry_def() -> AppEntryDef {
    AppEntryDef {
        entry_index: 0.into(),
        zome_index: 0.into(),
        visibility: EntryVisibility::Public,
    }
}

fn make_entry() -> Entry {
    let entry_bytes = UnsafeBytes::from(vec![1u8, 2, 3, 4, 5]);
    Entry::App(AppEntryBytes(SerializedBytes::from(entry_bytes)))
}

// ============================================================================
// Action Builders for Each Type
// ============================================================================

fn make_create_action() -> Create<EntryRateWeight> {
    Create {
        author: make_agent_pub_key(0x11),
        timestamp: make_timestamp(),
        action_seq: 5,
        prev_action: make_action_hash(0x22),
        entry_type: EntryType::App(make_app_entry_def()),
        entry_hash: make_entry_hash(0x33),
        weight: make_entry_rate_weight(),
    }
}

fn make_update_action() -> Update<EntryRateWeight> {
    Update {
        author: make_agent_pub_key(0x11),
        timestamp: make_timestamp(),
        action_seq: 6,
        prev_action: make_action_hash(0x22),
        entry_type: EntryType::App(make_app_entry_def()),
        entry_hash: make_entry_hash(0x33),
        original_action_address: make_action_hash(0x44),
        original_entry_address: make_entry_hash(0x55),
        weight: make_entry_rate_weight(),
    }
}

fn make_delete_action() -> Delete<RateWeight> {
    Delete {
        author: make_agent_pub_key(0x11),
        timestamp: make_timestamp(),
        action_seq: 7,
        prev_action: make_action_hash(0x22),
        deletes_address: make_action_hash(0x44),
        deletes_entry_address: make_entry_hash(0x55),
        weight: make_rate_weight(),
    }
}

fn make_create_link_action() -> CreateLink<RateWeight> {
    CreateLink {
        author: make_agent_pub_key(0x11),
        timestamp: make_timestamp(),
        action_seq: 8,
        prev_action: make_action_hash(0x22),
        base_address: make_any_linkable_hash(0x33),
        target_address: make_any_linkable_hash(0x44),
        zome_index: 0.into(),
        link_type: 0.into(),
        tag: LinkTag::new(vec![0x01, 0x02, 0x03]),
        weight: make_rate_weight(),
    }
}

fn make_delete_link_action() -> DeleteLink {
    DeleteLink {
        author: make_agent_pub_key(0x11),
        timestamp: make_timestamp(),
        action_seq: 9,
        prev_action: make_action_hash(0x22),
        base_address: make_any_linkable_hash(0x33),
        link_add_address: make_action_hash(0x44),
    }
}

// ============================================================================
// ChainOp Builders for Each Variant
// ============================================================================

fn make_store_record_op() -> DhtOp {
    let action = Action::Create(make_create_action());
    DhtOp::ChainOp(Box::new(ChainOp::StoreRecord(
        make_signature(),
        action,
        RecordEntry::Present(make_entry()),
    )))
}

fn make_store_entry_op() -> DhtOp {
    let action = NewEntryAction::Create(make_create_action());
    DhtOp::ChainOp(Box::new(ChainOp::StoreEntry(
        make_signature(),
        action,
        make_entry(),
    )))
}

fn make_register_agent_activity_op() -> DhtOp {
    let action = Action::Create(make_create_action());
    DhtOp::ChainOp(Box::new(ChainOp::RegisterAgentActivity(
        make_signature(),
        action,
    )))
}

fn make_register_updated_content_op() -> DhtOp {
    let action = make_update_action();
    DhtOp::ChainOp(Box::new(ChainOp::RegisterUpdatedContent(
        make_signature(),
        action,
        RecordEntry::Present(make_entry()),
    )))
}

fn make_register_updated_record_op() -> DhtOp {
    let action = make_update_action();
    DhtOp::ChainOp(Box::new(ChainOp::RegisterUpdatedRecord(
        make_signature(),
        action,
        RecordEntry::Present(make_entry()),
    )))
}

fn make_register_deleted_by_op() -> DhtOp {
    let action = make_delete_action();
    DhtOp::ChainOp(Box::new(ChainOp::RegisterDeletedBy(
        make_signature(),
        action,
    )))
}

fn make_register_deleted_entry_action_op() -> DhtOp {
    let action = make_delete_action();
    DhtOp::ChainOp(Box::new(ChainOp::RegisterDeletedEntryAction(
        make_signature(),
        action,
    )))
}

fn make_register_add_link_op() -> DhtOp {
    let action = make_create_link_action();
    DhtOp::ChainOp(Box::new(ChainOp::RegisterAddLink(
        make_signature(),
        action,
    )))
}

fn make_register_remove_link_op() -> DhtOp {
    let action = make_delete_link_action();
    DhtOp::ChainOp(Box::new(ChainOp::RegisterRemoveLink(
        make_signature(),
        action,
    )))
}

// ============================================================================
// Fixture Generation Tests - Print Known-Good Bytes
// ============================================================================

fn encode_and_print(name: &str, op: &DhtOp) -> Vec<u8> {
    let bytes = encode(op).expect("Failed to encode DhtOp");
    let base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();

    println!("\n=== {} ===", name);
    println!("Bytes length: {}", bytes.len());
    println!("Base64: {}", base64);
    println!("Hex: {}", hex);

    // Verify round-trip
    let decoded: DhtOp = decode(&bytes).expect("Failed to decode DhtOp");
    assert_eq!(op, &decoded, "Round-trip failed for {}", name);

    bytes
}

#[test]
fn generate_store_record_fixture() {
    let op = make_store_record_op();
    encode_and_print("StoreRecord", &op);
}

#[test]
fn generate_store_entry_fixture() {
    let op = make_store_entry_op();
    encode_and_print("StoreEntry", &op);
}

#[test]
fn generate_register_agent_activity_fixture() {
    let op = make_register_agent_activity_op();
    encode_and_print("RegisterAgentActivity", &op);
}

#[test]
fn generate_register_updated_content_fixture() {
    let op = make_register_updated_content_op();
    encode_and_print("RegisterUpdatedContent", &op);
}

#[test]
fn generate_register_updated_record_fixture() {
    let op = make_register_updated_record_op();
    encode_and_print("RegisterUpdatedRecord", &op);
}

#[test]
fn generate_register_deleted_by_fixture() {
    let op = make_register_deleted_by_op();
    encode_and_print("RegisterDeletedBy", &op);
}

#[test]
fn generate_register_deleted_entry_action_fixture() {
    let op = make_register_deleted_entry_action_op();
    encode_and_print("RegisterDeletedEntryAction", &op);
}

#[test]
fn generate_register_add_link_fixture() {
    let op = make_register_add_link_op();
    encode_and_print("RegisterAddLink", &op);
}

#[test]
fn generate_register_remove_link_fixture() {
    let op = make_register_remove_link_op();
    encode_and_print("RegisterRemoveLink", &op);
}

// ============================================================================
// External Fixture Validation - Test Bytes from Fishy Extension
// ============================================================================

/// Test helper to decode external bytes and report detailed errors
fn try_decode_external(name: &str, base64_bytes: &str) -> Result<DhtOp, String> {
    // Decode base64
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64_bytes)
        .map_err(|e| format!("Base64 decode failed: {}", e))?;

    println!("\n=== Testing External: {} ===", name);
    println!("Input bytes length: {}", bytes.len());
    println!("Input hex: {}", bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>());

    // Try to decode as DhtOp
    let op: DhtOp = decode(&bytes)
        .map_err(|e| format!("DhtOp decode failed: {:?}", e))?;

    println!("Successfully decoded as: {:?}", std::mem::discriminant(&op));

    Ok(op)
}

/// Placeholder test - add fishy-generated base64 here to test compatibility
#[test]
fn test_external_store_record_fixture() {
    // TODO: Replace with actual base64 from fishy extension
    // Run this test with fishy-generated bytes to verify compatibility
    let fishy_base64 = "";

    if fishy_base64.is_empty() {
        println!("SKIPPED: No external fixture provided for StoreRecord");
        println!("To test, add base64-encoded bytes from fishy extension");
        return;
    }

    match try_decode_external("StoreRecord (fishy)", fishy_base64) {
        Ok(op) => {
            println!("SUCCESS: Fishy StoreRecord decoded correctly");
            if let DhtOp::ChainOp(chain_op) = op {
                println!("ChainOp variant: {:?}", std::mem::discriminant(&*chain_op));
            }
        }
        Err(e) => panic!("FAILED: {}", e),
    }
}

#[test]
fn test_external_register_agent_activity_fixture() {
    // TODO: Replace with actual base64 from fishy extension
    let fishy_base64 = "";

    if fishy_base64.is_empty() {
        println!("SKIPPED: No external fixture provided for RegisterAgentActivity");
        return;
    }

    match try_decode_external("RegisterAgentActivity (fishy)", fishy_base64) {
        Ok(_) => println!("SUCCESS: Fishy RegisterAgentActivity decoded correctly"),
        Err(e) => panic!("FAILED: {}", e),
    }
}

// ============================================================================
// Comparison Test - Compare Fishy Output to Known-Good
// ============================================================================

/// Compare external bytes to known-good bytes and report differences
fn compare_bytes(name: &str, fishy_bytes: &[u8], rust_bytes: &[u8]) {
    println!("\n=== Comparing: {} ===", name);
    println!("Fishy length: {}, Rust length: {}", fishy_bytes.len(), rust_bytes.len());

    if fishy_bytes == rust_bytes {
        println!("MATCH: Bytes are identical");
        return;
    }

    // Find first difference
    for (i, (f, r)) in fishy_bytes.iter().zip(rust_bytes.iter()).enumerate() {
        if f != r {
            println!("DIFF at byte {}: fishy=0x{:02x}, rust=0x{:02x}", i, f, r);

            // Print context around difference
            let start = i.saturating_sub(5);
            let end = (i + 10).min(fishy_bytes.len().max(rust_bytes.len()));

            println!("Fishy context [{}-{}]: {:?}", start, end, &fishy_bytes[start..end.min(fishy_bytes.len())]);
            println!("Rust context [{}-{}]: {:?}", start, end, &rust_bytes[start..end.min(rust_bytes.len())]);
            break;
        }
    }

    // Length difference
    if fishy_bytes.len() != rust_bytes.len() {
        println!("LENGTH DIFF: fishy has {} bytes, rust has {} bytes",
                 fishy_bytes.len(), rust_bytes.len());
    }
}

#[test]
fn test_compare_store_record_bytes() {
    // Generate known-good bytes
    let rust_op = make_store_record_op();
    let rust_bytes = encode(&rust_op).expect("Failed to encode");

    // TODO: Add fishy bytes here
    let fishy_base64 = "";

    if fishy_base64.is_empty() {
        println!("SKIPPED: No fishy bytes to compare for StoreRecord");
        println!("Rust bytes (base64): {}", base64::engine::general_purpose::STANDARD.encode(&rust_bytes));
        return;
    }

    let fishy_bytes = base64::engine::general_purpose::STANDARD
        .decode(fishy_base64)
        .expect("Invalid base64");

    compare_bytes("StoreRecord", &fishy_bytes, &rust_bytes);
}

// ============================================================================
// Structural Analysis - Print Exact Field Layout
// ============================================================================

#[test]
fn analyze_create_action_structure() {
    println!("\n=== Create Action Structure Analysis ===\n");

    let action = make_create_action();
    let action_enum = Action::Create(action.clone());

    // Encode just the action
    let action_bytes = encode(&action_enum).expect("Failed to encode action");
    println!("Action (internally tagged enum):");
    println!("  Total bytes: {}", action_bytes.len());
    println!("  Hex: {}", action_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>());

    // Print field by field
    println!("\nExpected fields in Create action:");
    println!("  type: \"Create\" (internally tagged)");
    println!("  author: AgentPubKey (39 bytes with prefix)");
    println!("  timestamp: i64 microseconds");
    println!("  action_seq: u32");
    println!("  prev_action: ActionHash (39 bytes with prefix)");
    println!("  entry_type: EntryType enum");
    println!("  entry_hash: EntryHash (39 bytes with prefix)");
    println!("  weight: EntryRateWeight {{ bucket_id: u8, units: u64, rate_bytes: u64 }}");

    // Decode and verify
    let decoded: Action = decode(&action_bytes).expect("Failed to decode");
    assert_eq!(action_enum, decoded);
    println!("\nRound-trip: SUCCESS");
}

#[test]
fn analyze_create_link_action_structure() {
    println!("\n=== CreateLink Action Structure Analysis ===\n");

    let action = make_create_link_action();
    let action_enum = Action::CreateLink(action.clone());

    let action_bytes = encode(&action_enum).expect("Failed to encode action");
    println!("Action (internally tagged enum):");
    println!("  Total bytes: {}", action_bytes.len());
    println!("  Hex: {}", action_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>());

    println!("\nExpected fields in CreateLink action:");
    println!("  type: \"CreateLink\" (internally tagged)");
    println!("  author: AgentPubKey (39 bytes)");
    println!("  timestamp: i64 microseconds");
    println!("  action_seq: u32");
    println!("  prev_action: ActionHash (39 bytes)");
    println!("  base_address: AnyLinkableHash (39 bytes)");
    println!("  target_address: AnyLinkableHash (39 bytes)");
    println!("  zome_index: u8");
    println!("  link_type: u8");
    println!("  tag: LinkTag (bytes)");
    println!("  weight: RateWeight {{ bucket_id: u8, units: u64 }}");

    let decoded: Action = decode(&action_bytes).expect("Failed to decode");
    assert_eq!(action_enum, decoded);
    println!("\nRound-trip: SUCCESS");
}

#[test]
fn analyze_chain_op_structure() {
    println!("\n=== ChainOp Enum Structure Analysis ===\n");

    // RegisterAgentActivity is the simplest - just (Signature, Action)
    let action = Action::Create(make_create_action());
    let chain_op = ChainOp::RegisterAgentActivity(make_signature(), action);

    let op_bytes = encode(&chain_op).expect("Failed to encode");
    println!("ChainOp::RegisterAgentActivity:");
    println!("  Total bytes: {}", op_bytes.len());
    println!("  Hex: {}", op_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>());

    println!("\nExpected structure (externally tagged enum, tuple variant):");
    println!("  {{ \"RegisterAgentActivity\": [signature, action] }}");
    println!("  - signature: 64 bytes raw");
    println!("  - action: internally tagged enum with type field");

    let decoded: ChainOp = decode(&op_bytes).expect("Failed to decode");
    assert_eq!(chain_op, decoded);
    println!("\nRound-trip: SUCCESS");
}

#[test]
fn analyze_dht_op_wrapper() {
    println!("\n=== DhtOp Wrapper Structure Analysis ===\n");

    let chain_op = ChainOp::RegisterAgentActivity(
        make_signature(),
        Action::Create(make_create_action()),
    );
    let dht_op = DhtOp::ChainOp(Box::new(chain_op));

    let op_bytes = encode(&dht_op).expect("Failed to encode");
    println!("DhtOp::ChainOp:");
    println!("  Total bytes: {}", op_bytes.len());
    println!("  Base64: {}", base64::engine::general_purpose::STANDARD.encode(&op_bytes));

    println!("\nExpected structure:");
    println!("  {{ \"ChainOp\": {{ \"RegisterAgentActivity\": [sig, action] }} }}");

    let decoded: DhtOp = decode(&op_bytes).expect("Failed to decode");
    assert_eq!(dht_op, decoded);
    println!("\nRound-trip: SUCCESS");
}
