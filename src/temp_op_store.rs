//! Temporary OpStore for browser extension publishing.
//!
//! This module provides a temporary in-memory OpStore that holds ops during
//! the kitsune2 publish/fetch cycle. Since browser extension agents are zero-arc,
//! they won't be queried for ops after DHT authorities have fetched them.
//!
//! # Architecture
//!
//! ```text
//! Extension ──POST /publish──► Gateway TempOpStore
//!                                    │
//!                                    ▼
//!                              kitsune2.publish_ops(op_ids)
//!                                    │
//!                                    ▼
//!                              DHT authorities fetch ops
//!                                    │
//!                                    ▼
//!                              Gateway serves from TempOpStore
//!                                    │
//!                                    ▼
//!                              Ops deleted after TTL or fetch
//! ```
//!
//! # TTL Strategy
//!
//! Ops are stored with a 60-second TTL. This is sufficient for:
//! 1. DHT authorities to receive publish notification
//! 2. Fetch the actual op data back from the gateway
//! 3. Store the ops permanently
//!
//! After TTL, ops are cleaned up. This is safe because zero-arc nodes
//! are never DHT authorities, so they won't be queried later.

use bytes::Bytes;
use futures::future::BoxFuture;
use holochain_serialized_bytes::prelude::decode;
use holochain_types::dht_op::{DhtOp, DhtOpHashed};
use holochain_types::prelude::HashableContentExtSync;
use kitsune2_api::{
    BoxFut, Config, DhtArc, DynOpStore, DynOpStoreFactory, K2Result,
    MetaOp, OpId, OpStore, OpStoreFactory, SpaceId, Timestamp,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Time-to-live for stored ops (60 seconds).
const OP_TTL: Duration = Duration::from_secs(60);

/// Cleanup interval for expired ops.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);

/// A stored op with expiry tracking.
#[derive(Debug, Clone)]
struct StoredOpRecord {
    /// The OpId (DhtOpHash + basis location).
    op_id: OpId,
    /// The raw op data (msgpack-encoded DhtOp).
    op_data: Bytes,
    /// When this op was stored.
    stored_at: Instant,
    /// Creation timestamp from the op.
    created_at: Timestamp,
}

/// Factory for creating TempOpStore instances.
#[derive(Debug, Clone)]
pub struct TempOpStoreFactory {
    /// Shared inner store across all spaces.
    /// This allows the publish endpoint to store ops that kitsune2 can retrieve.
    inner: Arc<RwLock<TempOpStoreInner>>,
}

impl TempOpStoreFactory {
    /// Create a new TempOpStoreFactory.
    pub fn create() -> (Self, TempOpStoreHandle) {
        let inner = Arc::new(RwLock::new(TempOpStoreInner::default()));
        let handle = TempOpStoreHandle {
            inner: inner.clone(),
        };
        (Self { inner }, handle)
    }

    /// Create a DynOpStoreFactory.
    pub fn into_dyn(self) -> DynOpStoreFactory {
        Arc::new(self)
    }

    /// Start the background cleanup task.
    pub fn start_cleanup_task(&self) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(CLEANUP_INTERVAL).await;
                let mut lock = inner.write().await;
                let before = lock.ops.len();
                lock.cleanup_expired();
                let after = lock.ops.len();
                if before != after {
                    debug!(
                        removed = before - after,
                        remaining = after,
                        "Cleaned up expired ops"
                    );
                }
            }
        });
    }
}

impl OpStoreFactory for TempOpStoreFactory {
    fn default_config(&self, _config: &mut Config) -> K2Result<()> {
        Ok(())
    }

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        Ok(())
    }

    fn create(
        &self,
        _builder: Arc<kitsune2_api::Builder>,
        space_id: SpaceId,
    ) -> BoxFut<'static, K2Result<DynOpStore>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            info!(?space_id, "Creating TempOpStore for space");
            let store: DynOpStore = Arc::new(TempOpStore { inner, space_id });
            Ok(store)
        })
    }
}

/// Handle for external code to store ops.
///
/// This is used by the publish endpoint to add ops that will be
/// served when kitsune2 peers fetch them.
#[derive(Debug, Clone)]
pub struct TempOpStoreHandle {
    inner: Arc<RwLock<TempOpStoreInner>>,
}

impl TempOpStoreHandle {
    /// Store a raw DhtOp for publishing.
    ///
    /// Parses the op to compute its OpId and stores it for fetch.
    ///
    /// # Arguments
    ///
    /// * `op_bytes` - Msgpack-encoded DhtOp
    ///
    /// # Returns
    ///
    /// The computed OpId on success.
    pub async fn store_op(&self, op_bytes: Bytes) -> Result<OpId, String> {
        // Decode the DhtOp to get its hash and basis
        let op: DhtOp = decode(&op_bytes)
            .map_err(|e| format!("Failed to decode DhtOp: {}", e))?;

        // Compute the DhtOpHash using the hashing trait
        let op_hashed: DhtOpHashed = op.clone().into_hashed();
        let op_hash = op_hashed.hash;

        // Get the basis (determines DHT location)
        let basis = op.dht_basis();

        // Create located OpId (32-byte hash + 4-byte location)
        let op_id = op_hash.to_located_k2_op_id(&basis);

        // Get creation timestamp from the op's action
        let created_at = match &op {
            DhtOp::ChainOp(chain_op) => {
                Timestamp::from_micros(chain_op.action().timestamp().as_micros())
            }
            DhtOp::WarrantOp(warrant_op) => {
                Timestamp::from_micros(warrant_op.warrant().timestamp.as_micros())
            }
        };

        // Store the op
        let record = StoredOpRecord {
            op_id: op_id.clone(),
            op_data: op_bytes,
            stored_at: Instant::now(),
            created_at,
        };

        let mut lock = self.inner.write().await;
        lock.ops.insert(op_id.clone(), record);

        debug!(
            op_id = %op_id,
            basis_loc = basis.get_loc(),
            "Stored op for publishing"
        );

        Ok(op_id)
    }

    /// Get the number of currently stored ops.
    pub async fn op_count(&self) -> usize {
        self.inner.read().await.ops.len()
    }

    /// Clear all stored ops (for testing).
    pub async fn clear(&self) {
        self.inner.write().await.ops.clear();
    }
}

/// Inner storage shared across all TempOpStore instances.
#[derive(Debug, Default)]
struct TempOpStoreInner {
    /// Ops indexed by OpId.
    ops: HashMap<OpId, StoredOpRecord>,
}

impl TempOpStoreInner {
    /// Remove expired ops.
    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        self.ops.retain(|_, record| {
            now.duration_since(record.stored_at) < OP_TTL
        });
    }
}

/// Temporary OpStore implementation for zero-arc publishing.
///
/// This OpStore only implements the methods needed for the publish/fetch cycle:
/// - `retrieve_ops` - serve ops when DHT authorities fetch them
/// - `filter_out_existing_ops` - help kitsune2 know what's new
///
/// Other methods return empty results since zero-arc nodes don't participate
/// in gossip or DHT syncing.
#[derive(Debug)]
struct TempOpStore {
    inner: Arc<RwLock<TempOpStoreInner>>,
    space_id: SpaceId,
}

impl OpStore for TempOpStore {
    fn process_incoming_ops(&self, op_list: Vec<Bytes>) -> BoxFuture<'_, K2Result<Vec<OpId>>> {
        // Zero-arc nodes don't receive ops from the network
        // This is called when WE receive ops, not when others fetch from us
        Box::pin(async move {
            warn!(
                count = op_list.len(),
                "TempOpStore received incoming ops (unexpected for zero-arc node)"
            );
            Ok(vec![])
        })
    }

    fn retrieve_ops(&self, op_ids: Vec<OpId>) -> BoxFuture<'_, K2Result<Vec<MetaOp>>> {
        Box::pin(async move {
            let lock = self.inner.read().await;
            let mut result = Vec::with_capacity(op_ids.len());

            for op_id in &op_ids {
                if let Some(record) = lock.ops.get(op_id) {
                    result.push(MetaOp {
                        op_id: op_id.clone(),
                        op_data: record.op_data.clone(),
                    });
                }
            }

            debug!(
                requested = op_ids.len(),
                found = result.len(),
                "Retrieved ops for fetch"
            );

            Ok(result)
        })
    }

    fn filter_out_existing_ops(&self, op_ids: Vec<OpId>) -> BoxFuture<'_, K2Result<Vec<OpId>>> {
        Box::pin(async move {
            let lock = self.inner.read().await;
            let result: Vec<OpId> = op_ids
                .into_iter()
                .filter(|id| !lock.ops.contains_key(id))
                .collect();
            Ok(result)
        })
    }

    fn retrieve_op_hashes_in_time_slice(
        &self,
        _arc: DhtArc,
        _start: Timestamp,
        _end: Timestamp,
    ) -> BoxFuture<'_, K2Result<(Vec<OpId>, u32)>> {
        // Zero-arc nodes don't participate in gossip
        Box::pin(async move { Ok((vec![], 0)) })
    }

    fn retrieve_op_ids_bounded(
        &self,
        _arc: DhtArc,
        start: Timestamp,
        _limit_bytes: u32,
    ) -> BoxFuture<'_, K2Result<(Vec<OpId>, u32, Timestamp)>> {
        // Zero-arc nodes don't participate in gossip
        Box::pin(async move { Ok((vec![], 0, start)) })
    }

    fn earliest_timestamp_in_arc(
        &self,
        _arc: DhtArc,
    ) -> BoxFuture<'_, K2Result<Option<Timestamp>>> {
        // Zero-arc nodes don't participate in gossip
        Box::pin(async move { Ok(None) })
    }

    fn store_slice_hash(
        &self,
        _arc: DhtArc,
        _slice_index: u64,
        _slice_hash: Bytes,
    ) -> BoxFuture<'_, K2Result<()>> {
        // Zero-arc nodes don't participate in gossip
        Box::pin(async move { Ok(()) })
    }

    fn slice_hash_count(&self, _arc: DhtArc) -> BoxFuture<'_, K2Result<u64>> {
        // Zero-arc nodes don't participate in gossip
        Box::pin(async move { Ok(0) })
    }

    fn retrieve_slice_hash(
        &self,
        _arc: DhtArc,
        _slice_index: u64,
    ) -> BoxFuture<'_, K2Result<Option<Bytes>>> {
        // Zero-arc nodes don't participate in gossip
        Box::pin(async move { Ok(None) })
    }

    fn retrieve_slice_hashes(
        &self,
        _arc: DhtArc,
    ) -> BoxFuture<'_, K2Result<Vec<(u64, Bytes)>>> {
        // Zero-arc nodes don't participate in gossip
        Box::pin(async move { Ok(vec![]) })
    }
}

#[cfg(test)]
mod tests {
    use super::{TempOpStoreFactory, TempOpStoreInner, StoredOpRecord};
    use bytes::Bytes;
    use holochain_types::prelude::*;
    use kitsune2_api::OpId;
    use kitsune2_api::Timestamp as K2Timestamp;
    use std::time::{Duration, Instant};

    fn make_test_create_action() -> Action {
        let author = AgentPubKey::from_raw_32(vec![1; 32]);
        let entry_hash = EntryHash::from_raw_32(vec![2; 32]);
        let prev_action = ActionHash::from_raw_32(vec![3; 32]);

        Action::Create(Create {
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
        })
    }

    fn make_test_chain_op() -> DhtOp {
        let action = make_test_create_action();
        // Create entry bytes using UnsafeBytes
        let entry_bytes = UnsafeBytes::from(vec![1u8, 2, 3]);
        let entry = Entry::App(AppEntryBytes(SerializedBytes::from(entry_bytes)));
        let signature = Signature::from([0xaa; 64]);

        DhtOp::ChainOp(Box::new(ChainOp::StoreRecord(
            signature,
            action,
            RecordEntry::Present(entry),
        )))
    }

    #[tokio::test]
    async fn test_store_and_retrieve_op() {
        let (_factory, handle) = TempOpStoreFactory::create();

        // Create a test op
        let op = make_test_chain_op();
        let op_bytes = holochain_serialized_bytes::prelude::encode(&op).unwrap();

        // Store it
        let op_id = handle.store_op(Bytes::from(op_bytes.clone())).await.unwrap();

        // Should have 1 op
        assert_eq!(handle.op_count().await, 1);

        // Retrieve directly from the inner store (simulating what TempOpStore does)
        let lock = handle.inner.read().await;
        let record = lock.ops.get(&op_id).expect("Op should be stored");
        assert_eq!(record.op_data, Bytes::from(op_bytes));
    }

    #[tokio::test]
    async fn test_clear_ops() {
        let (_factory, handle) = TempOpStoreFactory::create();

        // Store an op
        let op = make_test_chain_op();
        let op_bytes = holochain_serialized_bytes::prelude::encode(&op).unwrap();
        handle.store_op(Bytes::from(op_bytes)).await.unwrap();

        assert_eq!(handle.op_count().await, 1);

        // Clear
        handle.clear().await;

        assert_eq!(handle.op_count().await, 0);
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let (_factory, _handle) = TempOpStoreFactory::create();

        // Create inner directly for testing
        let mut inner = TempOpStoreInner::default();

        // Add an expired op
        let expired_op_id = OpId::from(Bytes::from_static(&[0xaa; 36]));
        inner.ops.insert(
            expired_op_id.clone(),
            StoredOpRecord {
                op_id: expired_op_id.clone(),
                op_data: Bytes::from_static(&[1, 2, 3]),
                stored_at: Instant::now() - Duration::from_secs(120), // 2 minutes ago
                created_at: K2Timestamp::from_micros(0),
            },
        );

        // Add a fresh op
        let fresh_op_id = OpId::from(Bytes::from_static(&[0xbb; 36]));
        inner.ops.insert(
            fresh_op_id.clone(),
            StoredOpRecord {
                op_id: fresh_op_id.clone(),
                op_data: Bytes::from_static(&[4, 5, 6]),
                stored_at: Instant::now(),
                created_at: K2Timestamp::from_micros(0),
            },
        );

        assert_eq!(inner.ops.len(), 2);

        // Cleanup
        inner.cleanup_expired();

        // Only fresh should remain
        assert_eq!(inner.ops.len(), 1);
        assert!(inner.ops.contains_key(&fresh_op_id));
        assert!(!inner.ops.contains_key(&expired_op_id));
    }
}
