//! Publish endpoint for browser extension agents.
//!
//! Allows zero-arc browser agents to publish their source chain data
//! to the DHT via the gateway.

use crate::service::AppState;
use crate::{HcHttpGatewayError, HcHttpGatewayResult};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use bytes::Bytes;
use holochain_types::dht_op::DhtOp;
use holochain_types::prelude::{DnaHash, ExternIO};
use kitsune2_api::OpId;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ============================================================================
// Request/Response types
// ============================================================================

/// Path parameters for publish endpoint.
#[derive(Debug, Deserialize)]
pub struct PublishPath {
    /// DNA hash (base64 encoded).
    pub dna_hash: String,
}

/// A signed DhtOp ready for publishing.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignedDhtOp {
    /// The serialized DhtOp (msgpack encoded, base64 string).
    pub op_data: String,
    /// The signature over the op's action hash.
    pub signature: String,
}

/// Request body for publishing DhtOps.
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishRequest {
    /// List of signed ops to publish.
    pub ops: Vec<SignedDhtOp>,
}

/// Result for a single op publish attempt.
#[derive(Debug, Serialize, Deserialize)]
pub struct OpPublishResult {
    /// Whether this op was successfully queued for publishing.
    pub success: bool,
    /// Error message if failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response body for publish endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishResponse {
    /// Overall success status (true if all ops were stored AND published to at least one peer).
    pub success: bool,
    /// Number of ops successfully stored in TempOpStore.
    pub queued: usize,
    /// Number of ops that failed to store.
    pub failed: usize,
    /// Number of ops actually published to DHT peers (0 means retry needed).
    pub published: usize,
    /// Per-op results (same order as request).
    pub results: Vec<OpPublishResult>,
}

// ============================================================================
// Endpoint implementation
// ============================================================================

/// POST /dht/{dna_hash}/publish
///
/// Publish DhtOps from a browser extension agent to the DHT.
///
/// # Request
///
/// ```json
/// {
///   "ops": [
///     {
///       "op_data": "<base64 msgpack encoded DhtOp>",
///       "signature": "<base64 64-byte Ed25519 signature>"
///     }
///   ]
/// }
/// ```
///
/// # Response
///
/// ```json
/// {
///   "success": true,
///   "queued": 3,
///   "failed": 0,
///   "results": [
///     { "success": true },
///     { "success": true },
///     { "success": true }
///   ]
/// }
/// ```
#[tracing::instrument(skip(state, headers, body))]
pub async fn dht_publish(
    Path(path): Path<PublishPath>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PublishRequest>,
) -> HcHttpGatewayResult<Json<PublishResponse>> {
    // Verify session if authenticator is configured
    verify_session(&state, &headers).await?;

    // Parse DNA hash
    let dna_hash = DnaHash::try_from(path.dna_hash.clone())
        .map_err(|_| HcHttpGatewayError::RequestMalformed("Invalid DNA hash".to_string()))?;

    info!(
        dna = %dna_hash,
        op_count = body.ops.len(),
        "Processing publish request"
    );

    let mut results = Vec::with_capacity(body.ops.len());
    let mut queued = 0;
    let mut failed = 0;
    let mut processed_ops: Vec<ProcessedOp> = Vec::new();

    // Phase 1: Store all ops in TempOpStore
    for signed_op in &body.ops {
        match process_signed_op(&dna_hash, signed_op, &state).await {
            Ok(processed) => {
                queued += 1;
                processed_ops.push(processed);
                results.push(OpPublishResult {
                    success: true,
                    error: None,
                });
            }
            Err(e) => {
                failed += 1;
                warn!(error = %e, "Failed to process op for publishing");
                results.push(OpPublishResult {
                    success: false,
                    error: Some(e.to_string()),
                });
            }
        }
    }

    // Phase 2: Trigger kitsune2 publish for all stored ops
    // Group ops by basis location and publish to DHT authorities
    let mut published = 0usize;

    if !processed_ops.is_empty() {
        if let Some(gateway_kitsune) = &state.gateway_kitsune {
            // Group ops by basis location for efficient publishing
            use std::collections::HashMap;
            let mut ops_by_loc: HashMap<u32, Vec<OpId>> = HashMap::new();
            for op in &processed_ops {
                ops_by_loc.entry(op.basis_loc).or_default().push(op.op_id.clone());
            }

            // Publish each group to the appropriate DHT authorities
            for (basis_loc, op_ids) in ops_by_loc {
                let op_count = op_ids.len();
                match gateway_kitsune.publish_ops(&dna_hash, op_ids, basis_loc).await {
                    Ok(peer_count) => {
                        if peer_count > 0 {
                            // Only count as published if at least one peer received it
                            published += op_count;
                            debug!(
                                dna = %dna_hash,
                                basis_loc,
                                peer_count,
                                op_count,
                                "Published ops to DHT authorities"
                            );
                        } else {
                            warn!(
                                dna = %dna_hash,
                                basis_loc,
                                op_count,
                                "No peers available to publish ops - retry needed"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            dna = %dna_hash,
                            basis_loc,
                            error = %e,
                            "Failed to publish ops to DHT authorities"
                        );
                    }
                }
            }
        } else {
            warn!(
                dna = %dna_hash,
                "No GatewayKitsune configured - ops stored but not published to network"
            );
        }
    }

    // Success requires: no storage failures AND at least some ops published to peers
    let success = failed == 0 && (queued == 0 || published > 0);

    info!(
        dna = %dna_hash,
        queued,
        failed,
        published,
        success,
        "Publish request completed"
    );

    Ok(Json(PublishResponse {
        success,
        queued,
        failed,
        published,
        results,
    }))
}

/// Result of processing and storing an op.
struct ProcessedOp {
    op_id: OpId,
    basis_loc: u32,
}

/// Process a single signed DhtOp for publishing.
async fn process_signed_op(
    dna_hash: &DnaHash,
    signed_op: &SignedDhtOp,
    state: &AppState,
) -> Result<ProcessedOp, HcHttpGatewayError> {
    // Decode op data from base64
    let op_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &signed_op.op_data,
    )
    .map_err(|e| {
        HcHttpGatewayError::RequestMalformed(format!("Invalid op_data base64: {}", e))
    })?;

    // Decode signature from base64
    let sig_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &signed_op.signature,
    )
    .map_err(|e| {
        HcHttpGatewayError::RequestMalformed(format!("Invalid signature base64: {}", e))
    })?;

    // Validate signature length
    if sig_bytes.len() != 64 {
        return Err(HcHttpGatewayError::RequestMalformed(format!(
            "Invalid signature length: expected 64 bytes, got {}",
            sig_bytes.len()
        )));
    }

    // Decode DhtOp from msgpack bytes to get basis location for publishing
    let extern_io = ExternIO::from(op_bytes.clone());
    let op: DhtOp = extern_io.decode().map_err(|e| {
        HcHttpGatewayError::RequestMalformed(format!("Failed to decode DhtOp: {}", e))
    })?;

    // Get the chain op (browser extensions only produce ChainOps, not WarrantOps)
    let chain_op = match &op {
        DhtOp::ChainOp(op) => op,
        DhtOp::WarrantOp(_) => {
            return Err(HcHttpGatewayError::RequestMalformed(
                "WarrantOps are not supported for browser extension publishing".to_string(),
            ));
        }
    };

    // Get the action from the op for validation
    let action = chain_op.action();
    let author = action.author();

    // Get the basis location for DHT routing
    let basis = op.dht_basis();
    let basis_loc = basis.get_loc();

    debug!(
        dna = %dna_hash,
        author = %author,
        action_type = ?action.action_type(),
        basis_loc,
        "Validated DhtOp"
    );

    // TODO: Verify signature matches the action hash
    // This requires computing the action hash and verifying with the author's public key
    // For MVP, we trust that the browser extension has correctly signed the op

    // Store the op in TempOpStore if available
    let temp_op_store = state.temp_op_store.as_ref().ok_or_else(|| {
        HcHttpGatewayError::InternalServerError(
            "TempOpStore not configured - publishing not available".to_string(),
        )
    })?;

    let op_id = temp_op_store
        .store_op(Bytes::from(op_bytes))
        .await
        .map_err(|e| {
            HcHttpGatewayError::InternalServerError(format!("Failed to store op: {}", e))
        })?;

    debug!(
        dna = %dna_hash,
        op_id = %op_id,
        basis_loc,
        "Stored op in TempOpStore"
    );

    Ok(ProcessedOp { op_id, basis_loc })
}

/// Verify session from headers if authenticator is configured.
async fn verify_session(state: &AppState, headers: &HeaderMap) -> HcHttpGatewayResult<()> {
    if let Some(authenticator) = &state.authenticator {
        // Get session token from cookie or header
        let token = headers
            .get("X-Session-Token")
            .and_then(|v| v.to_str().ok())
            .or_else(|| {
                headers
                    .get("Cookie")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|cookies| {
                        cookies.split(';').find_map(|c| {
                            let c = c.trim();
                            if c.starts_with("session=") {
                                Some(&c[8..])
                            } else {
                                None
                            }
                        })
                    })
            })
            .ok_or_else(|| {
                HcHttpGatewayError::AuthenticationFailed("No session token provided".to_string())
            })?;

        authenticator.verify_session(token).await.map_err(|e| {
            HcHttpGatewayError::AuthenticationFailed(format!("Session verification failed: {}", e))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_request_deserialization() {
        let json = r#"{
            "ops": [
                {
                    "op_data": "gqR0eXBlpUNyZWF0ZQ==",
                    "signature": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                }
            ]
        }"#;

        let request: PublishRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.ops.len(), 1);
    }

    #[test]
    fn test_publish_response_serialization() {
        let response = PublishResponse {
            success: true,
            queued: 2,
            failed: 0,
            results: vec![
                OpPublishResult {
                    success: true,
                    error: None,
                },
                OpPublishResult {
                    success: true,
                    error: None,
                },
            ],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"queued\":2"));
    }
}
