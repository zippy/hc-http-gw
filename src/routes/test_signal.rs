//! Test signal endpoint for development/testing.
//!
//! This endpoint allows sending test signals to registered WebSocket clients
//! without requiring a full kitsune2 network setup.

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::routes::websocket::ServerMessage;
use crate::service::AppState;

/// Request body for sending a test signal.
#[derive(Debug, Deserialize)]
pub struct TestSignalRequest {
    /// Base64-encoded DNA hash.
    pub dna_hash: String,
    /// Base64-encoded agent public key to send signal to.
    pub agent_pubkey: String,
    /// Zome name (for display purposes).
    pub zome_name: String,
    /// Base64-encoded signal payload.
    pub signal: String,
}

/// Response for test signal endpoint.
#[derive(Debug, Serialize)]
pub struct TestSignalResponse {
    /// Whether the signal was sent successfully.
    pub success: bool,
    /// Message describing the result.
    pub message: String,
}

/// Send a test signal to a registered WebSocket client.
///
/// This endpoint is for testing the WebSocket signal forwarding path
/// without requiring a full kitsune2 network setup.
///
/// POST /test/signal
/// ```json
/// {
///   "dna_hash": "base64...",
///   "agent_pubkey": "base64...",
///   "zome_name": "test",
///   "signal": "base64..."
/// }
/// ```
#[tracing::instrument(skip(state))]
pub async fn test_signal(
    State(state): State<AppState>,
    Json(request): Json<TestSignalRequest>,
) -> Json<TestSignalResponse> {
    tracing::info!(
        "Test signal request: dna={}, agent={}, zome={}",
        &request.dna_hash[..20.min(request.dna_hash.len())],
        &request.agent_pubkey[..20.min(request.agent_pubkey.len())],
        request.zome_name
    );

    // Create the signal message
    let signal_msg = ServerMessage::Signal {
        dna_hash: request.dna_hash.clone(),
        from_agent: "test".to_string(),
        zome_name: request.zome_name,
        signal: request.signal,
    };

    // Send the signal via the agent proxy manager
    let sent = state
        .agent_proxy
        .send_signal(&request.dna_hash, &request.agent_pubkey, signal_msg)
        .await;

    if sent {
        Json(TestSignalResponse {
            success: true,
            message: "Signal sent to registered client".to_string(),
        })
    } else {
        Json(TestSignalResponse {
            success: false,
            message: "No client registered for this dna/agent".to_string(),
        })
    }
}
