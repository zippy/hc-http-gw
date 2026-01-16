//! Test signal endpoint for development/testing.
//!
//! This endpoint allows sending test signals to registered WebSocket clients
//! without requiring a full kitsune2 network setup.

use axum::{extract::State, Json};
use holochain_types::prelude::{AgentPubKey, DnaHash};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use crate::routes::websocket::ServerMessage;
use crate::service::AppState;

/// Request body for sending a test signal.
#[derive(Debug, Deserialize)]
pub struct TestSignalRequest {
    /// DNA hash (HoloHash string format).
    pub dna_hash: String,
    /// Agent public key (HoloHash string format).
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
///   "dna_hash": "uhC0k...",
///   "agent_pubkey": "uhCAk...",
///   "zome_name": "test",
///   "signal": "base64..."
/// }
/// ```
#[tracing::instrument(skip(state))]
pub async fn test_signal(
    State(state): State<AppState>,
    Json(request): Json<TestSignalRequest>,
) -> Json<TestSignalResponse> {
    // Parse the DNA hash and agent pubkey to proper types
    let dna = match DnaHash::try_from(request.dna_hash.as_str()) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                dna = %request.dna_hash,
                error = ?e,
                "Failed to parse DNA hash in test signal"
            );
            return Json(TestSignalResponse {
                success: false,
                message: format!("Invalid DNA hash: {:?}", e),
            });
        }
    };

    let agent = match AgentPubKey::try_from(request.agent_pubkey.as_str()) {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(
                agent = %request.agent_pubkey,
                error = ?e,
                "Failed to parse agent pubkey in test signal"
            );
            return Json(TestSignalResponse {
                success: false,
                message: format!("Invalid agent pubkey: {:?}", e),
            });
        }
    };

    tracing::info!(
        "Test signal request: dna={}, agent={}, zome={}",
        dna,
        agent,
        request.zome_name
    );

    // Create the signal message
    let signal_msg = ServerMessage::Signal {
        dna_hash: request.dna_hash.clone(),
        to_agent: request.agent_pubkey.clone(),
        from_agent: "test".to_string(),
        zome_name: request.zome_name,
        signal: request.signal,
    };

    // Send the signal via the agent proxy manager using proper types
    let sent = state.agent_proxy.send_signal(&dna, &agent, signal_msg).await;

    if sent {
        Json(TestSignalResponse {
            success: true,
            message: "Signal sent to registered client".to_string(),
        })
    } else {
        Json(TestSignalResponse {
            success: false,
            message: format!("No client registered for dna={}, agent={}", dna, agent),
        })
    }
}
