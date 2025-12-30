//! Authentication endpoints.
//!
//! Provides challenge-response authentication for browser extension agents.

use crate::auth::{AuthChallenge, AuthVerifyRequest};
use crate::service::AppState;
use crate::{HcHttpGatewayError, HcHttpGatewayResult};
use axum::extract::State;
use axum::Json;
use holochain_types::prelude::AgentPubKey;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default challenge TTL (60 seconds).
const CHALLENGE_TTL_SECS: u64 = 60;

/// Pending challenges awaiting verification.
/// In production, this would be in Redis or similar.
static PENDING_CHALLENGES: std::sync::LazyLock<RwLock<HashMap<String, PendingChallenge>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

struct PendingChallenge {
    nonce: Vec<u8>,
    expires_at: u64,
}

/// POST /auth/challenge
///
/// Request a challenge nonce for authentication.
#[tracing::instrument]
pub async fn auth_challenge() -> HcHttpGatewayResult<Json<AuthChallenge>> {
    // Generate random nonce
    use rand::Rng;
    let mut rng = rand::rng();
    let nonce_bytes: [u8; 32] = rng.random();
    let nonce = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &nonce_bytes);

    let expires_at = current_timestamp() + CHALLENGE_TTL_SECS;

    // Store pending challenge
    PENDING_CHALLENGES.write().unwrap().insert(
        nonce.clone(),
        PendingChallenge {
            nonce: nonce_bytes.to_vec(),
            expires_at,
        },
    );

    Ok(Json(AuthChallenge { nonce, expires_at }))
}

/// POST /auth/verify
///
/// Verify a signed challenge and return a session token.
#[tracing::instrument(skip(state))]
pub async fn auth_verify(
    State(state): State<AppState>,
    Json(request): Json<AuthVerifyRequest>,
) -> HcHttpGatewayResult<Json<AuthResponse>> {
    // Look up pending challenge
    let pending = {
        let challenges = PENDING_CHALLENGES.read().unwrap();
        challenges.get(&request.nonce).cloned()
    };

    let pending = pending
        .ok_or_else(|| HcHttpGatewayError::AuthenticationFailed("Invalid nonce".to_string()))?;

    // Check expiration
    if pending.expires_at < current_timestamp() {
        PENDING_CHALLENGES.write().unwrap().remove(&request.nonce);
        return Err(HcHttpGatewayError::AuthenticationFailed(
            "Challenge expired".to_string(),
        ));
    }

    // Remove used challenge
    PENDING_CHALLENGES.write().unwrap().remove(&request.nonce);

    // Parse agent public key
    let agent_pub_key = AgentPubKey::try_from(request.agent_pub_key.clone()).map_err(|e| {
        HcHttpGatewayError::AuthenticationFailed(format!("Invalid agent public key: {}", e))
    })?;

    // Parse signature
    let signature = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &request.signature)
        .map_err(|_| HcHttpGatewayError::AuthenticationFailed("Invalid signature encoding".to_string()))?;

    // Get authenticator from state and verify
    let authenticator = state.authenticator.as_ref().ok_or_else(|| {
        HcHttpGatewayError::AuthenticationFailed("Authentication not configured".to_string())
    })?;

    let parsed_request = crate::auth::types::ParsedAuthRequest {
        agent_pub_key,
        signature,
        nonce: pending.nonce,
    };

    let session = authenticator
        .authenticate(parsed_request)
        .await
        .map_err(|e| HcHttpGatewayError::AuthenticationFailed(e.to_string()))?;

    Ok(Json(AuthResponse {
        session_token: session.token,
        expires_at: session.expires_at,
    }))
}

/// Cleanup expired challenges.
pub fn cleanup_expired_challenges() {
    let now = current_timestamp();
    PENDING_CHALLENGES
        .write()
        .unwrap()
        .retain(|_, c| c.expires_at > now);
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl Clone for PendingChallenge {
    fn clone(&self) -> Self {
        Self {
            nonce: self.nonce.clone(),
            expires_at: self.expires_at,
        }
    }
}

/// Auth response structure.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuthResponse {
    /// Session token for subsequent requests.
    pub session_token: String,
    /// When the session expires (Unix timestamp in seconds).
    pub expires_at: u64,
}
