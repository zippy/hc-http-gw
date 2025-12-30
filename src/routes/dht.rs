//! DHT endpoints.
//!
//! Provides direct DHT access for browser extension agents via the dht_util zome.

use crate::app_selection::try_get_valid_app;
use crate::service::AppState;
use crate::transcode::{base64_json_to_hsb, hsb_to_json};
use crate::{HcHttpGatewayError, HcHttpGatewayResult};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use holochain_client::CellInfo;
use holochain_types::dna::DnaHash;
use serde::Deserialize;

/// DHT utility zome name.
const DHT_UTIL_ZOME: &str = "dht_util";

/// Path parameters for record/details endpoints.
#[derive(Debug, Deserialize)]
pub struct RecordPath {
    /// DNA hash.
    pub dna_hash: String,
    /// Action or entry hash.
    pub hash: String,
}

/// Query parameters for links endpoints.
#[derive(Debug, Deserialize)]
pub struct LinksQuery {
    /// Base address (base64 encoded).
    pub base: String,
    /// Optional link type filter.
    #[serde(rename = "type")]
    pub link_type: Option<u16>,
    /// Optional tag prefix (base64 encoded).
    pub tag: Option<String>,
}

/// Path parameters for links endpoints.
#[derive(Debug, Deserialize)]
pub struct LinksPath {
    /// DNA hash.
    pub dna_hash: String,
}

/// GET /dht/{dna_hash}/record/{hash}
///
/// Get a record by hash.
#[tracing::instrument(skip(state, headers))]
pub async fn dht_get_record(
    Path(path): Path<RecordPath>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> HcHttpGatewayResult<String> {
    // Verify session if authenticator is configured
    verify_session(&state, &headers).await?;

    // Parse DNA hash
    let dna_hash = DnaHash::try_from(path.dna_hash.clone())
        .map_err(|_| HcHttpGatewayError::RequestMalformed("Invalid DNA hash".to_string()))?;

    // Build payload for dht_get_record
    let payload = serde_json::json!({
        "hash": path.hash,
        "options": {
            "strategy": "Network"
        }
    });

    call_dht_util_zome(&state, dna_hash, "dht_get_record", payload).await
}

/// GET /dht/{dna_hash}/details/{hash}
///
/// Get details for a hash (including updates and deletes).
#[tracing::instrument(skip(state, headers))]
pub async fn dht_get_details(
    Path(path): Path<RecordPath>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> HcHttpGatewayResult<String> {
    // Verify session if authenticator is configured
    verify_session(&state, &headers).await?;

    // Parse DNA hash
    let dna_hash = DnaHash::try_from(path.dna_hash.clone())
        .map_err(|_| HcHttpGatewayError::RequestMalformed("Invalid DNA hash".to_string()))?;

    // Build payload for dht_get_details
    let payload = serde_json::json!({
        "hash": path.hash,
        "options": {
            "strategy": "Network"
        }
    });

    call_dht_util_zome(&state, dna_hash, "dht_get_details", payload).await
}

/// GET /dht/{dna_hash}/links
///
/// Get links from a base address.
#[tracing::instrument(skip(state, headers))]
pub async fn dht_get_links(
    Path(path): Path<LinksPath>,
    Query(query): Query<LinksQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> HcHttpGatewayResult<String> {
    // Verify session if authenticator is configured
    verify_session(&state, &headers).await?;

    // Parse DNA hash
    let dna_hash = DnaHash::try_from(path.dna_hash.clone())
        .map_err(|_| HcHttpGatewayError::RequestMalformed("Invalid DNA hash".to_string()))?;

    // Build payload for dht_get_links
    let mut payload = serde_json::json!({
        "base": query.base
    });

    if let Some(link_type) = query.link_type {
        payload["link_type"] = serde_json::json!(link_type);
    }

    if let Some(tag) = query.tag {
        // Decode base64 tag prefix
        let tag_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &tag)
            .map_err(|_| {
                HcHttpGatewayError::RequestMalformed("Invalid tag encoding".to_string())
            })?;
        payload["tag_prefix"] = serde_json::json!(tag_bytes);
    }

    call_dht_util_zome(&state, dna_hash, "dht_get_links", payload).await
}

/// GET /dht/{dna_hash}/links/count
///
/// Count links from a base address.
#[tracing::instrument(skip(state, headers))]
pub async fn dht_count_links(
    Path(path): Path<LinksPath>,
    Query(query): Query<LinksQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> HcHttpGatewayResult<String> {
    // Verify session if authenticator is configured
    verify_session(&state, &headers).await?;

    // Parse DNA hash
    let dna_hash = DnaHash::try_from(path.dna_hash.clone())
        .map_err(|_| HcHttpGatewayError::RequestMalformed("Invalid DNA hash".to_string()))?;

    // Build payload for dht_count_links
    let mut payload = serde_json::json!({
        "base": query.base
    });

    if let Some(link_type) = query.link_type {
        payload["link_type"] = serde_json::json!(link_type);
    }

    if let Some(tag) = query.tag {
        let tag_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &tag)
            .map_err(|_| {
                HcHttpGatewayError::RequestMalformed("Invalid tag encoding".to_string())
            })?;
        payload["tag_prefix"] = serde_json::json!(tag_bytes);
    }

    call_dht_util_zome(&state, dna_hash, "dht_count_links", payload).await
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

/// Call a function in the dht_util zome.
async fn call_dht_util_zome(
    state: &AppState,
    dna_hash: DnaHash,
    fn_name: &str,
    payload: serde_json::Value,
) -> HcHttpGatewayResult<String> {
    // Find an app with this DNA that has the dht_util zome
    // For now, we'll use any app that contains this DNA
    let app_info = try_get_valid_app(
        dna_hash.clone(),
        "dht_util".to_string(), // coordinator identifier
        state.app_info_cache.clone(),
        &state.configuration.allowed_app_ids,
        state.admin_call.clone(),
    )
    .await?;

    // Transcode payload to ExternIO
    let payload_str = serde_json::to_string(&payload)
        .map_err(|e| HcHttpGatewayError::RequestMalformed(format!("Invalid payload: {}", e)))?;
    let payload_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload_str.as_bytes());
    let zome_call_payload = base64_json_to_hsb(Some(payload_b64))?;

    // Get cell id
    let cell_id = app_info
        .cell_info
        .values()
        .flatten()
        .find_map(|cell_info| match cell_info {
            CellInfo::Provisioned(provisioned_cell) => {
                if *provisioned_cell.cell_id.dna_hash() == dna_hash {
                    Some(provisioned_cell.cell_id.clone())
                } else {
                    None
                }
            }
            _ => None,
        })
        .ok_or_else(|| {
            HcHttpGatewayError::RequestMalformed("DNA not found in any installed app".to_string())
        })?;

    // Call the zome
    let serialized_response = state
        .app_call
        .handle_zome_call(
            app_info.installed_app_id,
            cell_id,
            DHT_UTIL_ZOME.to_string(),
            fn_name.to_string(),
            zome_call_payload,
        )
        .await?;

    // Transcode response to JSON
    hsb_to_json(&serialized_response)
}
