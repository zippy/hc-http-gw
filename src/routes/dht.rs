//! DHT endpoints.
//!
//! Provides direct DHT access for browser extension agents via the dht_util zome.

use crate::service::AppState;
use crate::transcode::hsb_to_json;
use crate::{HcHttpGatewayError, HcHttpGatewayResult};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use holochain_client::CellInfo;
use holochain_types::dna::DnaHash;
use holochain_types::prelude::{
    ActionHash, AgentPubKey, AnyDhtHash, AnyLinkableHash, EntryHash, ExternalHash, ExternIO,
};
use serde::{Deserialize, Serialize};

/// DHT utility zome name.
const DHT_UTIL_ZOME: &str = "dht_util";

// ============================================================================
// Hash parsing helpers
// ============================================================================

/// Parse a hash string into AnyDhtHash.
///
/// The hash string format is "u{base64}" where base64 decodes to 39 bytes.
/// AnyDhtHash can be either EntryHash or ActionHash:
/// - uhCEk = Entry (0x84, 0x21, 0x24)
/// - uhCkk = Action (0x84, 0x29, 0x24)
fn parse_any_dht_hash(s: &str) -> Result<AnyDhtHash, HcHttpGatewayError> {
    if let Ok(hash) = EntryHash::try_from(s) {
        return Ok(AnyDhtHash::from(hash));
    }
    if let Ok(hash) = ActionHash::try_from(s) {
        return Ok(AnyDhtHash::from(hash));
    }
    Err(HcHttpGatewayError::RequestMalformed(format!(
        "Invalid DHT hash format: {}",
        s
    )))
}

/// Parse a hash string into AnyLinkableHash.
///
/// The hash string format is "u{base64}" where base64 decodes to 39 bytes.
/// The first 3 bytes are the prefix that identifies the hash type:
/// - uhCAk = Agent (0x84, 0x20, 0x24)
/// - uhCEk = Entry (0x84, 0x21, 0x24)
/// - uhCkk = Action (0x84, 0x29, 0x24)
/// - uhC8k = External (0x84, 0x2f, 0x24)
fn parse_any_linkable_hash(s: &str) -> Result<AnyLinkableHash, HcHttpGatewayError> {
    // Try parsing as each possible type and convert to AnyLinkableHash
    if let Ok(hash) = AgentPubKey::try_from(s) {
        return Ok(AnyLinkableHash::from(hash));
    }
    if let Ok(hash) = EntryHash::try_from(s) {
        return Ok(AnyLinkableHash::from(hash));
    }
    if let Ok(hash) = ActionHash::try_from(s) {
        return Ok(AnyLinkableHash::from(hash));
    }
    if let Ok(hash) = ExternalHash::try_from(s) {
        return Ok(AnyLinkableHash::from(hash));
    }
    Err(HcHttpGatewayError::RequestMalformed(format!(
        "Invalid hash format: {}",
        s
    )))
}

// ============================================================================
// Zome input types - must match dht_util zome's expected input structures
// ============================================================================

/// Get strategy for DHT operations
#[derive(Debug, Default, Serialize, Deserialize)]
pub enum GetStrategyInput {
    /// Get from local storage only
    Local,
    /// Get from network (default)
    #[default]
    Network,
}

/// Get options for record/details operations
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GetOptionsInput {
    #[serde(default)]
    pub strategy: GetStrategyInput,
}

/// Input for dht_get_record and dht_get_details zome functions
#[derive(Debug, Serialize, Deserialize)]
pub struct GetRecordInput {
    pub hash: AnyDhtHash,
    #[serde(default)]
    pub options: GetOptionsInput,
}

/// Input for dht_get_links zome function
#[derive(Debug, Serialize, Deserialize)]
pub struct GetLinksZomeInput {
    pub base: AnyLinkableHash,
    #[serde(default)]
    pub link_type: Option<u16>,
    #[serde(default)]
    pub tag_prefix: Option<Vec<u8>>,
}

/// Input for dht_count_links zome function
#[derive(Debug, Serialize, Deserialize)]
pub struct CountLinksZomeInput {
    pub base: AnyLinkableHash,
    #[serde(default)]
    pub link_type: Option<u16>,
    #[serde(default)]
    pub tag_prefix: Option<Vec<u8>>,
}

// ============================================================================
// HTTP request parameter types
// ============================================================================

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

    // Parse the record hash
    let hash = parse_any_dht_hash(&path.hash)?;

    // Build and encode payload for dht_get_record
    let input = GetRecordInput {
        hash,
        options: GetOptionsInput {
            strategy: GetStrategyInput::Network,
        },
    };
    let payload = ExternIO::encode(input)
        .map_err(|e| HcHttpGatewayError::RequestMalformed(format!("Failed to encode payload: {}", e)))?;

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

    // Parse the record hash
    let hash = parse_any_dht_hash(&path.hash)?;

    // Build and encode payload for dht_get_details
    let input = GetRecordInput {
        hash,
        options: GetOptionsInput {
            strategy: GetStrategyInput::Network,
        },
    };
    let payload = ExternIO::encode(input)
        .map_err(|e| HcHttpGatewayError::RequestMalformed(format!("Failed to encode payload: {}", e)))?;

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

    // Parse the base hash
    let base = parse_any_linkable_hash(&query.base)?;

    // Parse optional tag prefix
    let tag_prefix = if let Some(tag) = query.tag {
        let tag_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &tag)
            .map_err(|_| {
                HcHttpGatewayError::RequestMalformed("Invalid tag encoding".to_string())
            })?;
        Some(tag_bytes)
    } else {
        None
    };

    // Build and encode payload for dht_get_links
    let input = GetLinksZomeInput {
        base,
        link_type: query.link_type,
        tag_prefix,
    };
    let payload = ExternIO::encode(input)
        .map_err(|e| HcHttpGatewayError::RequestMalformed(format!("Failed to encode payload: {}", e)))?;

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

    // Parse the base hash
    let base = parse_any_linkable_hash(&query.base)?;

    // Parse optional tag prefix
    let tag_prefix = if let Some(tag) = query.tag {
        let tag_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &tag)
            .map_err(|_| {
                HcHttpGatewayError::RequestMalformed("Invalid tag encoding".to_string())
            })?;
        Some(tag_bytes)
    } else {
        None
    };

    // Build and encode payload for dht_count_links
    let input = CountLinksZomeInput {
        base,
        link_type: query.link_type,
        tag_prefix,
    };
    let payload = ExternIO::encode(input)
        .map_err(|e| HcHttpGatewayError::RequestMalformed(format!("Failed to encode payload: {}", e)))?;

    call_dht_util_zome(&state, dna_hash, "dht_count_links", payload).await
}

/// Find an app that contains the specified DNA hash.
/// Unlike try_get_valid_app, this searches through all allowed apps
/// instead of matching by coordinator_identifier.
async fn find_app_with_dna_and_zome(
    dna_hash: &DnaHash,
    _zome_name: &str,
    app_info_cache: crate::app_selection::AppInfoCache,
    allowed_app_ids: &crate::config::AllowedAppIds,
    admin_call: std::sync::Arc<dyn crate::AdminCall>,
) -> HcHttpGatewayResult<holochain_client::AppInfo> {
    use holochain_conductor_api::AppStatusFilter;

    // First check the cache
    {
        let installed_apps = app_info_cache.read().await;
        for app in installed_apps.iter() {
            if !allowed_app_ids.contains(&app.installed_app_id) {
                continue;
            }
            // Check if this app has a cell with the matching DNA hash
            let has_dna = app.cell_info.values().any(|cells| {
                cells.iter().any(|cell| match cell {
                    CellInfo::Provisioned(p) => p.cell_id.dna_hash() == dna_hash,
                    _ => false,
                })
            });
            if has_dna {
                return Ok(app.clone());
            }
        }
    }

    // Cache miss - refresh from admin websocket
    let new_apps = admin_call
        .list_apps(Some(AppStatusFilter::Enabled))
        .await
        .map_err(|e| {
            HcHttpGatewayError::RequestMalformed(format!("Failed to list apps: {}", e))
        })?;

    // Update cache
    {
        let mut cache = app_info_cache.write().await;
        *cache = new_apps.clone();
    }

    // Search again
    for app in new_apps {
        if !allowed_app_ids.contains(&app.installed_app_id) {
            continue;
        }
        let has_dna = app.cell_info.values().any(|cells| {
            cells.iter().any(|cell| match cell {
                CellInfo::Provisioned(p) => p.cell_id.dna_hash() == dna_hash,
                _ => false,
            })
        });
        if has_dna {
            return Ok(app);
        }
    }

    Err(HcHttpGatewayError::RequestMalformed(
        "No allowed app found containing the specified DNA".to_string(),
    ))
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
    payload: ExternIO,
) -> HcHttpGatewayResult<String> {
    // Find an app with this DNA - search through all allowed apps
    let app_info = find_app_with_dna_and_zome(
        &dna_hash,
        DHT_UTIL_ZOME,
        state.app_info_cache.clone(),
        &state.configuration.allowed_app_ids,
        state.admin_call.clone(),
    )
    .await?;

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
            payload,
        )
        .await?;

    // Transcode response to JSON
    hsb_to_json(&serialized_response)
}
