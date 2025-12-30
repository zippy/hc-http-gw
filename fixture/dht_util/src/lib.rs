//! DHT Utility Zome
//!
//! Provides passthrough functions for DHT operations, enabling browser extensions
//! to access DHT data via the HTTP gateway. This zome should be included in any
//! hApp that wants to support browser extension access.
//!
//! ## Functions
//!
//! - `dht_get_record` - Get a record by hash
//! - `dht_get_details` - Get full details (updates, deletes) for a hash
//! - `dht_get_links` - Get links from a base address
//! - `dht_count_links` - Count links from a base address

use hdk::prelude::*;
use serde::{Deserialize, Serialize};

/// Input for getting a record
#[derive(Debug, Serialize, Deserialize)]
pub struct GetRecordInput {
    /// The hash to look up (can be action hash or entry hash)
    pub hash: AnyDhtHash,
    /// Optional get options
    #[serde(default)]
    pub options: GetOptionsInput,
}

/// Get options input (simplified from GetOptions)
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GetOptionsInput {
    /// Strategy for getting the record
    #[serde(default)]
    pub strategy: GetStrategyInput,
}

/// Get strategy input
#[derive(Debug, Default, Serialize, Deserialize)]
pub enum GetStrategyInput {
    /// Get from local storage only
    Local,
    /// Get from network (default)
    #[default]
    Network,
}

impl From<GetOptionsInput> for GetOptions {
    fn from(input: GetOptionsInput) -> Self {
        match input.strategy {
            GetStrategyInput::Local => GetOptions::local(),
            GetStrategyInput::Network => GetOptions::network(),
        }
    }
}

/// Get a record by hash
///
/// This is a passthrough to the `get` host function.
#[hdk_extern]
pub fn dht_get_record(input: GetRecordInput) -> ExternResult<Option<Record>> {
    get(input.hash, input.options.into())
}

/// Get details for a hash
///
/// Returns full details including updates and deletes for the given hash.
/// This is a passthrough to the `get_details` host function.
#[hdk_extern]
pub fn dht_get_details(input: GetRecordInput) -> ExternResult<Option<Details>> {
    get_details(input.hash, input.options.into())
}

/// Input for getting links
#[derive(Debug, Serialize, Deserialize)]
pub struct GetLinksInput {
    /// Base address to get links from
    pub base: AnyLinkableHash,
    /// Optional link type filter (as integer - zome index * 256 + link type index)
    #[serde(default)]
    pub link_type: Option<u16>,
    /// Optional tag prefix filter
    #[serde(default)]
    pub tag_prefix: Option<Vec<u8>>,
}

/// Get links from a base address
///
/// This is a passthrough to the `get_links` host function.
#[hdk_extern]
pub fn dht_get_links(input: GetLinksInput) -> ExternResult<Vec<Link>> {
    // Build the link query
    let mut query_builder = LinkQuery::new(
        input.base,
        // For cross-zome queries, we use a special link type scope
        LinkTypeFilter::Dependencies(vec![]),
    );

    // Add link type filter if provided
    if let Some(link_type) = input.link_type {
        let zome_index = (link_type >> 8) as u8;
        let link_type_index = (link_type & 0xFF) as u8;
        query_builder = LinkQuery::new(
            query_builder.base.clone(),
            LinkTypeFilter::single_dep(ZomeIndex(zome_index)),
        );
        // Note: Proper filtering by link type would require more complex setup
        // For now we filter all links from the specified zome
        let _ = link_type_index; // Suppress unused warning
    }

    // Add tag prefix if provided
    if let Some(tag_prefix) = input.tag_prefix {
        query_builder = query_builder.tag_prefix(LinkTag::new(tag_prefix));
    }

    get_links(query_builder, GetStrategy::Network)
}

/// Input for counting links
#[derive(Debug, Serialize, Deserialize)]
pub struct CountLinksInput {
    /// Base address to count links from
    pub base: AnyLinkableHash,
    /// Optional link type filter
    #[serde(default)]
    pub link_type: Option<u16>,
    /// Optional tag prefix filter
    #[serde(default)]
    pub tag_prefix: Option<Vec<u8>>,
}

/// Count links from a base address
///
/// This is a passthrough to the `count_links` host function.
#[hdk_extern]
pub fn dht_count_links(input: CountLinksInput) -> ExternResult<usize> {
    // Build the link query
    let mut query_builder = LinkQuery::new(
        input.base,
        LinkTypeFilter::Dependencies(vec![]),
    );

    // Add link type filter if provided
    if let Some(link_type) = input.link_type {
        let zome_index = (link_type >> 8) as u8;
        query_builder = LinkQuery::new(
            query_builder.base.clone(),
            LinkTypeFilter::single_dep(ZomeIndex(zome_index)),
        );
    }

    // Add tag prefix if provided
    if let Some(tag_prefix) = input.tag_prefix {
        query_builder = query_builder.tag_prefix(LinkTag::new(tag_prefix));
    }

    count_links(query_builder)
}
