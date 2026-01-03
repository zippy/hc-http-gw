use hdk::prelude::*;
use integrity::*;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateResponse {
    pub created: ActionHashB64,
}

#[hdk_extern]
pub fn create_1() -> ExternResult<CreateResponse> {
    let time = sys_time()?;
    let created = create_entry(EntryTypes::TestType(TestType {
        value: format!("create_1_{time}"),
    }))?;

    create_link(base(), created.clone(), LinkTypes::Link, ())?;

    Ok(CreateResponse { created: created.into() })
}

#[hdk_extern]
pub fn get_all_1() -> ExternResult<Vec<TestType>> {
    let links = get_links(LinkQuery::try_new(base(), LinkTypes::Link)?, GetStrategy::default())?;

    let mut out = Vec::new();
    for link in links {
        let Some(target) = link.target.into_any_dht_hash() else {
            continue;
        };

        let Some(record) = get(target, GetOptions::local())? else {
            continue;
        };

        let Ok(Some(e)) = record.entry.to_app_option::<TestType>() else {
            continue;
        };

        out.push(e);
    }

    Ok(out)
}

#[hdk_extern]
pub fn get_mine(agent_pub_key: AgentPubKey) -> ExternResult<Vec<TestType>> {
    let links = get_links(LinkQuery::try_new(base(), LinkTypes::Link)?, GetStrategy::default())?;

    let mut out = Vec::new();
    for link in links {
        if link.author != agent_pub_key {
            continue;
        }

        let Some(target) = link.target.into_any_dht_hash() else {
            continue;
        };

        let Some(record) = get(target, GetOptions::local())? else {
            continue;
        };

        let Ok(Some(e)) = record.entry.to_app_option::<TestType>() else {
            continue;
        };

        out.push(e);
    }

    Ok(out)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetWithLimitRequest {
    limit: usize,
}

#[hdk_extern]
pub fn get_limited(request: GetWithLimitRequest) -> ExternResult<Vec<TestType>> {
    let links = get_links(LinkQuery::try_new(base(), LinkTypes::Link)?, GetStrategy::default())?;

    let mut out = Vec::new();
    for link in links {
        if out.len() >= request.limit {
            break;
        }

        let Some(target) = link.target.into_any_dht_hash() else {
            continue;
        };

        let Some(record) = get(target, GetOptions::local())? else {
            continue;
        };

        let Ok(Some(e)) = record.entry.to_app_option::<TestType>() else {
            continue;
        };

        out.push(e);
    }

    Ok(out)
}

fn base() -> AnyLinkableHash {
    EntryHash::from_raw_36(vec![1; 36]).into()
}

/// Input for creating a known entry
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKnownEntryInput {
    pub value: String,
}

/// Response with both action and entry hashes
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKnownEntryResponse {
    pub action_hash: ActionHashB64,
    pub entry_hash: EntryHashB64,
}

/// Create an entry with a known, deterministic value
/// The entry hash will be deterministic based on the content
#[hdk_extern]
pub fn create_known_entry(input: CreateKnownEntryInput) -> ExternResult<CreateKnownEntryResponse> {
    let entry = TestType { value: input.value.clone() };
    let entry_hash = hash_entry(&entry)?;
    let action_hash = create_entry(EntryTypes::TestType(entry))?;

    Ok(CreateKnownEntryResponse {
        action_hash: action_hash.into(),
        entry_hash: entry_hash.into(),
    })
}

// ============================================================================
// Remote Signal Testing Functions
// ============================================================================

/// Input for ping function
#[derive(Debug, Serialize, Deserialize)]
pub struct PingInput {
    /// Optional message to include in the pong response
    pub message: Option<String>,
    /// Target agent to send the pong signal to
    pub to_agent: AgentPubKey,
}

/// Signal payload for pong response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongSignal {
    pub pong: String,
    pub from_agent: AgentPubKeyB64,
    pub timestamp: Timestamp,
}

/// Ping function that sends a pong signal to a specified agent.
///
/// This is useful for testing remote signal delivery from conductor to browser.
/// Call this with the browser agent's pubkey to have the conductor send a signal to it.
#[hdk_extern]
pub fn ping(input: PingInput) -> ExternResult<String> {
    // Get our own agent key
    let my_agent = agent_info()?.agent_initial_pubkey;

    // Build pong message
    let message = input.message.unwrap_or_else(|| "ping".to_string());
    let pong_signal = PongSignal {
        pong: format!("pong: {}", message),
        from_agent: my_agent.into(),
        timestamp: sys_time()?,
    };

    // Send remote signal to target
    let encoded = ExternIO::encode(pong_signal)
        .map_err(|e| wasm_error!(WasmErrorInner::Guest(format!("Failed to encode signal: {}", e))))?;
    send_remote_signal(encoded, vec![input.to_agent.clone()])?;

    Ok(format!("Sent pong signal to {}", AgentPubKeyB64::from(input.to_agent)))
}

/// Send a test signal to a specific agent.
///
/// This allows sending arbitrary signals to test the remote signal pipeline.
#[derive(Debug, Serialize, Deserialize)]
pub struct SendSignalInput {
    pub to_agent: AgentPubKey,
    pub message: String,
}

#[hdk_extern]
pub fn send_test_signal(input: SendSignalInput) -> ExternResult<String> {
    let my_agent = agent_info()?.agent_initial_pubkey;

    let signal = PongSignal {
        pong: input.message,
        from_agent: my_agent.into(),
        timestamp: sys_time()?,
    };

    let encoded = ExternIO::encode(signal)
        .map_err(|e| wasm_error!(WasmErrorInner::Guest(format!("Failed to encode signal: {}", e))))?;
    send_remote_signal(encoded, vec![input.to_agent.clone()])?;

    Ok(format!("Sent signal to {}", AgentPubKeyB64::from(input.to_agent)))
}
