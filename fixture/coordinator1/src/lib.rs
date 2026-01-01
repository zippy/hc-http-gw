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
