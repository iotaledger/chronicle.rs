use std::{
    borrow::Cow,
    convert::TryFrom,
};

use bee_rest_api::types::dtos::{
    OutputDto,
    PayloadDto,
};

use permanode_storage::access::{
    AddressRecord,
    IndexationRecord,
    LedgerInclusionState,
    Message,
    OutputId,
    ParentRecord,
    Partitioned,
};
use scylla_cql::TryInto;
use serde::{
    Deserialize,
    Serialize,
};

/// Response of GET /info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InfoResponse {
    pub name: String,
    pub version: String,
    #[serde(rename = "isHealthy")]
    pub is_healthy: bool,
    #[serde(rename = "networkId")]
    pub network_id: String,
    #[serde(rename = "bech32HRP")]
    pub bech32_hrp: String,
    #[serde(rename = "latestMilestoneIndex")]
    pub latest_milestone_index: u32,
    #[serde(rename = "confirmedMilestoneIndex")]
    pub confirmed_milestone_index: u32,
    #[serde(rename = "pruningIndex")]
    pub pruning_index: u32,
    pub features: Vec<String>,
    #[serde(rename = "minPowScore")]
    pub min_pow_score: f64,
}

/// Response of GET /api/<keyspace>/messages/<message_id>
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageResponse {
    #[serde(rename = "networkId")]
    pub network_id: String,
    #[serde(rename = "parentMessageIds")]
    pub parents: Vec<String>,
    pub payload: Option<PayloadDto>,
    pub nonce: String,
}

impl TryFrom<Message> for MessageResponse {
    type Error = Cow<'static, str>;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        Ok(MessageResponse {
            network_id: message.network_id().to_string(),
            parents: message.parents().iter().map(|p| p.to_string()).collect(),
            payload: message.payload().as_ref().map(TryInto::try_into).transpose()?,
            nonce: message.nonce().to_string(),
        })
    }
}

/// Response of GET /api/<keyspace>/messages/<message_id>/metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageMetadataResponse {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "parentMessageIds")]
    pub parent_message_ids: Vec<String>,
    #[serde(rename = "isSolid")]
    pub is_solid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "referencedByMilestoneIndex")]
    pub referenced_by_milestone_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ledgerInclusionState")]
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "shouldPromote")]
    pub should_promote: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "shouldReattach")]
    pub should_reattach: Option<bool>,
}

/// Response of GET /api/<keyspace>/messages/<message_id>/children
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageChildrenResponse {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "maxResults")]
    pub max_results: usize,
    pub count: usize,
    #[serde(rename = "childrenMessageIds")]
    pub children_message_ids: Vec<Record>,
}

/// Response of GET /api/<keyspace>/messages?<index>
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagesForIndexResponse {
    pub index: String,
    #[serde(rename = "maxResults")]
    pub max_results: usize,
    pub count: usize,
    #[serde(rename = "messageIds")]
    pub message_ids: Vec<Record>,
}

/// Response of GET /api/<keyspace>/addresses/<address>/outputs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputsForAddressResponse {
    // The type of the address (1=Ed25519).
    #[serde(rename = "addressType")]
    pub address_type: u8,
    pub address: String,
    #[serde(rename = "maxResults")]
    pub max_results: usize,
    pub count: usize,
    #[serde(rename = "outputIds")]
    pub output_ids: Vec<Record>,
}

/// Response of GET /api/<keyspace>/outputs/<output_id>
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputResponse {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "transactionId")]
    pub transaction_id: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u16,
    #[serde(rename = "isSpent")]
    pub is_spent: bool,
    pub output: OutputDto,
}

/// Response of GET /api/<keyspace>/milestone/<index>
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MilestoneResponse {
    #[serde(rename = "index")]
    pub milestone_index: u32,
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub milestone_index: u32,
}

impl From<Partitioned<AddressRecord>> for Record {
    fn from(record: Partitioned<AddressRecord>) -> Self {
        Record {
            id: OutputId::new(record.transaction_id, record.index).unwrap().to_string(),
            inclusion_state: record.ledger_inclusion_state,
            milestone_index: record.milestone_index(),
        }
    }
}

impl From<Partitioned<IndexationRecord>> for Record {
    fn from(record: Partitioned<IndexationRecord>) -> Self {
        Record {
            id: record.message_id.to_string(),
            inclusion_state: record.ledger_inclusion_state,
            milestone_index: record.milestone_index(),
        }
    }
}

impl From<Partitioned<ParentRecord>> for Record {
    fn from(record: Partitioned<ParentRecord>) -> Self {
        Record {
            id: record.message_id.to_string(),
            inclusion_state: record.ledger_inclusion_state,
            milestone_index: record.milestone_index(),
        }
    }
}
