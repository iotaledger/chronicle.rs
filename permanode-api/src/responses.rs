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
    MessageMetadata,
    OutputId,
    ParentRecord,
    Partitioned,
};
use scylla_cql::TryInto;
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ListenerResponse {
    /// Response of GET /info
    Info {
        name: String,
        version: String,
        #[serde(rename = "isHealthy")]
        is_healthy: bool,
        #[serde(rename = "networkId")]
        network_id: String,
        #[serde(rename = "bech32HRP")]
        bech32_hrp: String,
        #[serde(rename = "latestMilestoneIndex")]
        latest_milestone_index: u32,
        #[serde(rename = "confirmedMilestoneIndex")]
        confirmed_milestone_index: u32,
        #[serde(rename = "pruningIndex")]
        pruning_index: u32,
        features: Vec<String>,
        #[serde(rename = "minPowScore")]
        min_pow_score: f64,
    },
    /// Response of GET /api/<keyspace>/messages/<message_id>
    /// and GET /api/<keyspace>/transactions/<transaction_id>/included-message
    Message {
        #[serde(rename = "networkId")]
        network_id: String,
        #[serde(rename = "parentMessageIds")]
        parents: Vec<String>,
        payload: Option<PayloadDto>,
        nonce: String,
    },
    /// Response of GET /api/<keyspace>/messages/<message_id>/metadata
    MessageMetadata {
        #[serde(rename = "messageId")]
        message_id: String,
        #[serde(rename = "parentMessageIds")]
        parent_message_ids: Vec<String>,
        #[serde(rename = "isSolid")]
        is_solid: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "referencedByMilestoneIndex")]
        referenced_by_milestone_index: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "ledgerInclusionState")]
        ledger_inclusion_state: Option<LedgerInclusionState>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "shouldPromote")]
        should_promote: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "shouldReattach")]
        should_reattach: Option<bool>,
    },
    /// Response of GET /api/<keyspace>/messages/<message_id>/children
    MessageChildren {
        #[serde(rename = "messageId")]
        message_id: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "childrenMessageIds")]
        children_message_ids: Vec<Record>,
        state: StateData,
    },
    /// Response of GET /api/<keyspace>/messages?<index>
    MessagesForIndex {
        index: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "messageIds")]
        message_ids: Vec<Record>,
        state: StateData,
    },
    /// Response of GET /api/<keyspace>/addresses/<address>/outputs
    OutputsForAddress {
        // The type of the address (1=Ed25519).
        #[serde(rename = "addressType")]
        address_type: u8,
        address: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "outputIds")]
        output_ids: Vec<OutputId>,
        state: StateData,
    },
    /// Response of GET /api/<keyspace>/addresses/<address>/outputs
    OutputsForAddressExpanded {
        // The type of the address (1=Ed25519).
        #[serde(rename = "addressType")]
        address_type: u8,
        address: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "outputIds")]
        output_ids: Vec<Record>,
        state: StateData,
    },
    /// Response of GET /api/<keyspace>/outputs/<output_id>
    Output {
        #[serde(rename = "messageId")]
        message_id: String,
        #[serde(rename = "transactionId")]
        transaction_id: String,
        #[serde(rename = "outputIndex")]
        output_index: u16,
        #[serde(rename = "isSpent")]
        is_spent: bool,
        output: OutputDto,
    },
    /// Response of GET /api/<keyspace>/milestone/<index>
    Milestone {
        #[serde(rename = "index")]
        milestone_index: u32,
        #[serde(rename = "messageId")]
        message_id: String,
        timestamp: u64,
    },
}

impl TryFrom<Message> for ListenerResponse {
    type Error = Cow<'static, str>;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        Ok(ListenerResponse::Message {
            network_id: message.network_id().to_string(),
            parents: message.parents().iter().map(|p| p.to_string()).collect(),
            payload: message.payload().as_ref().map(TryInto::try_into).transpose()?,
            nonce: message.nonce().to_string(),
        })
    }
}

impl From<MessageMetadata> for ListenerResponse {
    fn from(metadata: MessageMetadata) -> Self {
        ListenerResponse::MessageMetadata {
            message_id: metadata.message_id.to_string(),
            parent_message_ids: metadata
                .parent_message_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            is_solid: metadata.is_solid,
            referenced_by_milestone_index: metadata.referenced_by_milestone_index,
            ledger_inclusion_state: metadata.ledger_inclusion_state,
            should_promote: metadata.should_promote,
            should_reattach: metadata.should_reattach,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Record {
    pub id: String,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub milestone_index: u32,
}

impl TryFrom<Partitioned<AddressRecord>> for Record {
    type Error = anyhow::Error;

    fn try_from(record: Partitioned<AddressRecord>) -> Result<Self, Self::Error> {
        Ok(Record {
            id: OutputId::new(record.transaction_id, record.index)?.to_string(),
            inclusion_state: record.ledger_inclusion_state,
            milestone_index: record.milestone_index(),
        })
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StateData {
    #[serde(rename = "pagingState")]
    pub paging_state: Option<String>,
    #[serde(rename = "lastPartitionId")]
    pub last_partition_id: Option<u16>,
    #[serde(rename = "lastMilestoneIndex")]
    pub last_milestone_index: Option<u32>,
}

impl From<(Option<Vec<u8>>, Option<u16>, Option<u32>)> for StateData {
    fn from(
        (paging_state, last_partition_id, last_milestone_index): (Option<Vec<u8>>, Option<u16>, Option<u32>),
    ) -> Self {
        Self {
            paging_state: paging_state.map(|v| hex::encode(v)),
            last_partition_id,
            last_milestone_index,
        }
    }
}
