// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    prelude::{
        MilestoneIndex,
        OutputId,
    },
    Message,
};
use bee_rest_api::types::dtos::{
    OutputDto,
    PayloadDto,
};
use chronicle_broker::AnalyticData;
use chronicle_storage::access::{
    AddressRecord,
    IndexationRecord,
    LedgerInclusionState,
    MessageMetadata,
    ParentRecord,
    Partitioned,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    borrow::Cow,
    convert::TryFrom,
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
        children_message_ids: Vec<String>,
        state: Option<String>,
    },
    /// Response of GET /api/<keyspace>/messages/<message_id>/children[?expanded=true]
    MessageChildrenExpanded {
        #[serde(rename = "messageId")]
        message_id: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "childrenMessageIds")]
        children_message_ids: Vec<Record>,
        state: Option<String>,
    },
    /// Response of GET /api/<keyspace>/messages?<index>
    MessagesForIndex {
        index: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "messageIds")]
        message_ids: Vec<String>,
        state: Option<String>,
    },
    /// Response of GET /api/<keyspace>/messages?<index>[&expanded=true]
    MessagesForIndexExpanded {
        index: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "messageIds")]
        message_ids: Vec<Record>,
        state: Option<String>,
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
        state: Option<String>,
    },
    /// Response of GET /api/<keyspace>/addresses/<address>/outputs[?expanded=true]
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
        state: Option<String>,
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
    /// Response of GET /api/<keyspace>/analytics[?start=<u32>&end=<u32>]
    Analytics { ranges: Vec<AnalyticData> },
}

impl TryFrom<Message> for ListenerResponse {
    type Error = Cow<'static, str>;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        Ok(ListenerResponse::Message {
            network_id: message.network_id().to_string(),
            parents: message.parents().iter().map(|p| p.to_string()).collect(),
            payload: message.payload().as_ref().map(Into::into),
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
    pub paging_state: Option<Vec<u8>>,
    pub last_partition_id: Option<u16>,
    pub last_milestone_index: Option<u32>,
    pub partition_ids: Vec<(MilestoneIndex, u16)>,
}

impl From<(Option<Vec<u8>>, Option<u16>, Option<u32>, Vec<(MilestoneIndex, u16)>)> for StateData {
    fn from(
        (paging_state, last_partition_id, last_milestone_index, partition_ids): (
            Option<Vec<u8>>,
            Option<u16>,
            Option<u32>,
            Vec<(MilestoneIndex, u16)>,
        ),
    ) -> Self {
        Self {
            paging_state,
            last_partition_id,
            last_milestone_index,
            partition_ids,
        }
    }
}
