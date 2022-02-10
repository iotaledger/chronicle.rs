// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    input::Input,
    output::{
        Output,
        OutputId,
    },
    Message,
};
use bee_rest_api::types::{
    dtos::{
        InputDto,
        OutputDto,
        PayloadDto,
        UnlockBlockDto,
    },
    responses::MessageMetadataResponse,
};
use chronicle_storage::access::{
    InputData,
    LedgerInclusionState,
    LegacyOutputRecord,
    MsAnalyticsRecord,
    ParentRecord,
    TagRecord,
    TransactionRes,
    UnlockRes,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    borrow::{
        Borrow,
        Cow,
    },
    collections::VecDeque,
    convert::TryFrom,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ListenerResponseV1 {
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
    MessageMetadata(MessageMetadataResponse),
    /// Response of GET /api/<keyspace>/messages/<message_id>/children
    MessageChildren {
        #[serde(rename = "messageId")]
        message_id: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "childrenMessageIds")]
        children_message_ids: Vec<String>,
        paging_state: Option<String>,
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
        paging_state: Option<String>,
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
    /// Response of GET /api/<keyspace>/transactions/<message_id>
    Transaction(Transaction),
    /// Response of GET /api/<keyspace>/transactions/ed25519/<address>
    Transactions {
        transactions: Vec<Transaction>,
        state: Option<String>,
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

impl TryFrom<Message> for ListenerResponseV1 {
    type Error = Cow<'static, str>;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        Ok(ListenerResponseV1::Message {
            network_id: message.network_id().to_string(),
            parents: message.parents().iter().map(|p| p.to_string()).collect(),
            payload: message.payload().map(Into::into),
            nonce: message.nonce().to_string(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Record {
    pub id: String,
    #[serde(rename = "inclusionState")]
    pub inclusion_state: Option<LedgerInclusionState>,
    #[serde(rename = "milestoneIndex")]
    pub milestone_index: u32,
}

impl TryFrom<LegacyOutputRecord> for Record {
    type Error = anyhow::Error;

    fn try_from(record: LegacyOutputRecord) -> Result<Self, Self::Error> {
        Ok(Record {
            id: record.output_id.to_string(),
            inclusion_state: record.inclusion_state,
            milestone_index: record.milestone_index,
        })
    }
}

impl From<TagRecord> for Record {
    fn from(record: TagRecord) -> Self {
        Record {
            id: record.message_id.to_string(),
            inclusion_state: record.inclusion_state,
            milestone_index: record.milestone_index,
        }
    }
}

impl From<ParentRecord> for Record {
    fn from(record: ParentRecord) -> Self {
        Record {
            id: record.message_id.to_string(),
            inclusion_state: record.inclusion_state,
            milestone_index: record.milestone_index,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Transaction {
    /// The created output's message id
    #[serde(rename = "messageId")]
    pub message_id: String,
    /// The confirmation timestamp
    #[serde(rename = "milestoneIndex")]
    pub milestone_index: Option<u32>,
    /// The output
    pub outputs: Vec<MaybeSpentOutput>,
    /// The inputs, if they exist
    pub inputs: Vec<InputDto>,
}

impl From<TransactionRes> for Transaction {
    fn from(o: TransactionRes) -> Self {
        Self {
            message_id: o.message_id.to_string(),
            milestone_index: o.milestone_index.map(|ms| ms.0),
            outputs: o.outputs.into_iter().map(Into::into).collect(),
            inputs: o
                .inputs
                .into_iter()
                .map(|d| {
                    match d {
                        InputData::Utxo(i, _) => Input::Utxo(i),
                        InputData::Treasury(t) => Input::Treasury(t),
                    }
                    .borrow()
                    .into()
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MaybeSpentOutput {
    pub output: OutputDto,
    #[serde(rename = "spendingMessageId")]
    pub spending_message_id: Option<String>,
}

impl From<(Output, Option<UnlockRes>)> for MaybeSpentOutput {
    fn from((o, u): (Output, Option<UnlockRes>)) -> Self {
        Self {
            output: o.borrow().into(),
            spending_message_id: u.map(|u| u.message_id.to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Unlock {
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub block: UnlockBlockDto,
}

impl From<UnlockRes> for Unlock {
    fn from(u: UnlockRes) -> Self {
        Unlock {
            message_id: u.message_id.to_string(),
            block: u.block.borrow().into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StateData {
    #[serde(rename = "pagingState")]
    pub paging_state: Option<Vec<u8>>,
    #[serde(rename = "partitionIds")]
    pub partition_ids: VecDeque<u32>,
}

impl From<(Option<Vec<u8>>, Vec<u32>)> for StateData {
    fn from((paging_state, partition_ids): (Option<Vec<u8>>, Vec<u32>)) -> Self {
        Self {
            paging_state,
            partition_ids,
        }
    }
}
