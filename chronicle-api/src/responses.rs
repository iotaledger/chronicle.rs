// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    address::Address,
    input::Input,
    milestone::MilestoneIndex,
    output::{
        Output,
        OutputId,
    },
    payload::transaction::TransactionId,
    MessageId,
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
use chronicle_storage::{
    access::{
        InputData,
        LedgerInclusionState,
        LegacyOutputRecord,
        MsAnalyticsRecord,
        OutputType,
        TagRecord,
        TransactionRes,
        UnlockRes,
    },
    MessageRecord,
    ProtocolId,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    borrow::Borrow,
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
        network_id: Option<String>,
        #[serde(rename = "protocolId")]
        protocol_id: ProtocolId,
        #[serde(rename = "parentMessageIds")]
        parents: Vec<MessageId>,
        payload: Option<PayloadDto>,
        nonce: String,
    },
    /// Response of GET /api/<keyspace>/messages/<message_id>/metadata
    MessageMetadata(MessageMetadataResponse),
    /// Response of GET /api/<keyspace>/messages/<message_id>/children
    MessageChildren {
        #[serde(rename = "messageId")]
        message_id: MessageId,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "childrenMessageIds")]
        children_message_ids: Vec<MessageId>,
    },
    /// Response of GET /api/<keyspace>/messages/<message_id>/children[?expanded=true]
    MessageChildrenExpanded {
        #[serde(rename = "messageId")]
        message_id: MessageId,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "childrenMessageIds")]
        children_message_ids: Vec<Record>,
    },
    /// Response of GET /api/<keyspace>/messages?<index>
    MessagesForIndex {
        index: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "messageIds")]
        message_ids: Vec<MessageId>,
    },
    /// Response of GET /api/<keyspace>/messages?<index>[&expanded=true]
    MessagesForIndexExpanded {
        index: String,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "messageIds")]
        message_ids: Vec<Record>,
    },
    /// Response of GET /api/<keyspace>/addresses/<address>/outputs
    OutputsForAddress {
        address: Address,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "outputIds")]
        output_ids: Vec<OutputId>,
    },
    /// Response of GET /api/<keyspace>/addresses/<address>/outputs[?expanded=true]
    OutputsForAddressExpanded {
        address: Address,
        #[serde(rename = "maxResults")]
        max_results: usize,
        count: usize,
        #[serde(rename = "outputIds")]
        output_ids: Vec<Record>,
    },
    /// Response of GET /api/<keyspace>/outputs/<output_id>
    Output {
        #[serde(rename = "messageId")]
        message_id: MessageId,
        #[serde(rename = "transactionId")]
        transaction_id: TransactionId,
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
    },
    TransactionHistory {
        transactions: Vec<Transfer>,
    },
    /// Response of GET /api/<keyspace>/milestone/<index>
    Milestone {
        #[serde(rename = "index")]
        milestone_index: MilestoneIndex,
        #[serde(rename = "messageId")]
        message_id: MessageId,
        timestamp: u64,
    },
    /// Response of GET /api/<keyspace>/analytics[?start=<u32>&end=<u32>]
    Analytics {
        ranges: Vec<MsAnalyticsRecord>,
    },
}

impl From<chronicle_storage::Message> for ListenerResponseV1 {
    fn from(message: chronicle_storage::Message) -> Self {
        ListenerResponseV1::Message {
            network_id: None,
            protocol_id: message.protocol_id(),
            parents: message.parents().iter().map(|p| *p).collect(),
            payload: message.payload().map(Into::into),
            nonce: message.nonce().to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Transfer {
    #[serde(rename = "outputId")]
    pub output_id: OutputId,
    #[serde(rename = "outputType")]
    pub output_type: OutputType,
    #[serde(rename = "isUsed")]
    pub is_used: bool,
    #[serde(rename = "inclusionState")]
    pub inclusion_state: Option<LedgerInclusionState>,
    #[serde(rename = "messageId")]
    pub message_id: MessageId,
    pub amount: Option<u64>,
    pub address: Address,
}

impl From<LegacyOutputRecord> for Transfer {
    fn from(rec: LegacyOutputRecord) -> Self {
        Self {
            output_id: rec.output_id,
            output_type: rec.output_type,
            is_used: rec.is_used as u8 != 0,
            inclusion_state: rec.inclusion_state,
            message_id: rec.message_id,
            amount: rec.amount,
            address: rec.address,
        }
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
            milestone_index: record.partition_data.milestone_index.0,
        })
    }
}

impl From<TagRecord> for Record {
    fn from(record: TagRecord) -> Self {
        Record {
            id: record.message_id.to_string(),
            inclusion_state: record.inclusion_state,
            milestone_index: record.partition_data.milestone_index.0,
        }
    }
}

impl From<MessageRecord> for Record {
    fn from(record: MessageRecord) -> Self {
        Record {
            id: record.message_id.to_string(),
            inclusion_state: record.inclusion_state,
            milestone_index: record.milestone_index.unwrap_or_default().0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Transaction {
    /// The created output's message id
    #[serde(rename = "messageId")]
    pub message_id: MessageId,
    /// The confirmation timestamp
    #[serde(rename = "milestoneIndex")]
    pub milestone_index: Option<MilestoneIndex>,
    /// The output
    pub outputs: Vec<MaybeSpentOutput>,
    /// The inputs, if they exist
    pub inputs: Vec<InputDto>,
}

impl From<TransactionRes> for Transaction {
    fn from(o: TransactionRes) -> Self {
        Self {
            message_id: o.message_id,
            milestone_index: o.milestone_index,
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
    pub spending_message_id: Option<MessageId>,
}

impl From<(Output, Option<UnlockRes>)> for MaybeSpentOutput {
    fn from((o, u): (Output, Option<UnlockRes>)) -> Self {
        Self {
            output: o.borrow().into(),
            spending_message_id: u.map(|u| u.message_id),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Unlock {
    #[serde(rename = "messageId")]
    pub message_id: MessageId,
    pub block: UnlockBlockDto,
}

impl From<UnlockRes> for Unlock {
    fn from(u: UnlockRes) -> Self {
        Unlock {
            message_id: u.message_id,
            block: u.block.borrow().into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StateData {
    #[serde(rename = "pagingState")]
    pub paging_state: Option<Vec<u8>>,
}

impl From<Option<Vec<u8>>> for StateData {
    fn from(paging_state: Option<Vec<u8>>) -> Self {
        Self { paging_state }
    }
}
