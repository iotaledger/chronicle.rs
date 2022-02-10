// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod address_hints;
mod alias_outputs;
mod basic_outputs;
mod daily_analytics;
mod foundry_outputs;
mod legacy_outputs;
mod messages;
mod milestones;
mod ms_analytics;
mod nft_outputs;
mod parents;
mod sync;
mod tag_hints;
mod tags;
mod transactions;

pub use address_hints::*;
pub use alias_outputs::*;
pub use basic_outputs::*;
use bee_rest_api::types::dtos::LedgerInclusionStateDto;
pub use daily_analytics::*;
pub use foundry_outputs::*;
pub use legacy_outputs::*;
pub use messages::*;
pub use milestones::*;
pub use ms_analytics::*;
pub use nft_outputs::*;
pub use parents::*;
pub use sync::*;
pub use tag_hints::*;
pub use tags::*;
pub use transactions::*;

use anyhow::{
    anyhow,
    bail,
    ensure,
};
use bee_message::{
    address::*,
    milestone::{
        Milestone,
        MilestoneIndex,
    },
    output::{
        feature_block::*,
        unlock_condition::*,
        *,
    },
    payload::{
        transaction::{
            TransactionEssence,
            TransactionId,
        },
        MilestonePayload,
        Payload,
    },
    Message,
    MessageId,
};
use chronicle_common::Wrapper;
use chrono::NaiveDateTime;
use packable::{
    Packable,
    PackableExt,
};
use scylla_rs::{
    cql::TokenEncodeChain,
    prelude::*,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
        HashMap,
        HashSet,
    },
    convert::TryFrom,
    ops::{
        Deref,
        DerefMut,
    },
    str::FromStr,
};

/// Index type
pub type Index = u16;
/// Amount type
pub type Amount = u64;
/// Output type
pub type OutputType = u8;
/// ParentIndex type
pub type ParentIndex = u16;
/// Identify theoretical nodeid which updated/set the synced_by column in sync table
pub type SyncedBy = u8;
/// Identify theoretical nodeid which updated/set the logged_by column in sync table.
/// This enables the admin to locate the generated logs across cluster of chronicles
pub type LoggedBy = u8;
/// Milestone Range Id
pub type MsRangeId = u32;

/// A marker for a paged result
#[derive(Clone, Debug)]
pub struct Paged<T> {
    inner: T,
    /// The paging state for the query
    pub paging_state: Option<Vec<u8>>,
}

impl<T> Paged<T> {
    /// Creates a new paged marker with an inner type and a paging state
    pub fn new(inner: T, paging_state: Option<Vec<u8>>) -> Self {
        Self { inner, paging_state }
    }
}

impl<T> Deref for Paged<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for Paged<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Wrapper for json data
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct JsonData<T> {
    data: T,
}

impl<T> Deref for JsonData<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> Wrapper for JsonData<T> {
    fn into_inner(self) -> Self::Target {
        self.data
    }
}

pub trait Partitioned {
    const MS_CHUNK_SIZE: u32;

    #[inline]
    fn range_id(ms: u32) -> MsRangeId {
        ms / Self::MS_CHUNK_SIZE
    }
}

#[derive(Clone, Debug)]
/// Wrapper around MessageCount u32
pub struct MessageCount(pub u32);
impl Deref for MessageCount {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Clone, Debug)]
/// Wrapper around TransactionCount u32
pub struct TransactionCount(pub u32);
impl Deref for TransactionCount {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Clone, Debug)]
/// Wrapper around MessageCount u64
pub struct TransferredTokens(pub u64);
impl Deref for TransferredTokens {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A message's ledger inclusion state
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum LedgerInclusionState {
    /// A conflicting message, ex. a double spend
    #[serde(rename = "conflicting")]
    Conflicting = 0,
    /// A successful, included message
    #[serde(rename = "included")]
    Included = 1,
    /// A message without a transaction
    #[serde(rename = "noTransaction")]
    NoTransaction = 2,
}

impl TryFrom<u8> for LedgerInclusionState {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Conflicting),
            1 => Ok(Self::Included),
            2 => Ok(Self::NoTransaction),
            n => bail!("Unexpected ledger inclusion byte state: {}", n),
        }
    }
}

impl From<LedgerInclusionStateDto> for LedgerInclusionState {
    fn from(value: LedgerInclusionStateDto) -> Self {
        match value {
            LedgerInclusionStateDto::Conflicting => Self::Conflicting,
            LedgerInclusionStateDto::Included => Self::Included,
            LedgerInclusionStateDto::NoTransaction => Self::NoTransaction,
        }
    }
}

impl Into<LedgerInclusionStateDto> for LedgerInclusionState {
    fn into(self) -> LedgerInclusionStateDto {
        match self {
            Self::Conflicting => LedgerInclusionStateDto::Conflicting,
            Self::Included => LedgerInclusionStateDto::Included,
            Self::NoTransaction => LedgerInclusionStateDto::NoTransaction,
        }
    }
}

impl ColumnEncoder for LedgerInclusionState {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let num = *self as u8;
        num.encode(buffer)
    }
}

impl ColumnDecoder for LedgerInclusionState {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        u8::try_decode_column(slice)?.try_into()
    }
}

/// A `bee` type wrapper which is used to apply the `ColumnEncoder`
/// functionality over predefined types which are `Packable`.
#[derive(Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub struct Bee<Type>(pub Type);

impl<Type> Bee<Type> {
    /// Consume the wrapper and return the inner `bee` type
    pub fn into_inner(self) -> Type {
        self.0
    }
}

impl<Type> Bee<Type> {
    pub fn as_ref(&self) -> Bee<&Type> {
        Bee(&self.0)
    }

    pub fn as_mut(&mut self) -> Bee<&mut Type> {
        Bee(&mut self.0)
    }
}

impl<Type> Deref for Bee<Type> {
    type Target = Type;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Type> DerefMut for Bee<Type> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<Type> From<Type> for Bee<Type> {
    fn from(t: Type) -> Self {
        Bee(t)
    }
}

impl TokenEncoder for Bee<Milestone> {
    fn encode_token(&self) -> TokenEncodeChain {
        Bee(self.message_id()).chain(&self.timestamp())
    }
}
macro_rules! impl_simple_packable {
    ($t:ty) => {
        impl ColumnDecoder for Bee<$t> {
            fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
                <$t>::unpack_verified(slice)
                    .map_err(|e| anyhow!("{:?}", e))
                    .map(Into::into)
            }
        }

        impl ColumnEncoder for Bee<$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                // TODO: Do these need to encode the length first???
                self.pack(buffer).ok();
            }
        }

        impl TokenEncoder for Bee<$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                // TODO: Do these need to encode the length first???
                self.pack(buffer).ok();
            }
        }

        impl TokenEncoder for Bee<&$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&mut $t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                // TODO: Do these need to encode the length first???
                self.pack(buffer).ok();
            }
        }

        impl TokenEncoder for Bee<&mut $t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }
    };
}

macro_rules! impl_string_packable {
    ($t:ty) => {
        impl ColumnDecoder for Bee<$t> {
            fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
                <$t>::from_str(&String::try_decode_column(slice)?)
                    .map_err(|e| anyhow!("{:?}", e))
                    .map(Into::into)
            }
        }

        impl ColumnEncoder for Bee<$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.to_string().encode(buffer)
            }
        }

        impl TokenEncoder for Bee<$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.to_string().encode(buffer)
            }
        }

        impl TokenEncoder for Bee<&$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&mut $t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.to_string().encode(buffer)
            }
        }

        impl TokenEncoder for Bee<&mut $t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }
    };
}

impl ColumnDecoder for Bee<MilestoneIndex> {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let ms_index = u32::try_decode_column(slice)?;
        Ok(Bee(MilestoneIndex(ms_index)))
    }
}

impl ColumnEncoder for Bee<MilestoneIndex> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.0 .0.encode(buffer)
    }
}

impl TokenEncoder for Bee<MilestoneIndex> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl ColumnEncoder for Bee<&MilestoneIndex> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.0 .0.encode(buffer)
    }
}

impl ColumnDecoder for Bee<Address> {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Address::try_from_bech32(&String::try_decode_column(slice)?)
            .map_err(|e| anyhow!("{:?}", e))
            .map(Into::into)
    }
}

impl ColumnEncoder for Bee<Address> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_bech32("iota").encode(buffer)
    }
}

impl TokenEncoder for Bee<Address> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl ColumnEncoder for Bee<&Address> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_bech32("iota").encode(buffer)
    }
}

impl TokenEncoder for Bee<&Address> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl ColumnEncoder for Bee<&mut Address> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_bech32("iota").encode(buffer)
    }
}

impl TokenEncoder for Bee<&mut Address> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl_simple_packable!(Message);
impl_simple_packable!(Output);
impl_simple_packable!(BasicOutput);
impl_simple_packable!(AliasOutput);
impl_simple_packable!(FoundryOutput);
impl_simple_packable!(NftOutput);
impl_simple_packable!(MilestonePayload);

impl_string_packable!(MessageId);
impl_string_packable!(AliasId);
impl_string_packable!(FoundryId);
impl_string_packable!(NftId);
impl_string_packable!(OutputId);
impl_string_packable!(TransactionId);
impl_string_packable!(Ed25519Address);

impl Row for Bee<Milestone> {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Milestone::new(
            rows.column_value::<Bee<MessageId>>()?.into_inner(),
            rows.column_value()?,
        )
        .into())
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PartitionData {
    pub ms_range_id: u32,
    pub milestone_index: MilestoneIndex,
    pub ms_timestamp: NaiveDateTime,
}

impl PartitionData {
    pub fn new(ms_range_id: u32, milestone_index: MilestoneIndex, ms_timestamp: NaiveDateTime) -> Self {
        Self {
            ms_range_id,
            milestone_index,
            ms_timestamp,
        }
    }
}

impl<B: Binder> Bindable<B> for PartitionData {
    fn bind(&self, binder: B) -> B {
        binder
            .value(self.ms_range_id)
            .value(Bee(&self.milestone_index))
            .value(self.ms_timestamp)
    }
}
/// A result struct which holds a retrieved output as well as all associated unlock blocks
#[derive(Debug, Clone)]
pub struct OutputRes {
    /// The created output's message id
    pub message_id: MessageId,
    /// The output
    pub output: Output,
    /// Zero or more unlock blocks for this output.
    /// Only one can be valid, which indicates the output `is_spent`.
    pub unlock_blocks: Vec<UnlockRes>,
}

/// Milestone data
#[derive(Debug, Deserialize, Serialize)]
pub struct MilestoneData {
    pub message_id: MessageId,
    pub milestone_index: u32,
    pub payload: MilestonePayload,
    pub messages: BTreeSet<MessageRecord>,
}

impl MilestoneData {
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
    pub fn payload(&self) -> &MilestonePayload {
        &self.payload
    }
    /// Get the milestone's messages
    pub fn messages(&self) -> &BTreeSet<MessageRecord> {
        &self.messages
    }
    /// Get the analytics from the collected messages
    pub fn get_analytic_record(&self) -> anyhow::Result<MsAnalyticsRecord> {
        // The accumulators
        let mut transaction_count: u32 = 0;
        let mut transferred_tokens: u64 = 0;

        // Iterate the messages to calculate analytics
        for rec in self.messages() {
            // Accumulate confirmed(included) transaction value
            if let Some(LedgerInclusionState::Included) = rec.inclusion_state {
                if let Some(Payload::Transaction(payload)) = rec.message.payload() {
                    // Accumulate the transaction count
                    transaction_count += 1;
                    let TransactionEssence::Regular(regular_essence) = payload.essence();
                    {
                        for output in regular_essence.outputs() {
                            match output {
                                // Accumulate the transferred token amount
                                Output::SignatureLockedSingle(output) => transferred_tokens += output.amount(),
                                Output::SignatureLockedDustAllowance(output) => transferred_tokens += output.amount(),
                                // Note that the transaction payload don't have Treasury
                                _ => anyhow::bail!("Unexpected Output variant in transaction payload"),
                            }
                        }
                    }
                }
            }
        }
        let analytic_record = MsAnalyticsRecord {
            ms_range_id: MsAnalyticsRecord::range_id(self.milestone_index()),
            milestone_index: MilestoneIndex(self.milestone_index()),
            message_count: self.messages().len() as u32,
            transaction_count,
            transferred_tokens,
        };

        // Return the analytic record
        Ok(analytic_record)
    }
}

/// Milestone data builder
#[derive(Debug, Clone)]
pub struct MilestoneDataBuilder {
    pub(crate) message_id: MessageId,
    pub(crate) milestone_index: u32,
    pub(crate) payload: Option<MilestonePayload>,
    pub(crate) messages: BTreeMap<MessageId, MessageRecord>,
    pub(crate) selected_messages: HashMap<MessageId, Selected>,
    pub(crate) pending: HashSet<MessageId>,
    pub(crate) created_by: CreatedBy,
}

impl MilestoneDataBuilder {
    pub fn new(message_id: MessageId, milestone_index: u32, created_by: CreatedBy) -> Self {
        Self {
            message_id,
            milestone_index,
            payload: None,
            messages: BTreeMap::new(),
            selected_messages: HashMap::new(),
            pending: HashSet::new(),
            created_by,
        }
    }
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }
    pub fn with_payload(mut self, payload: MilestonePayload) -> Self {
        self.payload = Some(payload);
        self
    }
    pub fn set_payload(&mut self, payload: MilestonePayload) {
        self.payload = Some(payload);
    }
    pub fn add_message(&mut self, message: MessageRecord, selected: Option<Selected>) -> anyhow::Result<()> {
        let message_id = message.message_id;
        ensure!(
            self.messages.insert(message_id, message).is_none(),
            "Message already exists"
        );
        if let Some(selected) = selected {
            self.selected_messages.insert(message_id, selected);
        }
        Ok(())
    }
    pub fn add_pending(&mut self, message_id: MessageId) -> anyhow::Result<()> {
        ensure!(self.pending.insert(message_id), "Message already pending");
        Ok(())
    }
    pub fn remove_pending(&mut self, message_id: MessageId) -> anyhow::Result<()> {
        ensure!(self.pending.remove(&message_id), "Message not pending");
        Ok(())
    }
    /// Get the milestone's messages
    pub fn messages(&self) -> &BTreeMap<MessageId, MessageRecord> {
        &self.messages
    }
    /// Get the pending messages
    pub fn pending(&self) -> &HashSet<MessageId> {
        &self.pending
    }
    pub fn selected_messages(&self) -> &HashMap<MessageId, Selected> {
        &self.selected_messages
    }
    /// Get the milestone's messages
    pub fn messages_mut(&mut self) -> &mut BTreeMap<MessageId, MessageRecord> {
        &mut self.messages
    }
    /// Get the pending messages
    pub fn pending_mut(&mut self) -> &mut HashSet<MessageId> {
        &mut self.pending
    }
    pub fn selected_messages_mut(&mut self) -> &mut HashMap<MessageId, Selected> {
        &mut self.selected_messages
    }
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
    pub fn created_by(&self) -> &CreatedBy {
        &self.created_by
    }
    pub fn set_created_by(&mut self, created_by: CreatedBy) {
        self.created_by = created_by;
    }
    pub fn payload(&self) -> &Option<MilestonePayload> {
        &self.payload
    }
    pub fn timestamp(&self) -> Option<u64> {
        self.payload.as_ref().map(|p| p.essence().timestamp())
    }
    pub fn valid(&self) -> bool {
        self.payload.is_some() && self.pending.is_empty()
    }
    pub fn build(self) -> anyhow::Result<MilestoneData> {
        Ok(MilestoneData {
            message_id: self.message_id,
            milestone_index: self.milestone_index,
            payload: self.payload.ok_or_else(|| anyhow::anyhow!("No milestone payload"))?,
            messages: self.messages.into_values().collect(),
        })
    }
}

/// Pre-war Milestone data
/// Used for deserializing old archive files
#[derive(Debug, Deserialize, Serialize)]
pub struct OldMilestoneData {
    pub(crate) milestone_index: u32,
    pub(crate) milestone: MilestonePayload,
    pub(crate) messages: BTreeMap<MessageId, OldFullMessage>,
}

impl TryInto<MilestoneData> for OldMilestoneData {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<MilestoneData, Self::Error> {
        Ok(MilestoneData {
            message_id: self
                .messages
                .values()
                .find_map(|m| match m.0.payload() {
                    Some(payload) => match payload {
                        bee_message::payload::Payload::Milestone(_) => Some(m.1.message_id),
                        _ => None,
                    },
                    None => None,
                })
                .ok_or_else(|| anyhow::anyhow!("No milestone payload in messages!"))?,
            milestone_index: self.milestone_index,
            payload: self.milestone,
            messages: self.messages.into_values().map(Into::into).collect(),
        })
    }
}

/// A "full" message payload, including both message and metadata
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OldFullMessage(pub Message, pub OldMessageMetadata);

impl Into<MessageRecord> for OldFullMessage {
    fn into(self) -> MessageRecord {
        MessageRecord {
            message_id: self.1.message_id,
            message: self.0,
            milestone_index: self.1.referenced_by_milestone_index.map(|i| MilestoneIndex(i)),
            inclusion_state: self.1.ledger_inclusion_state,
            conflict_reason: None,
            proof: None,
        }
    }
}

/// MessageMetadata storage object
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OldMessageMetadata {
    #[serde(rename = "messageId")]
    pub message_id: MessageId,
    #[serde(rename = "parentMessageIds")]
    pub parent_message_ids: Vec<MessageId>,
    #[serde(rename = "isSolid")]
    pub is_solid: bool,
    #[serde(rename = "referencedByMilestoneIndex")]
    pub referenced_by_milestone_index: Option<u32>,
    #[serde(rename = "ledgerInclusionState")]
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
    #[serde(rename = "shouldPromote")]
    pub should_promote: Option<bool>,
    #[serde(rename = "shouldReattach")]
    pub should_reattach: Option<bool>,
}

/// Created by sources
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[repr(u8)]
pub enum CreatedBy {
    /// Created by the new incoming messages from the network
    Incoming = 0,
    /// Created by the new expected messages from the network
    Expected = 1,
    /// Created by solidifiy/sync request from syncer
    Syncer = 2,
    /// Created by the exporter
    Exporter = 3,
}

impl Default for CreatedBy {
    fn default() -> Self {
        Self::Incoming
    }
}

impl From<CreatedBy> for u8 {
    fn from(value: CreatedBy) -> u8 {
        value as u8
    }
}

/// An index in plain-text, unhashed
#[derive(Clone, Debug)]
pub struct Indexation(pub String);

impl ColumnEncoder for Indexation {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.0.encode(buffer)
    }
}

impl TokenEncoder for Indexation {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}
