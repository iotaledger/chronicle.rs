// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod address_analytics;
mod address_hints;
mod alias_outputs;
mod basic_outputs;
mod daily_analytics;
mod date_cache;
mod foundry_outputs;
mod legacy_outputs;
mod messages;
mod metrics_cache;
mod milestones;
mod ms_analytics;
mod nft_outputs;
mod parents;
mod sync;
mod tag_hints;
mod tags;
mod transactions;

pub use address_analytics::*;
pub use address_hints::*;
pub use alias_outputs::*;
pub use basic_outputs::*;
use bee_rest_api::types::dtos::LedgerInclusionStateDto;
pub use daily_analytics::*;
pub use date_cache::*;
pub use foundry_outputs::*;
pub use legacy_outputs::*;
pub use messages::*;
pub use metrics_cache::*;
pub use milestones::*;
use mongodb::Collection;
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
    milestone::MilestoneIndex,
    output::*,
    payload::{
        transaction::TransactionId,
        MilestonePayload,
        Payload,
    },
    MessageId,
};
use chronicle_common::Wrapper;
use chrono::NaiveDateTime;
use packable::Packable;
use pin_project_lite::pin_project;
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
        VecDeque,
    },
    convert::TryFrom,
    ops::{
        Deref,
        DerefMut,
    },
    str::FromStr,
    task::{
        Context,
        Poll,
    },
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

#[derive(Copy, Clone, Debug)]
pub struct TTL(u32);
impl TTL {
    pub fn new(ttl: u32) -> Self {
        Self(ttl)
    }
}
impl From<u32> for TTL {
    fn from(ttl: u32) -> Self {
        Self(ttl)
    }
}
impl Deref for TTL {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for TTL {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

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

impl<T: IntoIterator> Paged<T> {
    pub fn into_iter(self) -> impl Iterator<Item = T::Item> {
        self.inner.into_iter()
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

pub enum Hint {
    Address(AddressHint),
    Tag(TagHint),
}

#[allow(missing_docs)]
impl Hint {
    pub fn address(address_hint: AddressHint) -> Self {
        Self::Address(address_hint)
    }
    pub fn tag(tag_hint: TagHint) -> Self {
        Self::Tag(tag_hint)
    }
    pub fn legacy_outputs_by_address(address: Address) -> Self {
        Self::address(AddressHint::legacy_outputs_by_address(address))
    }
    pub fn basic_outputs_by_address(address: Address) -> Self {
        Self::address(AddressHint::basic_outputs_by_address(address))
    }
    pub fn basic_outputs_by_sender(address: Address) -> Self {
        Self::address(AddressHint::basic_outputs_by_sender(address))
    }
    pub fn alias_outputs_by_sender(address: Address) -> Self {
        Self::address(AddressHint::alias_outputs_by_sender(address))
    }
    pub fn alias_outputs_by_issuer(address: Address) -> Self {
        Self::address(AddressHint::alias_outputs_by_issuer(address))
    }
    pub fn alias_outputs_by_state_controller(address: Address) -> Self {
        Self::address(AddressHint::alias_outputs_by_state_controller(address))
    }
    pub fn alias_outputs_by_governor(address: Address) -> Self {
        Self::address(AddressHint::alias_outputs_by_governor(address))
    }
    pub fn foundry_outputs_by_address(address: Address) -> Self {
        Self::address(AddressHint::foundry_outputs_by_address(address))
    }
    pub fn nft_outputs_by_address(address: Address) -> Self {
        Self::address(AddressHint::nft_outputs_by_address(address))
    }
    pub fn nft_outputs_by_dust_return_address(address: Address) -> Self {
        Self::address(AddressHint::nft_outputs_by_dust_return_address(address))
    }
    pub fn nft_outputs_by_sender(address: Address) -> Self {
        Self::address(AddressHint::nft_outputs_by_sender(address))
    }
    pub fn nft_outputs_by_issuer(address: Address) -> Self {
        Self::address(AddressHint::nft_outputs_by_issuer(address))
    }
    pub fn tags(tag: String) -> Self {
        Self::tag(TagHint::regular(tag))
    }
    pub fn basic_outputs_by_tag(tag: String) -> Self {
        Self::tag(TagHint::basic_output(tag))
    }
    pub fn nft_outputs_by_tag(tag: String) -> Self {
        Self::tag(TagHint::nft_output(tag))
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum OutputSpendKind {
    Created = 0,
    Used = 1,
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MilestoneData {
    pub milestone: MilestoneMessage,
    pub messages: BTreeSet<MessageRecord>,
}

impl MilestoneData {
    pub fn new(milestone: MilestoneMessage) -> Self {
        Self {
            milestone,
            messages: BTreeSet::new(),
        }
    }
    pub fn message_id(&self) -> &MessageId {
        self.milestone.message().message_id()
    }
    pub fn milestone_index(&self) -> MilestoneIndex {
        self.milestone.milestone_index()
    }
    /// Get the milestone's messages
    pub fn messages(&self) -> &BTreeSet<MessageRecord> {
        &self.messages
    }

    pub fn milestone(&self) -> &MilestoneMessage {
        &self.milestone
    }

    pub fn milestone_payload(&self) -> &MilestonePayload {
        match self
            .milestone()
            .message()
            .payload()
            .expect("Milestone message is not a milestone")
        {
            Payload::Milestone(ref payload) => &**payload,
            _ => panic!("Milestone message is not a milestone"),
        }
    }
}

/// Milestone data builder
#[derive(Debug, Clone)]
pub struct MilestoneDataBuilder {
    pub(crate) milestone_index: u32,
    pub(crate) milestone: Option<MilestoneMessage>,
    pub(crate) messages: BTreeMap<MessageId, MessageRecord>,
    pub(crate) selected_messages: HashMap<MessageId, Selected>,
    pub(crate) pending: HashSet<MessageId>,
    pub(crate) created_by: CreatedBy,
}

impl MilestoneDataBuilder {
    pub fn new(milestone_index: u32, created_by: CreatedBy) -> Self {
        Self {
            milestone_index,
            milestone: None,
            messages: BTreeMap::new(),
            selected_messages: HashMap::new(),
            pending: HashSet::new(),
            created_by,
        }
    }
    pub fn with_milestone(mut self, milestone_message: MilestoneMessage) -> Self {
        self.milestone.replace(milestone_message);
        self
    }
    pub fn set_milestone(&mut self, milestone_message: MilestoneMessage) {
        self.milestone.replace(milestone_message);
    }
    pub fn add_message(&mut self, message: MessageRecord, selected: Option<Selected>) -> anyhow::Result<()> {
        let message_id = message.message_id;
        self.messages.insert(message_id, message);
        if let Some(selected) = selected {
            self.selected_messages.insert(message_id, selected);
        }
        Ok(())
    }
    pub fn add_pending(&mut self, message_id: MessageId) -> bool {
        self.pending.insert(message_id)
    }
    pub fn remove_pending(&mut self, message_id: MessageId) -> bool {
        self.pending.remove(&message_id)
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
    pub fn milestone(&self) -> &Option<MilestoneMessage> {
        &self.milestone
    }
    pub fn timestamp(&self) -> Option<u64> {
        self.milestone.as_ref().map(|m| m.timestamp())
    }
    pub fn valid(&self) -> bool {
        self.milestone.is_some() && self.pending.is_empty()
    }
    pub fn build(self) -> anyhow::Result<MilestoneData> {
        Ok(MilestoneData {
            milestone: self.milestone.ok_or_else(|| anyhow::anyhow!("No milestone payload"))?,
            messages: self.messages.into_values().collect(),
        })
    }
}

impl From<MilestoneData> for MilestoneDataBuilder {
    fn from(d: MilestoneData) -> Self {
        Self {
            milestone_index: d.milestone.milestone_index().0,
            milestone: Some(d.milestone),
            selected_messages: d
                .messages
                .iter()
                .map(|r| {
                    (
                        r.message_id,
                        Selected {
                            require_proof: r.proof().is_some(),
                        },
                    )
                })
                .collect(),
            messages: d.messages.into_iter().map(|r| (r.message_id, r)).collect(),
            pending: Default::default(),
            created_by: CreatedBy::Importer,
        }
    }
}
pin_project! {
    #[must_use = "futures/streams do nothing unless you poll them"]
    pub struct MilestoneDataSearch {
        #[pin]
        data: MilestoneDataBuilder,
        #[pin]
        should_be_visited: VecDeque<Proof>,
        #[pin]
        visited: HashSet<MessageId>,
        budget: usize,
        counter: usize,
    }
}

impl std::convert::TryFrom<MilestoneDataBuilder> for MilestoneDataSearch {
    type Error = anyhow::Error;

    fn try_from(data: MilestoneDataBuilder) -> Result<Self, Self::Error> {
        if !data.valid() {
            anyhow::bail!("cannot make milestone data search struct for uncompleted milestone data")
        }
        let milestone_message_id = data
            .milestone()
            .as_ref()
            .unwrap() // unwrap is safe, as this right after valid check.
            .message()
            .message_id();
        let mut should_be_visited = VecDeque::new();
        // we start from the root
        should_be_visited.push_back(Proof::new(data.milestone_index(), vec![*milestone_message_id]));
        Ok(Self {
            data,
            should_be_visited,
            visited: Default::default(),
            budget: 128,
            counter: 0,
        })
    }
}

impl futures::stream::Stream for MilestoneDataSearch {
    type Item = MessageRecord;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut project = self.as_mut().project();
        if project.counter == project.budget {
            *project.counter = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        while let Some(current_proof) = project.should_be_visited.pop_front() {
            *project.counter += 1;
            // safe to unwrap.
            let message_id = *current_proof.path().last().unwrap();
            project.visited.insert(message_id);
            // check if message is selected
            let is_selected = project.data.selected_messages().contains_key(&message_id);
            // iterate over its parents
            if let Some(message) = project.data.messages_mut().get_mut(&message_id) {
                let parents_iter = message.parents().iter();
                for parent_id in parents_iter {
                    if !project.visited.contains(parent_id) {
                        let mut vertex = current_proof.clone();
                        vertex.path_mut().push(parent_id.clone());
                        project.should_be_visited.push_back(vertex);
                    }
                }
                // check if this message is selected
                if is_selected {
                    message.proof.replace(current_proof);
                    return Poll::Ready(Some(message.clone()));
                } else {
                    return Poll::Ready(Some(message.clone()));
                }
            } else {
                // reached the end of the branch, proceed to the next should_be_visited
                continue;
            }
        }
        Poll::Ready(None)
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
            milestone: self
                .messages
                .values()
                .find_map(|m| match m.0.payload() {
                    Some(payload) => match payload {
                        bee_message_old::payload::Payload::Milestone(_) => {
                            let m: MessageRecord = m.clone().into();
                            Some(m.try_into().unwrap()) // safe to unwrap as the milestone payload check already done.
                        }
                        _ => None,
                    },
                    None => None,
                })
                .ok_or_else(|| anyhow::anyhow!("No milestone payload in messages!"))?,
            messages: self.messages.into_values().map(Into::into).collect(),
        })
    }
}

/// A "full" message payload, including both message and metadata
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OldFullMessage(pub bee_message_old::Message, pub OldMessageMetadata);

impl Into<MessageRecord> for OldFullMessage {
    fn into(self) -> MessageRecord {
        MessageRecord {
            message_id: self.1.message_id,
            message: self.0.try_into().unwrap(),
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
    /// Created by solidify/sync request from syncer
    Syncer = 2,
    /// Created by the exporter
    Exporter = 3,
    /// Created by the archive file importer
    Importer = 4,
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
