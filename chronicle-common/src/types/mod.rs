// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "mongo")]
mod mongo;

use anyhow::*;
use bee_message_shimmer::semantic::ConflictReason;
use derive_more::From;
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
    fmt::Display,
    ops::{
        Deref,
        DerefMut,
        Range,
    },
    str::FromStr,
    task::{
        Context,
        Poll,
    },
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, From, Hash, Ord, PartialOrd)]
pub enum MessageId {
    /// Chrysalis compatible message
    Chrysalis(bee_message_cpt2::MessageId),
    /// Shimmer compatible message
    Shimmer(bee_message_shimmer::MessageId),
}

impl MessageId {
    pub fn is_null(&self) -> bool {
        match self {
            MessageId::Chrysalis(id) => id == &bee_message_cpt2::MessageId::null(),
            MessageId::Shimmer(id) => id == &bee_message_shimmer::MessageId::null(),
        }
    }
}

impl Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chrysalis(id) => write!(f, "{}", id),
            Self::Shimmer(id) => write!(f, "{}", id),
        }
    }
}

impl FromStr for MessageId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(bee_message_shimmer::MessageId::from_str(s)
            .map(Self::Shimmer)
            .or_else(|_| bee_message_cpt2::MessageId::from_str(s).map(Self::Chrysalis))?)
    }
}

/// Represent versioned message type.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, From)]
pub enum Message {
    /// Chrysalis compatible message
    Chrysalis(bee_message_cpt2::Message),
    /// Shimmer compatible message
    Shimmer(bee_message_shimmer::Message),
}

impl Message {
    pub fn protocol_version(&self) -> u8 {
        match self {
            Message::Chrysalis(_) => 0,
            Message::Shimmer(m) => m.protocol_version(),
        }
    }
}

impl std::convert::TryFrom<bee_rest_api_cpt2::types::dtos::MessageDto> for Message {
    type Error = anyhow::Error;
    fn try_from(chrysalis_dto_message: bee_rest_api_cpt2::types::dtos::MessageDto) -> Result<Self, Self::Error> {
        Ok(Self::Chrysalis(
            bee_message_cpt2::Message::try_from(&chrysalis_dto_message)?.into(),
        ))
    }
}

impl std::convert::TryFrom<bee_rest_api_shimmer::types::dtos::MessageDto> for Message {
    type Error = anyhow::Error;
    fn try_from(shimmer_dto_message: bee_rest_api_shimmer::types::dtos::MessageDto) -> Result<Self, Self::Error> {
        Ok(Self::Shimmer(
            bee_message_shimmer::Message::try_from(&shimmer_dto_message)?.into(),
        ))
    }
}

impl Message {
    /// Returns the message id
    pub fn id(&self) -> MessageId {
        match self {
            Self::Chrysalis(msg) => MessageId::Chrysalis(msg.id().0),
            Self::Shimmer(msg) => MessageId::Shimmer(msg.id()),
        }
    }
    /// Returns the parents of the message
    pub fn parents(&self) -> impl Iterator<Item = MessageId> + '_ {
        match self {
            Self::Chrysalis(msg) => {
                Box::new(msg.parents().iter().map(|p| MessageId::Chrysalis(*p))) as Box<dyn Iterator<Item = MessageId>>
            }
            Self::Shimmer(msg) => {
                Box::new(msg.parents().iter().map(|p| MessageId::Shimmer(*p))) as Box<dyn Iterator<Item = MessageId>>
            }
        }
    }
    /// Check if the message has milestone payload
    pub fn is_milestone(&self) -> bool {
        match self {
            Self::Chrysalis(msg) => {
                if let Some(bee_message_cpt2::payload::Payload::Milestone(_)) = msg.payload() {
                    return true;
                }
            }
            Self::Shimmer(msg) => {
                if let Some(bee_message_shimmer::payload::Payload::Milestone(_)) = msg.payload() {
                    return true;
                }
            }
        }
        false
    }
}
/// Chronicle Message record
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MessageRecord {
    pub message_id: MessageId,
    // TODO: make use of protocol version to deserialize this
    pub message: Message,
    pub milestone_index: Option<u32>,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub conflict_reason: Option<ConflictReason>,
    pub proof: Option<Proof>,
    pub protocol_version: u8,
}

impl MessageRecord {
    /// Create new message record
    pub fn new(message_id: MessageId, message: Message) -> Self {
        Self {
            message_id,
            protocol_version: message.protocol_version(),
            message,
            milestone_index: None,
            inclusion_state: None,
            conflict_reason: None,
            proof: None,
        }
    }
    /// Return Message id of the message
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }
    /// Return the message
    pub fn message(&self) -> &Message {
        &self.message
    }
    /// Return referenced milestone index
    pub fn milestone_index(&self) -> Option<u32> {
        self.milestone_index
    }
    /// Return inclusion_state
    pub fn inclusion_state(&self) -> Option<&LedgerInclusionState> {
        self.inclusion_state.as_ref()
    }
    /// Return conflict_reason
    pub fn conflict_reason(&self) -> Option<&ConflictReason> {
        self.conflict_reason.as_ref()
    }
    /// Return proof
    pub fn proof(&self) -> Option<&Proof> {
        self.proof.as_ref()
    }

    /// Get the message's nonce
    pub fn nonce(&self) -> u64 {
        match &self.message {
            Message::Chrysalis(m) => m.nonce(),
            Message::Shimmer(m) => m.nonce(),
        }
    }
}

impl Deref for MessageRecord {
    type Target = Message;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl DerefMut for MessageRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
    }
}

impl PartialOrd for MessageRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MessageRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.message_id.cmp(&other.message_id)
    }
}

impl PartialEq for MessageRecord {
    fn eq(&self, other: &Self) -> bool {
        self.message_id == other.message_id
    }
}
impl Eq for MessageRecord {}

impl From<(Message, bee_rest_api_shimmer::types::responses::MessageMetadataResponse)> for MessageRecord {
    fn from((message, metadata): (Message, bee_rest_api_shimmer::types::responses::MessageMetadataResponse)) -> Self {
        MessageRecord {
            message_id: message.id(),
            protocol_version: message.protocol_version(),
            message,
            milestone_index: metadata.referenced_by_milestone_index,
            inclusion_state: metadata.ledger_inclusion_state.map(Into::into),
            conflict_reason: metadata.conflict_reason.and_then(|c| c.try_into().ok()),
            proof: None,
        }
    }
}

impl From<(Message, bee_rest_api_cpt2::types::responses::MessageMetadataResponse)> for MessageRecord {
    fn from((message, metadata): (Message, bee_rest_api_cpt2::types::responses::MessageMetadataResponse)) -> Self {
        MessageRecord {
            message_id: message.id(),
            protocol_version: message.protocol_version(),
            message,
            milestone_index: metadata.referenced_by_milestone_index,
            inclusion_state: metadata.ledger_inclusion_state.map(Into::into),
            conflict_reason: metadata.conflict_reason.and_then(|c| c.try_into().ok()),
            proof: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Proof {
    milestone_index: u32,
    path: Vec<MessageId>,
}

impl Proof {
    pub fn new(milestone_index: u32, path: Vec<MessageId>) -> Self {
        Self { milestone_index, path }
    }
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
    pub fn path(&self) -> &[MessageId] {
        &self.path
    }
    pub fn path_mut(&mut self) -> &mut Vec<MessageId> {
        &mut self.path
    }
}

/// A milestone message
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MilestoneMessage {
    message: MessageRecord,
}

impl std::convert::TryFrom<MessageRecord> for MilestoneMessage {
    type Error = anyhow::Error;
    fn try_from(message: MessageRecord) -> Result<Self, Self::Error> {
        if message.is_milestone() {
            Ok(Self { message })
        } else {
            bail!("Failed to create MilestoneMessage from regular message record without milestone payload")
        }
    }
}

impl MilestoneMessage {
    pub fn new(message: MessageRecord) -> Self {
        Self { message }
    }
    /// Returns the message record
    pub fn message(&self) -> &MessageRecord {
        &self.message
    }
    /// Returns the milestone index
    pub fn milestone_index(&self) -> bee_message_shimmer::milestone::MilestoneIndex {
        match self.message.message() {
            Message::Chrysalis(msg) => {
                // unwrap is safe, as the milestone message cannot be created unless it contains milestone payload
                if let bee_message_cpt2::payload::Payload::Milestone(ms_payload) = msg.payload().as_ref().unwrap() {
                    ms_payload.essence().index().0.into()
                } else {
                    unreachable!("No milestone payload in milestone message")
                }
            }
            Message::Shimmer(msg) => {
                // unwrap is safe, as the milestone message cannot be created unless it contains milestone payload
                if let bee_message_shimmer::payload::Payload::Milestone(ms_payload) = msg.payload().as_ref().unwrap() {
                    ms_payload.essence().index()
                } else {
                    unreachable!("No milestone payload in milestone message")
                }
            }
        }
    }
    /// Returns the timestamp of a [MilestoneEssence].
    pub fn timestamp(&self) -> u64 {
        match self.message.message() {
            Message::Chrysalis(msg) => {
                // unwrap is safe, as the milestone message cannot be created unless it contains milestone payload
                if let bee_message_cpt2::payload::Payload::Milestone(ms_payload) = msg.payload().as_ref().unwrap() {
                    ms_payload.essence().timestamp()
                } else {
                    unreachable!("No milestone payload in milestone message")
                }
            }
            Message::Shimmer(msg) => {
                // unwrap is safe, as the milestone message cannot be created unless it contains milestone payload
                if let bee_message_shimmer::payload::Payload::Milestone(ms_payload) = msg.payload().as_ref().unwrap() {
                    ms_payload.essence().timestamp()
                } else {
                    unreachable!("No milestone payload in milestone message")
                }
            }
        }
    }

    pub fn into_inner(self) -> MessageRecord {
        self.message
    }
}

impl Deref for MilestoneMessage {
    type Target = MessageRecord;
    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

/// A message's ledger inclusion state
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

impl From<bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto> for LedgerInclusionState {
    fn from(value: bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto) -> Self {
        match value {
            bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto::Conflicting => Self::Conflicting,
            bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto::Included => Self::Included,
            bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto::NoTransaction => Self::NoTransaction,
        }
    }
}

impl Into<bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto> for LedgerInclusionState {
    fn into(self) -> bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto {
        match self {
            Self::Conflicting => bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto::Conflicting,
            Self::Included => bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto::Included,
            Self::NoTransaction => bee_rest_api_cpt2::types::dtos::LedgerInclusionStateDto::NoTransaction,
        }
    }
}

impl From<bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto> for LedgerInclusionState {
    fn from(value: bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto) -> Self {
        match value {
            bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto::Conflicting => Self::Conflicting,
            bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto::Included => Self::Included,
            bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto::NoTransaction => Self::NoTransaction,
        }
    }
}

impl Into<bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto> for LedgerInclusionState {
    fn into(self) -> bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto {
        match self {
            Self::Conflicting => bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto::Conflicting,
            Self::Included => bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto::Included,
            Self::NoTransaction => bee_rest_api_shimmer::types::dtos::LedgerInclusionStateDto::NoTransaction,
        }
    }
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
    pub fn milestone_index(&self) -> bee_message_shimmer::milestone::MilestoneIndex {
        self.milestone.milestone_index()
    }
    /// Get the milestone's messages
    pub fn messages(&self) -> &BTreeSet<MessageRecord> {
        &self.messages
    }

    pub fn milestone(&self) -> &MilestoneMessage {
        &self.milestone
    }
}
/// Created by sources
#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
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

#[derive(Debug, Copy, Clone)]
pub struct Selected {
    /// Store proof in the database
    pub(crate) require_proof: bool,
}

impl Selected {
    pub fn select() -> Self {
        Self { require_proof: false }
    }
    pub fn with_proof(mut self) -> Self {
        self.require_proof = true;
        self
    }
    /// Check if we have to store the proof of inclusion
    pub fn require_proof(&self) -> bool {
        self.require_proof
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
            let selected = project.data.selected_messages().get(&message_id).clone();
            let (is_selected, require_proof) = selected.map(|s| (true, s.require_proof())).unwrap_or_default();
            // iterate over its parents
            if let Some(message) = project.data.messages_mut().get_mut(&message_id) {
                let parents_iter = message.parents();
                for parent_id in parents_iter {
                    if !project.visited.contains(&parent_id) {
                        let mut vertex = current_proof.clone();
                        vertex.path_mut().push(parent_id.clone());
                        project.should_be_visited.push_back(vertex);
                    }
                }
                // check if this message is selected
                if is_selected {
                    if require_proof {
                        message.proof.replace(current_proof);
                    }
                    return Poll::Ready(Some(message.clone()));
                } else {
                    if project.counter == project.budget {
                        *project.counter = 0;
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    continue;
                }
            } else {
                // reached the end of the branch, proceed to the next should_be_visited
                if project.counter == project.budget {
                    *project.counter = 0;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                continue;
            }
        }
        Poll::Ready(None)
    }
}

/// ASC ordering wrapper
pub struct Ascending<T> {
    inner: T,
}

impl<T> Deref for Ascending<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::convert::Into<MilestoneData> for Ascending<MilestoneData> {
    fn into(self) -> MilestoneData {
        self.inner
    }
}

impl<T> Ascending<T> {
    /// Wrap inner with ASC ordering
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}
impl Ascending<(Option<MilestoneData>, u32)> {
    /// Returns the milestone index
    pub fn milestone_index(&self) -> u32 {
        self.1
    }
}

impl Ascending<MilestoneData> {
    /// Returns the milestone index
    pub fn milestone_index(&self) -> u32 {
        self.inner.milestone_index().0
    }
}
impl std::convert::Into<Option<MilestoneData>> for Ascending<(Option<MilestoneData>, u32)> {
    fn into(self) -> Option<MilestoneData> {
        self.inner.0
    }
}

impl std::cmp::Ord for Ascending<MilestoneData> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.inner.milestone_index().cmp(&self.inner.milestone_index())
    }
}

impl std::cmp::PartialOrd for Ascending<MilestoneData> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.inner.milestone_index().cmp(&self.inner.milestone_index()))
    }
}

impl std::cmp::PartialEq for Ascending<MilestoneData> {
    fn eq(&self, other: &Self) -> bool {
        if self.inner.milestone_index() == other.inner.milestone_index() {
            true
        } else {
            false
        }
    }
}

impl std::cmp::Eq for Ascending<MilestoneData> {}

impl std::cmp::Ord for Ascending<(Option<MilestoneData>, u32)> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.inner.1.cmp(&self.inner.1)
    }
}

impl std::cmp::PartialOrd for Ascending<(Option<MilestoneData>, u32)> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.inner.1.cmp(&self.inner.1))
    }
}

impl std::cmp::PartialEq for Ascending<(Option<MilestoneData>, u32)> {
    fn eq(&self, other: &Self) -> bool {
        if self.inner.1 == other.inner.1 {
            true
        } else {
            false
        }
    }
}

impl std::cmp::Eq for Ascending<(Option<MilestoneData>, u32)> {}

/// Identify theoretical nodeid which updated/set the synced_by column in sync table
pub type SyncedBy = u8;
/// Identify theoretical nodeid which updated/set the logged_by column in sync table.
/// This enables the admin to locate the generated logs across cluster of chronicles
pub type LoggedBy = u8;
/// A 'sync' table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SyncRecord {
    pub milestone_index: u32,
    pub synced_by: Option<SyncedBy>,
    pub logged_by: Option<LoggedBy>,
}

impl SyncRecord {
    /// Creates a new sync row
    pub fn new(milestone_index: u32, synced_by: Option<SyncedBy>, logged_by: Option<LoggedBy>) -> Self {
        Self {
            milestone_index,
            synced_by,
            logged_by,
        }
    }
}

/// Representation of the database sync data
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SyncData {
    /// The completed(synced and logged) milestones data
    pub completed: Vec<Range<u32>>,
    /// Synced milestones data but unlogged
    pub synced_but_unlogged: Vec<Range<u32>>,
    /// Gaps/missings milestones data
    pub gaps: Vec<Range<u32>>,
}

impl SyncData {
    /// Takes the lowest gap from the sync_data
    pub fn take_lowest_gap(&mut self) -> Option<Range<u32>> {
        self.gaps.pop()
    }
    /// Takes the lowest unlogged range from the sync_data
    pub fn take_lowest_unlogged(&mut self) -> Option<Range<u32>> {
        self.synced_but_unlogged.pop()
    }
    /// Takes the lowest unlogged or gap from the sync_data
    pub fn take_lowest_gap_or_unlogged(&mut self) -> Option<Range<u32>> {
        let lowest_gap = self.gaps.last();
        let lowest_unlogged = self.synced_but_unlogged.last();
        match (lowest_gap, lowest_unlogged) {
            (Some(gap), Some(unlogged)) => {
                if gap.start < unlogged.start {
                    self.gaps.pop()
                } else {
                    self.synced_but_unlogged.pop()
                }
            }
            (Some(_), None) => self.gaps.pop(),
            (None, Some(_)) => self.synced_but_unlogged.pop(),
            _ => None,
        }
    }
    /// Takes the lowest uncomplete(mixed range for unlogged and gap) from the sync_data
    pub fn take_lowest_uncomplete(&mut self) -> Option<Range<u32>> {
        if let Some(mut pre_range) = self.take_lowest_gap_or_unlogged() {
            loop {
                if let Some(next_range) = self.get_lowest_gap_or_unlogged() {
                    if next_range.start.eq(&pre_range.end) {
                        pre_range.end = next_range.end;
                        let _ = self.take_lowest_gap_or_unlogged();
                    } else {
                        return Some(pre_range);
                    }
                } else {
                    return Some(pre_range);
                }
            }
        } else {
            None
        }
    }
    pub fn get_lowest_gap_or_unlogged(&self) -> Option<&Range<u32>> {
        let lowest_gap = self.gaps.last();
        let lowest_unlogged = self.synced_but_unlogged.last();
        match (lowest_gap, lowest_unlogged) {
            (Some(gap), Some(unlogged)) => {
                if gap.start < unlogged.start {
                    self.gaps.last()
                } else {
                    self.synced_but_unlogged.last()
                }
            }
            (Some(_), None) => self.gaps.last(),
            (None, Some(_)) => self.synced_but_unlogged.last(),
            _ => None,
        }
    }
    pub fn process_rest(&mut self, logged_by: &Option<u8>, milestone_index: u32, pre_lb: &Option<u8>) {
        if logged_by.is_some() {
            // process logged
            Self::proceed(&mut self.completed, milestone_index, pre_lb.is_some());
        } else {
            // process_unlogged
            let unlogged = &mut self.synced_but_unlogged;
            Self::proceed(unlogged, milestone_index, pre_lb.is_none());
        }
    }
    pub fn process_gaps(&mut self, pre_ms: u32, milestone_index: u32) {
        let gap_start = milestone_index + 1;
        if gap_start != pre_ms {
            // create missing gap
            let gap = Range {
                start: gap_start,
                end: pre_ms,
            };
            self.gaps.push(gap);
        }
    }
    pub fn proceed(ranges: &mut Vec<Range<u32>>, milestone_index: u32, check: bool) {
        let end_ms = milestone_index + 1;
        if let Some(Range { start, .. }) = ranges.last_mut() {
            if check && *start == end_ms {
                *start = milestone_index;
            } else {
                let range = Range {
                    start: milestone_index,
                    end: end_ms,
                };
                ranges.push(range)
            }
        } else {
            let range = Range {
                start: milestone_index,
                end: end_ms,
            };
            ranges.push(range);
        };
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

impl<T> crate::Wrapper for JsonData<T> {
    fn into_inner(self) -> Self::Target {
        self.data
    }
}

/// Pre-war Milestone data
/// Used for deserializing old archive files
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct OldMilestoneData {
    pub(crate) milestone_index: u32,
    pub(crate) milestone: Option<Box<bee_message_cpt2::payload::milestone::MilestonePayload>>,
    pub(crate) messages: HashMap<MessageId, OldFullMessage>,
    pub(crate) pending: HashMap<MessageId, ()>,
    pub(crate) created_by: CreatedBy,
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
                        bee_message_cpt2::payload::Payload::Milestone(_) => {
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
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct OldFullMessage(pub bee_message_cpt2::Message, pub OldMessageMetadata);

impl Into<MessageRecord> for OldFullMessage {
    fn into(self) -> MessageRecord {
        MessageRecord {
            message_id: self.1.message_id,
            protocol_version: 0,
            message: self.0.into(),
            milestone_index: self.1.referenced_by_milestone_index,
            inclusion_state: self.1.ledger_inclusion_state,
            conflict_reason: None,
            proof: None,
        }
    }
}

/// MessageMetadata storage object
#[allow(missing_docs)]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
