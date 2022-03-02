// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bee_rest_api::types::responses::MessageMetadataResponse;
use bee_tangle::ConflictReason;
use packable::{
    error::UnpackError,
    prefix::{
        UnpackPrefixError,
        VecPrefix,
    },
};
use std::convert::Infallible;

/// Chronicle Message record
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageRecord {
    pub message_id: MessageId,
    pub message: Message,
    pub milestone_index: Option<MilestoneIndex>,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub conflict_reason: Option<ConflictReason>,
    pub proof: Option<Proof>,
}

impl MessageRecord {
    /// Create new message record
    pub fn new(message_id: MessageId, message: Message) -> Self {
        Self {
            message_id,
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
    pub fn milestone_index(&self) -> Option<&MilestoneIndex> {
        self.milestone_index.as_ref()
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

impl From<Message> for MessageRecord {
    fn from(message: Message) -> Self {
        MessageRecord {
            message_id: message.id(),
            message,
            milestone_index: None,
            inclusion_state: None,
            conflict_reason: None,
            proof: None,
        }
    }
}

impl From<(Message, MessageMetadataResponse)> for MessageRecord {
    fn from((message, metadata): (Message, MessageMetadataResponse)) -> Self {
        MessageRecord {
            message_id: message.id(),
            message,
            milestone_index: metadata.referenced_by_milestone_index.map(|i| MilestoneIndex(i)),
            inclusion_state: metadata.ledger_inclusion_state.map(Into::into),
            conflict_reason: metadata.conflict_reason.and_then(|c| c.try_into().ok()),
            proof: None,
        }
    }
}

impl TokenEncoder for MessageRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.message_id)).into()
    }
}

impl Row for MessageRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            message_id: rows.column_value::<Bee<MessageId>>()?.into_inner(),
            message: rows.column_value::<Bee<Message>>()?.into_inner(),
            milestone_index: rows
                .column_value::<Option<Bee<MilestoneIndex>>>()?
                .map(|a| a.into_inner()),
            inclusion_state: rows.column_value()?,
            conflict_reason: rows.column_value::<Option<u8>>()?.map(|r| r.try_into()).transpose()?,
            proof: rows.column_value()?,
        })
    }
}

impl<B: Binder> Bindable<B> for MessageRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.message_id))
            .value(Bee(&self.message))
            .value(self.milestone_index.as_ref().map(Bee))
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
            .value(self.conflict_reason.as_ref().map(|c| *c as u8))
            .value(&self.proof)
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Packable)]
#[packable(unpack_error = anyhow::Error)]
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

impl ColumnEncoder for Proof {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.packed_len() as i32));
        self.pack(buffer).ok();
    }
}

impl ColumnDecoder for Proof {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Self::unpack_verified(slice).map_err(|e| match e {
            UnpackError::Packable(e) => e,
            UnpackError::Unpacker(e) => anyhow!(e),
        })
    }
}

/// A milestone message
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MilestoneMessage {
    message: MessageRecord,
}

impl std::convert::TryFrom<MessageRecord> for MilestoneMessage {
    type Error = anyhow::Error;
    fn try_from(message: MessageRecord) -> Result<Self, Self::Error> {
        if let Some(Payload::Milestone(ms_payload)) = message.payload() {
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
    pub fn milestone_index(&self) -> MilestoneIndex {
        // unwrap is safe, as the milestone message cannot be created unless it contains milestone payload
        if let Payload::Milestone(ms_payload) = self
            .message
            .payload()
            .expect("Failed to unwrap milestone payload from milestone message")
        {
            ms_payload.essence().index()
        } else {
            unreachable!("No milestone payload in milestone message")
        }
    }
    /// Returns the timestamp of a [MilestoneEssence].
    pub fn timestamp(&self) -> u64 {
        // unwrap is safe, as the milestone message cannot be created unless it contains milestone payload
        if let Payload::Milestone(ms_payload) = self
            .message
            .payload()
            .expect("Failed to unwrap milestone payload from milestone message")
        {
            ms_payload.essence().timestamp()
        } else {
            unreachable!("No milestone payload in milestone message")
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
