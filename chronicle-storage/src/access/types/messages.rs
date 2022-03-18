// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bee_rest_api::types::{
    dtos::ChrysalisMessageDto,
    responses::MessageMetadataResponse,
};
use bee_tangle::ConflictReason;
use packable::{
    error::UnpackError,
    prefix::{
        UnpackPrefixError,
        VecPrefix,
    },
};

use bee_message::{
    parent::Parents,
    payload::{
        OptionalPayload,
        Payload,
    },
    Error,
    MessageId,
};

use bee_common::packable::Packable as LegacyPackable;
use packable::{
    error::UnpackErrorExt,
    packer::Packer,
    unpacker::Unpacker,
    Packable,
    PackableExt,
};



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
