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

use std::convert::Infallible;

use bee_message::{
    parent::Parents,
    payload::{
        OptionalPayload,
        Payload,
    },
    Error,
    MessageId,
};

use bee_pow::providers::{
    miner::Miner,
    NonceProvider,
    NonceProviderBuilder,
};

use crypto::hashes::{
    blake2b::Blake2b256,
    Digest,
};

use packable::{
    error::UnpackErrorExt,
    packer::Packer,
    unpacker::Unpacker,
    Packable,
    PackableExt,
};

/// A builder to build a [`Message`].
#[must_use]
pub struct MessageBuilder<P: NonceProvider = u64> {
    protocol_id: Option<ProtocolId>,
    parents: Option<Parents>,
    payload: Option<Payload>,
    nonce_provider: Option<(P, f64)>,
}

impl<P: NonceProvider> Default for MessageBuilder<P> {
    fn default() -> Self {
        Self {
            protocol_id: None,
            parents: None,
            payload: None,
            nonce_provider: None,
        }
    }
}

impl<P: NonceProvider> MessageBuilder<P> {
    const DEFAULT_POW_SCORE: f64 = 4000f64;
    const DEFAULT_NONCE: u64 = 0;

    /// Creates a new [`MessageBuilder`].
    #[inline(always)]
    pub fn new() -> Self {
        Default::default()
    }
    /// Adds a protocol id to a [`MessageBuilder`].
    #[inline(always)]
    pub fn with_protocol_id(mut self, protocol_id: ProtocolId) -> Self {
        self.protocol_id = Some(protocol_id);
        self
    }
    /// Adds parents to a [`MessageBuilder`].
    #[inline(always)]
    pub fn with_parents(mut self, parents: Parents) -> Self {
        self.parents = Some(parents);
        self
    }

    /// Adds a payload to a [`MessageBuilder`].
    #[inline(always)]
    pub fn with_payload(mut self, payload: Payload) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Adds a nonce provider to a [`MessageBuilder`].
    #[inline(always)]
    pub fn with_nonce_provider(mut self, nonce_provider: P, target_score: f64) -> Self {
        self.nonce_provider = Some((nonce_provider, target_score));
        self
    }

    /// Finishes the [`MessageBuilder`] into a [`Message`].
    pub fn finish(self) -> Result<Message, Error> {
        let protocol_id = self.protocol_id.ok_or(Error::MissingField("protocol_id"))?;

        let parents = self.parents.ok_or(Error::MissingField("parents"))?;

        if !matches!(
            self.payload,
            None | Some(Payload::Transaction(_))
                | Some(Payload::Milestone(_))
                | Some(Payload::TaggedData(_))
                | Some(Payload::Indexation(_))
                | Some(Payload::ChrysalisTransaction(_))
        ) {
            // Safe to unwrap since it's known not to be None.
            return Err(Error::InvalidPayloadKind(self.payload.unwrap().kind()));
        }

        let mut message = Message {
            protocol_id,
            parents,
            payload: self.payload.into(),
            nonce: 0,
        };

        let message_bytes = message.pack_to_vec();

        if message_bytes.len() > Message::LENGTH_MAX {
            return Err(Error::InvalidMessageLength(message_bytes.len()));
        }

        let (nonce_provider, target_score) = self
            .nonce_provider
            .unwrap_or((P::Builder::new().finish(), Self::DEFAULT_POW_SCORE));

        message.nonce = nonce_provider
            .nonce(
                &message_bytes[..message_bytes.len() - core::mem::size_of::<u64>()],
                target_score,
            )
            .unwrap_or(Self::DEFAULT_NONCE);
        Ok(message)
    }
}

/// Represent the Protocol id for both legacy and stardust message type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Packable, serde::Deserialize, serde::Serialize)]
#[packable(tag_type = u8)]
pub enum ProtocolId {
    /// Chrysalis network id
    #[packable(tag = 0)]
    NetworkId(u64),
    /// Stardust protocol version
    #[packable(tag = 1)]
    ProtocolVersion(u8),
}

/// Represent the object that nodes gossip around the network.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Message {
    // Protocol id with backward compatability
    protocol_id: ProtocolId,
    /// The [`MessageId`]s that this message directly approves.
    parents: Parents,
    /// The optional [Payload] of the message.
    payload: OptionalPayload,
    /// The result of the Proof of Work in order for the message to be accepted into the tangle.
    nonce: u64,
}

impl Message {
    /// The minimum number of bytes in a message.
    pub const LENGTH_MIN: usize = 53;
    /// The maximum number of bytes in a message.
    pub const LENGTH_MAX: usize = 32768;

    /// Creates a new [`MessageBuilder`] to construct an instance of a [`Message`].
    #[inline(always)]
    pub fn builder() -> MessageBuilder {
        MessageBuilder::new()
    }

    /// Computes the identifier of the message.
    #[inline(always)]
    pub fn id(&self) -> MessageId {
        MessageId::new(Blake2b256::digest(&self.pack_to_vec()[1..]).into())
    }

    /// Returns the protocol id of a [`Message`].
    #[inline(always)]
    pub fn protocol_id(&self) -> ProtocolId {
        self.protocol_id
    }

    /// Returns the parents of a [`Message`].
    #[inline(always)]
    pub fn parents(&self) -> &Parents {
        &self.parents
    }

    /// Returns the optional payload of a [`Message`].
    #[inline(always)]
    pub fn payload(&self) -> Option<&Payload> {
        self.payload.as_ref()
    }

    /// Returns the nonce of a [`Message`].
    #[inline(always)]
    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Consumes the [`Message`], and returns ownership over its [`Parents`].
    #[inline(always)]
    pub fn into_parents(self) -> Parents {
        self.parents
    }
}

impl Packable for Message {
    type UnpackError = Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        self.protocol_id.pack(packer)?;
        self.parents.pack(packer)?;
        self.payload.pack(packer)?;
        self.nonce.pack(packer)?;
        Ok(())
    }

    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, UnpackError<Self::UnpackError, U::Error>> {
        let protocol_id = ProtocolId::unpack::<_, VERIFY>(unpacker)
            .map_err(|e| UnpackError::Packable(Error::MissingField("protocol_id")))?;

        let parents = Parents::unpack::<_, VERIFY>(unpacker)?;

        let payload = OptionalPayload::unpack::<_, VERIFY>(unpacker)?;

        if VERIFY
            && !matches!(
                *payload,
                None | Some(Payload::Transaction(_))
                    | Some(Payload::Milestone(_))
                    | Some(Payload::TaggedData(_))
                    | Some(Payload::Indexation(_))
                    | Some(Payload::ChrysalisTransaction(_))
            )
        {
            // Safe to unwrap since it's known not to be None.
            return Err(UnpackError::Packable(Error::InvalidPayloadKind(
                Into::<Option<Payload>>::into(payload).unwrap().kind(),
            )));
        }

        let nonce = u64::unpack::<_, VERIFY>(unpacker).infallible()?;

        let message = Self {
            protocol_id,
            parents,
            payload,
            nonce,
        };

        let message_len = message.packed_len() - 1;

        // FIXME: compute this in a more efficient way.
        if VERIFY && message_len > Message::LENGTH_MAX {
            return Err(UnpackError::Packable(Error::InvalidMessageLength(message_len)));
        }

        // When parsing the message is complete, there should not be any trailing bytes left that were not parsed.
        if VERIFY && u8::unpack::<_, VERIFY>(unpacker).is_ok() {
            return Err(UnpackError::Packable(Error::RemainingBytesAfterMessage));
        }

        Ok(message)
    }
}

#[repr(transparent)]
pub struct Chrysalis(Message);
#[repr(transparent)]
pub struct Stardust(Message);

impl Into<Message> for Chrysalis {
    fn into(self) -> Message {
        self.0
    }
}

impl Into<Message> for Stardust {
    fn into(self) -> Message {
        self.0
    }
}

impl TryFrom<Message> for Chrysalis {
    type Error = anyhow::Error;

    fn try_from(msg: Message) -> Result<Self, Self::Error> {
        if let ProtocolId::NetworkId(_) = msg.protocol_id() {
            Ok(Self(msg))
        } else {
            bail!("Invalid protocol id")
        }
    }
}

impl TryFrom<Message> for Stardust {
    type Error = anyhow::Error;

    fn try_from(msg: Message) -> Result<Self, Self::Error> {
        if let ProtocolId::ProtocolVersion(_) = msg.protocol_id() {
            Ok(Self(msg))
        } else {
            bail!("Invalid protocol id")
        }
    }
}

impl Packable for Chrysalis {
    type UnpackError = Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        if let ProtocolId::NetworkId(network_id) = self.0.protocol_id() {
            network_id.pack(packer)?
        } else {
            unreachable!("return error as this is misuse")
        }
        self.0.parents.pack(packer)?;
        self.0.payload.pack(packer)?;
        self.0.nonce.pack(packer)?;
        Ok(())
    }

    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, UnpackError<Self::UnpackError, U::Error>> {
        let protocol_id = ProtocolId::NetworkId(
            u64::unpack::<_, VERIFY>(unpacker)
                .map_err(|e| UnpackError::Packable(Error::MissingField("protocol_id")))?,
        );
        let parents = Parents::unpack::<_, VERIFY>(unpacker)?;

        let payload = OptionalPayload::unpack::<_, VERIFY>(unpacker)?;

        if VERIFY
            && !matches!(
                *payload,
                None | Some(Payload::ChrysalisTransaction(_))
                    | Some(Payload::Milestone(_))
                    | Some(Payload::Indexation(_))
            )
        {
            // Safe to unwrap since it's known not to be None.
            return Err(UnpackError::Packable(Error::InvalidPayloadKind(
                Into::<Option<Payload>>::into(payload).unwrap().kind(),
            )));
        }

        let nonce = u64::unpack::<_, VERIFY>(unpacker).infallible()?;

        let message = Message {
            protocol_id,
            parents,
            payload,
            nonce,
        };

        let message_len = message.packed_len();

        // FIXME: compute this in a more efficient way.
        if VERIFY && message_len > Message::LENGTH_MAX {
            return Err(UnpackError::Packable(Error::InvalidMessageLength(message_len)));
        }

        // When parsing the message is complete, there should not be any trailing bytes left that were not parsed.
        if VERIFY && u8::unpack::<_, VERIFY>(unpacker).is_ok() {
            return Err(UnpackError::Packable(Error::RemainingBytesAfterMessage));
        }

        Ok(Self(message))
    }
}

impl Packable for Stardust {
    type UnpackError = Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        if let ProtocolId::ProtocolVersion(protocol_version) = self.0.protocol_id() {
            protocol_version.pack(packer)?
        } else {
            unreachable!("return error as this is misuse")
        }
        self.0.parents.pack(packer)?;
        self.0.payload.pack(packer)?;
        self.0.nonce.pack(packer)?;
        Ok(())
    }

    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, UnpackError<Self::UnpackError, U::Error>> {
        let protocol_id = ProtocolId::ProtocolVersion(
            u8::unpack::<_, VERIFY>(unpacker).map_err(|e| UnpackError::Packable(Error::MissingField("protocol_id")))?,
        );

        let parents = Parents::unpack::<_, VERIFY>(unpacker)?;

        let payload = OptionalPayload::unpack::<_, VERIFY>(unpacker)?;
        if VERIFY
            && !matches!(
                *payload,
                None | Some(Payload::Transaction(_)) | Some(Payload::Milestone(_)) | Some(Payload::TaggedData(_))
            )
        {
            // Safe to unwrap since it's known not to be None.
            return Err(UnpackError::Packable(Error::InvalidPayloadKind(
                Into::<Option<Payload>>::into(payload).unwrap().kind(),
            )));
        }

        let nonce = u64::unpack::<_, VERIFY>(unpacker).infallible()?;

        let message = Message {
            protocol_id,
            parents,
            payload,
            nonce,
        };

        let message_len = message.packed_len();

        // FIXME: compute this in a more efficient way.
        if VERIFY && message_len > Message::LENGTH_MAX {
            return Err(UnpackError::Packable(Error::InvalidMessageLength(message_len)));
        }

        // When parsing the message is complete, there should not be any trailing bytes left that were not parsed.
        if VERIFY && u8::unpack::<_, VERIFY>(unpacker).is_ok() {
            return Err(UnpackError::Packable(Error::RemainingBytesAfterMessage));
        }

        Ok(Self(message))
    }
}

impl TryFrom<bee_message_old::Message> for Message {
    type Error = anyhow::Error;
    fn try_from(msg: bee_message_old::Message) -> Result<Self, Self::Error> {
        let mut message_builder = Self::builder();
        let protocol_id = ProtocolId::NetworkId(msg.network_id());
        let nonce = msg.nonce();
        message_builder = message_builder
            .with_protocol_id(protocol_id)
            .with_nonce_provider(nonce, MessageBuilder::<u64>::DEFAULT_POW_SCORE);
        let mut parents: Vec<MessageId> = Vec::new();
        for parent_id in msg.parents().iter() {
            parents.push(MessageId::new(parent_id.as_ref().try_into().unwrap()));
        }
        let new_parents = bee_message::parent::Parents::new(parents)?;
        message_builder = message_builder.with_parents(new_parents);
        if let Some(p) = msg.payload() {
            match p {
                bee_message_old::payload::Payload::Milestone(ms) => {
                    let old_essence = ms.essence();
                    let old_index = old_essence.index();
                    let old_timestamp = old_essence.timestamp();
                    let mut parents: Vec<MessageId> = Vec::new();
                    for parent_id in old_essence.parents().iter() {
                        parents.push(MessageId::new(parent_id.as_ref().try_into().unwrap()));
                    }
                    let new_parents = bee_message::parent::Parents::new(parents)?;
                    let merkle_proof = old_essence.merkle_proof();
                    let next_pow_score = old_essence.next_pow_score();
                    let next_pow_score_milestone_index = old_essence.next_pow_score_milestone_index();
                    let public_keys = old_essence.public_keys().clone();
                    let mut receipt = None;
                    if let Some(bee_message_old::payload::Payload::Receipt(old_receipt)) = old_essence.receipt() {
                        let migrated_at = old_receipt.migrated_at().0.into();
                        let last = old_receipt.last();
                        let mut funds: Vec<bee_message::payload::receipt::MigratedFundsEntry> = Vec::new();
                        for entry in old_receipt.funds().iter() {
                            let addr = match entry.output().address() {
                                bee_message_old::address::Address::Ed25519(a) => {
                                    bee_message::address::Address::Ed25519(Ed25519Address::new(a.as_ref().try_into()?))
                                }
                            };
                            funds.push(bee_message::payload::receipt::MigratedFundsEntry::new(
                                bee_message::payload::receipt::TailTransactionHash::new(
                                    entry.tail_transaction_hash().as_ref().try_into()?,
                                )?,
                                addr,
                                entry.output().amount(),
                            )?);
                        }
                        if let bee_message_old::payload::Payload::TreasuryTransaction(t) = old_receipt.transaction() {
                            if let (
                                bee_message_old::input::Input::Treasury(i),
                                bee_message_old::output::Output::Treasury(o),
                            ) = (t.input(), t.output())
                            {
                                let new_input =
                                    bee_message::input::Input::Treasury(bee_message::input::TreasuryInput::new(
                                        bee_message::payload::milestone::MilestoneId::new(
                                            i.milestone_id().as_ref().try_into()?,
                                        ),
                                    ));
                                let new_output = bee_message::output::Output::Treasury(
                                    bee_message::output::TreasuryOutput::new(o.amount())?,
                                );
                                let new_treasury_tx =
                                    bee_message::payload::TreasuryTransactionPayload::new(new_input, new_output)?;
                                let new_receipt = bee_message::payload::ReceiptPayload::new(
                                    migrated_at,
                                    last,
                                    funds,
                                    bee_message::payload::Payload::TreasuryTransaction(Box::new(new_treasury_tx)),
                                )?;
                                receipt.replace(bee_message::payload::Payload::Receipt(Box::new(new_receipt)));
                                let essence = bee_message::payload::milestone::MilestoneEssence::new(
                                    bee_message::milestone::MilestoneIndex(*old_index),
                                    old_timestamp,
                                    new_parents,
                                    merkle_proof.try_into()?,
                                    next_pow_score,
                                    next_pow_score_milestone_index,
                                    public_keys,
                                    receipt,
                                )?;
                                let signatures = ms
                                    .signatures()
                                    .into_iter()
                                    .map(|b| <[u8; 64]>::try_from(&**b))
                                    .collect::<Result<Vec<_>, _>>()?;
                                let inner_p = MilestonePayload::new(essence, signatures)?;
                                message_builder = message_builder.with_payload(Payload::Milestone(Box::new(inner_p)));
                            } else {
                                bail!("Failed to get treasury input/output from treasury transaction")
                            }
                        } else {
                            bail!("Failed to get treasury transaction from receipt payload")
                        }
                    }
                }
                bee_message_old::payload::Payload::Indexation(indexation) => {
                    let index = indexation.index();
                    let data = indexation.data();
                    let indexation_payload = bee_message::payload::IndexationPayload::new(index.into(), data.into())?;
                    message_builder = message_builder.with_payload(Payload::Indexation(Box::new(indexation_payload)));
                }
                bee_message_old::payload::Payload::Transaction(tx_payload) => {
                    let bee_message_old::payload::transaction::Essence::Regular(r) = tx_payload.essence();
                    let inputs_commitment: [u8; 32] = [0; 32];
                    let mut essence_builder = bee_message::payload::transaction::RegularTransactionEssenceBuilder::new(
                        msg.network_id(),
                        inputs_commitment,
                    );
                    let mut inputs = Vec::new();
                    for input in r.inputs() {
                        match input {
                            bee_message_old::input::Input::Utxo(utxo) => {
                                let utxo_input_tx_id =
                                    TransactionId::new(utxo.output_id().transaction_id().as_ref().try_into()?);
                                let utxo =
                                    bee_message::input::UtxoInput::new(utxo_input_tx_id, utxo.output_id().index())?;
                                let input = bee_message::input::Input::Utxo(utxo);
                                inputs.push(input);
                            }
                            bee_message_old::input::Input::Treasury(treasury) => {
                                let milestone_id = bee_message::payload::milestone::MilestoneId::new(
                                    treasury.milestone_id().as_ref().try_into()?,
                                );
                                let treasury = bee_message::input::TreasuryInput::new(milestone_id);
                                let input = bee_message::input::Input::Treasury(treasury);
                                inputs.push(input);
                            }
                        }
                    }
                    essence_builder = essence_builder.with_inputs(inputs);
                    let mut outputs = Vec::new();
                    for output in r.outputs() {
                        match output {
                            bee_message_old::output::Output::SignatureLockedSingle(o) => {
                                let bee_message_old::prelude::Address::Ed25519(ed) = o.address();
                                let amount = o.amount();
                                let address = Address::Ed25519(Ed25519Address::new(ed.as_ref().try_into()?));
                                let o = bee_message::output::SignatureLockedSingleOutput::new(address, amount)?;
                                let output = bee_message::output::Output::SignatureLockedSingle(o);
                                outputs.push(output);
                            }
                            bee_message_old::output::Output::SignatureLockedDustAllowance(o) => {
                                let bee_message_old::prelude::Address::Ed25519(ed) = o.address();
                                let amount = o.amount();
                                let address = Address::Ed25519(Ed25519Address::new(ed.as_ref().try_into()?));
                                let o = bee_message::output::SignatureLockedDustAllowanceOutput::new(address, amount)?;
                                let output = bee_message::output::Output::SignatureLockedDustAllowance(o);
                                outputs.push(output);
                            }
                            bee_message_old::output::Output::Treasury(o) => {
                                let amount = o.amount();
                                let treasury = TreasuryOutput::new(amount)?;
                                let output = bee_message::output::Output::Treasury(treasury);
                                outputs.push(output);
                            }
                        }
                    }
                    let essence = bee_message::payload::transaction::TransactionEssence::Regular(
                        essence_builder.with_outputs(outputs).finish()?,
                    );
                    let mut unlock_blocks = Vec::new();
                    for unlock_block in tx_payload.unlock_blocks().into_iter() {
                        match unlock_block {
                            bee_message_old::unlock::UnlockBlock::Signature(
                                bee_message_old::signature::SignatureUnlock::Ed25519(s),
                            ) => {
                                let ed = bee_message::signature::Ed25519Signature::new(
                                    s.public_key().as_ref().try_into()?,
                                    s.signature().as_ref().try_into()?,
                                );
                                let sig = bee_message::signature::Signature::Ed25519(ed);
                                let unlock_block = bee_message::unlock_block::SignatureUnlockBlock::new(sig);
                                unlock_blocks.push(bee_message::unlock_block::UnlockBlock::Signature(unlock_block));
                            }
                            bee_message_old::unlock::UnlockBlock::Reference(r) => {
                                let unlock_block = bee_message::unlock_block::ReferenceUnlockBlock::new(r.index())?;
                                unlock_blocks.push(bee_message::unlock_block::UnlockBlock::Reference(unlock_block));
                            }
                        }
                    }
                    let unlock_blocks = bee_message::unlock_block::UnlockBlocks::new(unlock_blocks)?;
                    let tx_payload = bee_message::payload::TransactionPayload::new(essence, unlock_blocks)?;
                    message_builder = message_builder.with_payload(Payload::Transaction(Box::new(tx_payload)));
                }
                e => bail!("Invalid payload kind: {}", e.kind()),
            }
        };
        Ok(message_builder.finish()?)
    }
}

impl TryFrom<&ChrysalisMessageDto> for Message {
    type Error = bee_rest_api::types::error::Error;

    fn try_from(value: &ChrysalisMessageDto) -> Result<Self, Self::Error> {
        let mut builder = MessageBuilder::new()
            .with_protocol_id(ProtocolId::NetworkId(
                value
                    .network_id
                    .parse()
                    .map_err(|_| bee_rest_api::types::error::Error::InvalidField("networkId"))?,
            ))
            .with_parents(Parents::new(
                value
                    .parents
                    .iter()
                    .map(|m| {
                        m.parse::<MessageId>()
                            .map_err(|_| bee_rest_api::types::error::Error::InvalidField("parentMessageIds"))
                    })
                    .collect::<Result<Vec<MessageId>, _>>()?,
            )?)
            .with_nonce_provider(
                value
                    .nonce
                    .parse::<u64>()
                    .map_err(|_| bee_rest_api::types::error::Error::InvalidField("nonce"))?,
                0f64,
            );
        if let Some(p) = value.payload.as_ref() {
            builder = builder.with_payload(p.try_into()?);
        }
        Ok(builder.finish()?)
    }
}

/// Chronicle Message record
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
