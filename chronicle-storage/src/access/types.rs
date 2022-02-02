// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bee_common::packable::Packable;
use bee_message::{
    milestone::MilestoneIndex,
    Message,
};
use chronicle_common::Wrapper;
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
        HashSet,
    },
    io::Cursor,
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
impl TokenEncoder for Hint {
    fn encode_token(&self) -> TokenEncodeChain {
        self.hint.encode_token()
    }
}
macro_rules! impl_simple_packable {
    ($t:ty) => {
        impl ColumnDecoder for Bee<$t> {
            fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
                <$t>::unpack(&mut Cursor::new(slice))
                    .map_err(|e| anyhow!("{:?}", e))
                    .map(Into::into)
            }
        }

        impl ColumnEncoder for Bee<$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
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

impl_simple_packable!(Message);
impl_string_packable!(MessageId);
impl_string_packable!(OutputId);
impl_string_packable!(TransactionId);
impl_string_packable!(Ed25519Address);

/// A transaction's unlock data, to be stored in a `transactions` row.
/// Holds a reference to the input which it signs.
#[derive(Debug, Clone)]
pub struct UnlockData {
    /// it holds the transaction_id of the input which created the unlock_block
    pub input_tx_id: TransactionId,
    /// it holds the input_index of the input which created the unlock_block
    pub input_index: u16,
    /// it's the unlock_block
    pub unlock_block: UnlockBlock,
}
impl UnlockData {
    /// Creates a new unlock data
    pub fn new(input_tx_id: TransactionId, input_index: u16, unlock_block: UnlockBlock) -> Self {
        Self {
            input_tx_id,
            input_index,
            unlock_block,
        }
    }
}
impl Packable for UnlockData {
    type Error = anyhow::Error;
    fn packed_len(&self) -> usize {
        self.input_tx_id.packed_len() + self.input_index.packed_len() + self.unlock_block.packed_len()
    }
    fn pack<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        self.input_tx_id.pack(writer)?;
        self.input_index.pack(writer)?;
        self.unlock_block.pack(writer)?;
        Ok(())
    }
    fn unpack_inner<R: std::io::Read + ?Sized, const CHECK: bool>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self {
            input_tx_id: TransactionId::unpack(reader)?,
            input_index: u16::unpack(reader)?,
            unlock_block: UnlockBlock::unpack(reader)?,
        })
    }
}

/// A transaction's input data, to be stored in a `transactions` row.
#[derive(Debug, Clone)]
pub enum InputData {
    /// An regular Input which spends a prior Output and its unlock block
    Utxo(UtxoInput, UnlockBlock),
    /// A special input for migrating funds from another network
    Treasury(TreasuryInput),
}

impl InputData {
    /// Creates a regular Input Data
    pub fn utxo(utxo_input: UtxoInput, unlock_block: UnlockBlock) -> Self {
        Self::Utxo(utxo_input, unlock_block)
    }
    /// Creates a special migration Input Data
    pub fn treasury(treasury_input: TreasuryInput) -> Self {
        Self::Treasury(treasury_input)
    }
}

impl Packable for InputData {
    type Error = anyhow::Error;
    fn packed_len(&self) -> usize {
        match self {
            InputData::Utxo(utxo_input, unlock_block) => {
                0u8.packed_len() + utxo_input.packed_len() + unlock_block.packed_len()
            }
            InputData::Treasury(treasury_input) => 0u8.packed_len() + treasury_input.packed_len(),
        }
    }
    fn pack<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        match self {
            InputData::Utxo(utxo_input, unlock_block) => {
                0u8.pack(writer)?;
                utxo_input.pack(writer)?;
                unlock_block.pack(writer)?;
            }
            InputData::Treasury(treasury_input) => {
                1u8.pack(writer)?;
                treasury_input.pack(writer)?;
            }
        }
        Ok(())
    }
    fn unpack_inner<R: std::io::Read + ?Sized, const CHECK: bool>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(match u8::unpack(reader)? {
            0 => InputData::Utxo(UtxoInput::unpack(reader)?, UnlockBlock::unpack(reader)?),
            1 => InputData::Treasury(TreasuryInput::unpack(reader)?),
            _ => bail!("Tried to unpack an invalid inputdata variant!"),
        })
    }
}

// input unlocked my input
#[derive(Debug, Clone)]
/// Chrysalis transaction data
pub enum TransactionData {
    /// An unspent transaction input
    Input(InputData),
    /// A transaction output
    Output(Output),
    /// A signed block which can be used to unlock an input
    Unlock(UnlockData),
}

impl Packable for TransactionData {
    type Error = anyhow::Error;

    fn packed_len(&self) -> usize {
        match self {
            TransactionData::Input(utxo_input) => 0u8.packed_len() + utxo_input.packed_len(),
            TransactionData::Output(output) => 0u8.packed_len() + output.packed_len(),
            TransactionData::Unlock(block) => 0u8.packed_len() + block.packed_len(),
        }
    }

    fn pack<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        match self {
            TransactionData::Input(input_data) => {
                0u8.pack(writer)?;
                input_data.pack(writer)?;
            }
            TransactionData::Output(output) => {
                1u8.pack(writer)?;
                output.pack(writer)?;
            }
            TransactionData::Unlock(block_data) => {
                2u8.pack(writer)?;
                block_data.pack(writer)?;
            }
        }
        Ok(())
    }

    fn unpack_inner<R: std::io::Read + ?Sized, const CHECK: bool>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(match u8::unpack(reader)? {
            0 => TransactionData::Input(InputData::unpack(reader)?),
            1 => TransactionData::Output(Output::unpack(reader)?),
            2 => TransactionData::Unlock(UnlockData::unpack(reader)?),
            _ => bail!("Tried to unpack an invalid transaction variant!"),
        })
    }
}

impl ColumnDecoder for TransactionData {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Self::unpack(&mut Cursor::new(slice)).map(Into::into)
    }
}
/// MessageMetadata storage object
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageMetadata {
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

impl ColumnEncoder for LedgerInclusionState {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let bytes = bincode_config().serialize(self).unwrap();
        buffer.extend(&i32::to_be_bytes(bytes.len() as i32));
        buffer.extend(bytes)
    }
}

impl ColumnDecoder for LedgerInclusionState {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        bincode_config().deserialize(slice).map_err(Into::into)
    }
}

impl ColumnEncoder for TransactionData {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let mut bytes = Vec::new();
        self.pack(&mut bytes).expect("Unable to pack TransactionData");
        buffer.extend(&i32::to_be_bytes(bytes.len() as i32));
        buffer.extend(bytes)
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

/// A result struct which holds a retrieved transaction
#[derive(Debug, Clone)]
pub struct TransactionRes {
    /// The transaction's message id
    pub message_id: MessageId,
    /// The transaction's milestone index
    pub milestone_index: Option<MilestoneIndex>,
    /// The output
    pub outputs: Vec<(Output, Option<UnlockRes>)>,
    /// The inputs, if any exist
    pub inputs: Vec<InputData>,
}

/// A result struct which holds an unlock row from the `transactions` table
#[derive(Debug, Clone)]
pub struct UnlockRes {
    /// The message ID for the transaction which this unlocks
    pub message_id: MessageId,
    /// The unlock block
    pub block: UnlockBlock,
    /// This transaction's ledger inclusion state
    pub inclusion_state: Option<LedgerInclusionState>,
}

/// A "full" message payload, including both message and metadata
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FullMessage(pub Message, pub MessageMetadata);

impl FullMessage {
    /// Create a new full message
    pub fn new(message: Message, metadata: MessageMetadata) -> Self {
        Self(message, metadata)
    }
    /// Get the message ID
    pub fn message_id(&self) -> &MessageId {
        &self.1.message_id
    }
    /// Get the message's metadata
    pub fn metadata(&self) -> &MessageMetadata {
        &self.1
    }
    /// Get the message
    pub fn message(&self) -> &Message {
        &self.0
    }
    /// Get the milestone index that references this
    pub fn ref_ms(&self) -> Option<u32> {
        self.1.referenced_by_milestone_index
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageRecord {
    pub message_id: MessageId,
    pub message: Message,
    #[serde(skip)]
    pub version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub milestone_index: Option<MilestoneIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusion_state: Option<LedgerInclusionState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_reason: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof: Option<Vec<u8>>,
}

impl MessageRecord {
    /// Create a new full message
    pub fn new(message_id: MessageId, message: Message, version: u8) -> Self {
        Self {
            message_id,
            message,
            version,
            milestone_index: Default::default(),
            inclusion_state: Default::default(),
            conflict_reason: Default::default(),
            proof: Default::default(),
        }
    }
    pub fn with_milestone_index(mut self, milestone_index: MilestoneIndex) -> Self {
        self.milestone_index = Some(milestone_index);
        self
    }
    pub fn with_inclusion_state(mut self, inclusion_state: LedgerInclusionState) -> Self {
        self.inclusion_state = Some(inclusion_state);
        self
    }
    pub fn with_conflict_reason(mut self, conflict_reason: u8) -> Self {
        self.conflict_reason = Some(conflict_reason);
        self
    }
    pub fn with_proof(mut self, proof: Vec<u8>) -> Self {
        self.proof = Some(proof);
        self
    }
    /// Get the message ID
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }
    /// Get the message
    pub fn message(&self) -> &Message {
        &self.message
    }
    /// Get the message's version
    pub fn version(&self) -> u8 {
        self.version
    }
    /// Get the message's milestone index
    pub fn milestone_index(&self) -> Option<MilestoneIndex> {
        self.milestone_index
    }
    /// Get the message's inclusion state
    pub fn inclusion_state(&self) -> Option<LedgerInclusionState> {
        self.inclusion_state
    }
    /// Get the message's conflict reason
    pub fn conflict_reason(&self) -> Option<u8> {
        self.conflict_reason
    }
    /// Get the message's proof
    pub fn proof(&self) -> Option<Vec<u8>> {
        self.proof
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
            version: rows.column_value()?,
            milestone_index: rows
                .column_value::<Option<Bee<MilestoneIndex>>>()?
                .map(|i| i.into_inner()),
            inclusion_state: rows.column_value()?,
            conflict_reason: rows.column_value()?,
            proof: rows.column_value()?,
        })
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

/// A type alias for partition ids
pub type MsRangeId = u32;

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

/// A hint, used to lookup in the `hints` table
#[derive(Clone, Debug)]
pub struct Hint {
    /// The hint string
    pub hint: String,
    /// The hint variant. Can be 'parent', 'address', or 'index'.
    pub variant: HintVariant,
}

impl Hint {
    /// Creates a new index hint
    pub fn index(index: String) -> Self {
        Self {
            hint: index,
            variant: HintVariant::Index,
        }
    }

    /// Creates a new address hint
    pub fn address(address: String) -> Self {
        Self {
            hint: address,
            variant: HintVariant::Address,
        }
    }

    /// Creates a new parent hint
    pub fn parent(parent: String) -> Self {
        Self {
            hint: parent,
            variant: HintVariant::Parent,
        }
    }
}

/// Hint variants
#[derive(Clone, Debug)]
pub enum HintVariant {
    /// An address
    Address,
    /// An unhashed index
    Index,
    /// A parent message id
    Parent,
}

impl std::fmt::Display for HintVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                HintVariant::Address => "address",
                HintVariant::Index => "index",
                HintVariant::Parent => "parent",
            }
        )
    }
}

impl ColumnEncoder for HintVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
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

impl<T: Row> RowsDecoder for Paged<Iter<T>> {
    type Row = T;
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Paged<Iter<T>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        Ok(Some(Paged::new(iter, paging_state)))
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
#[derive(Clone, Debug)]
/// MilestoneData analytics information.
pub struct AnalyticRecord {
    /// Duh it's the milestone index
    pub milestone_index: MilestoneIndex,
    /// The total number of messages within a milestone cone
    pub message_count: MessageCount,
    /// The total number of transactions within a milestone cone
    pub transaction_count: TransactionCount,
    /// Transferred IOTA tokens volume within a milestone cone
    pub transferred_tokens: TransferredTokens,
}

impl AnalyticRecord {
    /// Create new MilestoneDataInfo object
    pub fn new(
        milestone_index: MilestoneIndex,
        message_count: MessageCount,
        transaction_count: TransactionCount,
        transferred_tokens: TransferredTokens,
    ) -> Self {
        Self {
            milestone_index,
            message_count,
            transaction_count,
            transferred_tokens,
        }
    }
    /// Gets the milestone index
    pub fn milestone_index(&self) -> &MilestoneIndex {
        &self.milestone_index
    }
    /// Gets the message_count
    pub fn message_count(&self) -> &MessageCount {
        &self.message_count
    }
    /// Gets the transaction count
    pub fn transaction_count(&self) -> &TransactionCount {
        &self.transaction_count
    }
    /// Gets the transferred tokens
    pub fn transferred_tokens(&self) -> &TransferredTokens {
        &self.transferred_tokens
    }
}

#[derive(Deserialize, Serialize)]
/// Defines a message to/from the Broker or its children
pub enum BrokerSocketMsg<T> {
    /// A message to/from the Broker
    ChronicleBroker(T),
}

/// Milestone data
#[derive(Debug, Deserialize, Serialize)]
pub struct MilestoneData {
    pub(crate) message_id: MessageId,
    pub(crate) milestone_index: u32,
    pub(crate) payload: MilestonePayload,
    pub(crate) messages: BTreeSet<MessageRecord>,
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
    pub fn get_analytic_record(&self) -> anyhow::Result<AnalyticRecord> {
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
                    let Essence::Regular(regular_essence) = payload.essence();
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
        let analytic_record = AnalyticRecord::new(
            MilestoneIndex(self.milestone_index()),
            MessageCount(self.messages().len() as u32),
            TransactionCount(transaction_count),
            TransferredTokens(transferred_tokens),
        );
        // Return the analytic record
        Ok(analytic_record)
    }
}

/// Milestone data builder
#[derive(Debug)]
pub struct MilestoneDataBuilder {
    pub(crate) message_id: MessageId,
    pub(crate) milestone_index: u32,
    pub(crate) payload: Option<MilestonePayload>,
    pub(crate) messages: BTreeMap<MessageId, MessageRecord>,
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
            pending: HashSet::new(),
            created_by,
        }
    }
    pub fn with_payload(mut self, payload: MilestonePayload) -> Self {
        self.payload = Some(payload);
        self
    }
    pub fn add_message(&mut self, message: MessageRecord) -> anyhow::Result<()> {
        ensure!(
            self.messages.insert(*message.message_id(), message).is_none(),
            "Message already exists"
        );
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
    /// Get the milestone's messages
    pub fn messages_mut(&mut self) -> &mut BTreeMap<MessageId, MessageRecord> {
        &mut self.messages
    }
    /// Get the pending messages
    pub fn pending_mut(&mut self) -> &mut HashSet<MessageId> {
        &mut self.pending
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
#[derive(Debug, Deserialize, Serialize)]
pub struct OldMilestoneData {
    pub(crate) milestone_index: u32,
    pub(crate) milestone: Option<Box<MilestonePayload>>,
    pub(crate) messages: BTreeMap<MessageId, FullMessage>,
    #[serde(skip)]
    pub(crate) pending: HashSet<MessageId>,
    #[serde(skip)]
    pub(crate) created_by: CreatedBy,
}

impl OldMilestoneData {
    /// Create new milestone data
    pub fn new(milestone_index: u32, created_by: CreatedBy) -> Self {
        Self {
            milestone_index,
            milestone: None,
            messages: BTreeMap::new(),
            pending: HashSet::new(),
            created_by,
        }
    }
    /// Get the milestone index from this milestone data
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
    pub fn milestone(&self) -> Option<&MilestonePayload> {
        self.milestone.as_ref().map(|m| &**m)
    }
    /// Get the analytics from the collected messages
    pub fn get_analytic_record(&self) -> anyhow::Result<AnalyticRecord> {
        if !self.check_if_completed() {
            anyhow::bail!("cannot get analytics for uncompleted milestone data")
        }
        // The accumulators
        let mut transaction_count: u32 = 0;
        let mut message_count: u32 = 0;
        let mut transferred_tokens: u64 = 0;

        // Iterate the messages to calculate analytics
        for (_, FullMessage(message, metadata)) in &self.messages {
            // Accumulate the message count
            message_count += 1;
            // Accumulate confirmed(included) transaction value
            if let Some(LedgerInclusionState::Included) = metadata.ledger_inclusion_state {
                if let Some(Payload::Transaction(payload)) = message.payload() {
                    // Accumulate the transaction count
                    transaction_count += 1;
                    let Essence::Regular(regular_essence) = payload.essence();
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
        let milestone_index = self.milestone_index();
        let analytic_record = AnalyticRecord::new(
            MilestoneIndex(milestone_index),
            MessageCount(message_count),
            TransactionCount(transaction_count),
            TransferredTokens(transferred_tokens),
        );
        // Return the analytic record
        Ok(analytic_record)
    }
    pub fn set_milestone(&mut self, boxed_milestone_payload: Box<MilestonePayload>) {
        self.milestone.replace(boxed_milestone_payload);
    }
    /// Check if the milestone exists
    pub fn milestone_exist(&self) -> bool {
        self.milestone.is_some()
    }
    pub fn add_full_message(&mut self, full_message: FullMessage) {
        self.messages.insert(*full_message.message_id(), full_message);
    }
    pub fn add_pending(&mut self, parent_id: MessageId) {
        self.pending.insert(parent_id);
    }
    pub fn remove_from_pending(&mut self, message_id: &MessageId) {
        self.pending.remove(message_id);
    }
    /// Get the milestone's messages
    pub fn messages(&self) -> &BTreeMap<MessageId, FullMessage> {
        &self.messages
    }
    /// Get the pending messages
    pub fn pending(&self) -> &HashSet<MessageId> {
        &self.pending
    }
    /// Get the source this was created by
    pub fn created_by(&self) -> &CreatedBy {
        &self.created_by
    }
    /// Set created by
    pub fn set_created_by(&mut self, created_by: CreatedBy) {
        self.created_by = created_by
    }
    /// Check if the milestone data is completed
    pub fn check_if_completed(&self) -> bool {
        // Check if there are no pending at all to set complete to true
        let no_pending_left = self.pending().is_empty();
        let milestone_exist = self.milestone_exist();
        if no_pending_left && milestone_exist {
            // milestone data is complete now
            return true;
        }
        false
    }
}

impl std::iter::IntoIterator for OldMilestoneData {
    type Item = (MessageId, FullMessage);
    type IntoIter = std::collections::btree_map::IntoIter<MessageId, FullMessage>;
    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
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

#[cfg(feature = "sync")]
pub use sync::*;
#[cfg(feature = "sync")]
mod sync {
    use super::*;
    use chronicle_common::SyncRange;
    use scylla_rs::prelude::{
        Consistency,
        GetStaticSelectRequest,
        Iter,
        Select,
    };
    use std::ops::Range;

    /// Representation of the database sync data
    #[derive(Debug, Clone, Default, Serialize)]
    pub struct SyncData {
        /// The completed(synced and logged) milestones data
        pub completed: Vec<Range<u32>>,
        /// Synced milestones data but unlogged
        pub synced_but_unlogged: Vec<Range<u32>>,
        /// Gaps/missings milestones data
        pub gaps: Vec<Range<u32>>,
    }

    impl SyncData {
        /// Try to fetch the sync data from the sync table for the provided keyspace and sync range
        pub async fn try_fetch<S: 'static + Select<String, SyncRange, Iter<SyncRecord>>>(
            keyspace: &S,
            sync_range: &SyncRange,
            retries: usize,
        ) -> anyhow::Result<SyncData> {
            let res = keyspace
                .select(&"permanode".to_string(), &sync_range.clone().into())
                .consistency(Consistency::One)
                .build()?
                .worker()
                .with_retries(retries)
                .get_local()
                .await?;
            let mut sync_data = SyncData::default();
            if let Some(mut sync_rows) = res {
                // Get the first row, note: the first row is always with the largest milestone_index
                let SyncRecord {
                    milestone_index,
                    logged_by,
                    ..
                } = sync_rows.next().unwrap();
                // push missing row/gap (if any)
                sync_data.process_gaps(sync_range.to, *milestone_index);
                sync_data.process_rest(&logged_by, *milestone_index, &None);
                let mut pre_ms = milestone_index;
                let mut pre_lb = logged_by;
                // Generate and identify missing gaps in order to fill them
                while let Some(SyncRecord {
                    milestone_index,
                    logged_by,
                    ..
                }) = sync_rows.next()
                {
                    // check if there are any missings
                    sync_data.process_gaps(*pre_ms, *milestone_index);
                    sync_data.process_rest(&logged_by, *milestone_index, &pre_lb);
                    pre_ms = milestone_index;
                    pre_lb = logged_by;
                }
                // pre_ms is the most recent milestone we processed
                // it's also the lowest milestone index in the select response
                // so anything < pre_ms && anything >= (self.sync_range.from - 1)
                // (lower provided sync bound) are missing
                // push missing row/gap (if any)
                sync_data.process_gaps(*pre_ms, sync_range.from - 1);
                Ok(sync_data)
            } else {
                // Everything is missing as gaps
                sync_data.process_gaps(sync_range.to, sync_range.from - 1);
                Ok(sync_data)
            }
        }
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
        fn get_lowest_gap_or_unlogged(&self) -> Option<&Range<u32>> {
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
        fn process_rest(&mut self, logged_by: &Option<u8>, milestone_index: u32, pre_lb: &Option<u8>) {
            if logged_by.is_some() {
                // process logged
                Self::proceed(&mut self.completed, milestone_index, pre_lb.is_some());
            } else {
                // process_unlogged
                let unlogged = &mut self.synced_but_unlogged;
                Self::proceed(unlogged, milestone_index, pre_lb.is_none());
            }
        }
        fn process_gaps(&mut self, pre_ms: u32, milestone_index: u32) {
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
        fn proceed(ranges: &mut Vec<Range<u32>>, milestone_index: u32, check: bool) {
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
}

#[cfg(feature = "analytic")]
pub use analytic::*;
#[cfg(feature = "analytic")]
mod analytic {
    use super::*;
    use chronicle_common::SyncRange;
    use scylla_rs::prelude::{
        Consistency,
        GetStaticSelectRequest,
        Iter,
        Select,
    };
    use std::ops::Range;

    /// Representation of vector of analytic data
    #[derive(Debug, Clone, Default, Serialize)]
    pub struct AnalyticsData {
        /// Vector of sequential ranges of analytics data
        #[serde(flatten)]
        pub analytics: Vec<AnalyticData>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    /// AnalyticData representation for a continuous range in scylla
    pub struct AnalyticData {
        #[serde(flatten)]
        range: Range<u32>,
        message_count: u128,
        transaction_count: u128,
        transferred_tokens: u128,
    }
    impl From<AnalyticRecord> for AnalyticData {
        fn from(record: AnalyticRecord) -> Self {
            // create analytic
            let milestone_index = **record.milestone_index();
            let message_count = **record.message_count() as u128;
            let transaction_count = **record.transaction_count() as u128;
            let transferred_tokens = **record.transferred_tokens() as u128;
            let range = Range {
                start: milestone_index,
                end: milestone_index + 1,
            };
            AnalyticData::new(range, message_count, transaction_count, transferred_tokens)
        }
    }
    impl AnalyticData {
        pub(crate) fn new(
            range: Range<u32>,
            message_count: u128,
            transaction_count: u128,
            transferred_tokens: u128,
        ) -> Self {
            Self {
                range,
                message_count,
                transaction_count,
                transferred_tokens,
            }
        }
        async fn process(mut self, analytics_data: &mut AnalyticsData, records: &mut Iter<AnalyticRecord>) {
            while let Some(record) = records.next() {
                self = self.process_record(record, analytics_data);
            }
            analytics_data.add_analytic_data(self);
        }
        fn process_record(mut self, record: AnalyticRecord, analytics_data: &mut AnalyticsData) -> Self {
            if self.start() - 1 == **record.milestone_index() {
                self.acc(record);
            } else {
                // there is gap, therefore we finish self
                analytics_data.add_analytic_data(self);
                // create new analytic_data
                self = AnalyticData::from(record);
            }
            self
        }
        fn start(&self) -> u32 {
            self.range.start
        }
        fn acc(&mut self, record: AnalyticRecord) {
            self.range.start -= 1;
            self.message_count += **record.message_count() as u128;
            self.transaction_count += **record.transaction_count() as u128;
            self.transferred_tokens += **record.transferred_tokens() as u128;
        }
    }

    impl AnalyticsData {
        /// Try to fetch the analytics data from the analytics table for the provided keyspace and sync range
        pub async fn try_fetch<S: 'static + Select<String, SyncRange, Iter<AnalyticRecord>>>(
            keyspace: &S,
            sync_range: &SyncRange,
            retries: usize,
            page_size: i32,
        ) -> anyhow::Result<AnalyticsData> {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            Self::query_analytics_table(keyspace, sync_range, retries, tx.clone(), page_size, None)?;
            let mut analytics_data = AnalyticsData::default();
            while let Some(mut records) = rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("Unable to fetch the analytics response"))??
            {
                if let Some(paging_state) = records.take_paging_state() {
                    // this will request the next page, and value worker will pass it to us through rx
                    Self::query_analytics_table(keyspace, sync_range, retries, tx.clone(), page_size, paging_state)?;
                    // Gets the first record in the page result, which is used to trigger accumulation
                    analytics_data.try_trigger(&mut records).await;
                } else {
                    // no more pages to fetch, therefore we process whatever we received, ..
                    analytics_data.try_trigger(&mut records).await;
                    // break the while
                    break;
                }
            }
            Ok(analytics_data)
        }
        async fn try_trigger(&mut self, analytics_rows: &mut Iter<AnalyticRecord>) {
            if let Some(analytic_record) = analytics_rows.next() {
                self.process(analytic_record, analytics_rows).await;
            }
        }
        async fn process(&mut self, record: AnalyticRecord, records: &mut Iter<AnalyticRecord>) {
            // check if there is an active analytic_data with continuous range
            if let Some(mut analytic_data) = self.try_pop_recent_analytic_data() {
                analytic_data = analytic_data.process_record(record, self);
                analytic_data.process(self, records).await;
            } else {
                let analytic_data = AnalyticData::from(record);
                analytic_data.process(self, records).await;
            }
        }
        fn query_analytics_table<
            S: 'static + Select<String, SyncRange, Iter<AnalyticRecord>>,
            P: Into<Option<Vec<u8>>>,
        >(
            keyspace: &S,
            sync_range: &SyncRange,
            retries: usize,
            tx: tokio::sync::mpsc::UnboundedSender<Result<Option<Iter<AnalyticRecord>>, scylla_rs::app::WorkerError>>,
            page_size: i32,
            paging_state: P,
        ) -> anyhow::Result<()> {
            let paging_state = paging_state.into();
            keyspace
                .select(&"permanode".to_string(), sync_range)
                .consistency(Consistency::One)
                .page_size(page_size)
                .paging_state(&paging_state)
                .build()?
                .worker()
                .with_retries(retries)
                .with_handle(tx)
                .send_local()?;
            Ok(())
        }
        fn try_pop_recent_analytic_data(&mut self) -> Option<AnalyticData> {
            self.analytics.pop()
        }
        fn add_analytic_data(&mut self, analytic_data: AnalyticData) {
            self.analytics.push(analytic_data);
        }
    }
}

pub struct MetricFilter {
    pub start_date: i64,
    pub end_date: i64,
    pub metrics: Vec<String>,
}

#[derive(Row)]
pub struct MetricRecord {
    pub date: i64,
    pub metric: String,
    pub value: String,
}
