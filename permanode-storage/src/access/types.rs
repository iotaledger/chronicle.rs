use std::{
    borrow::Cow,
    ops::{
        Deref,
        DerefMut,
    },
};

use bee_common::packable::Packable;
pub use bee_ledger::types::{
    Balance,
    OutputDiff,
    Unspent,
};

pub use bee_message::{
    ledger_index::LedgerIndex,
    milestone::Milestone,
    prelude::{
        Address,
        ConsumedOutput,
        CreatedOutput,
        Ed25519Address,
        Input,
        MilestoneIndex,
        Output,
        OutputId,
        Parents,
        Payload,
        TransactionId,
        TreasuryInput,
        UTXOInput,
        UnlockBlock,
        HASHED_INDEX_LENGTH,
    },
    solid_entry_point::SolidEntryPoint,
    Message,
    MessageId,
};
pub use bee_snapshot::SnapshotInfo;
pub use bee_tangle::{
    flags::Flags,
    unconfirmed_message::UnconfirmedMessage,
};
/// Index type
pub type Index = u16;
/// Amount type
pub type Amount = u64;
/// Output type
pub type OutputType = u8;
/// ParentIndex type
pub type ParentIndex = u16;

use super::*;
use std::io::Cursor;

/// A `bee` type wrapper which is used to apply the `ColumnEncoder`
/// functionality over predefined types which are `Packable`.
#[derive(Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Bee<Type> {
    inner: Type,
}

impl<Type> Bee<Type> {
    /// Wrap a `bee` type
    pub fn wrap(t: Type) -> Bee<Type> {
        Bee { inner: t }
    }

    /// Consume the wrapper and return the inner `bee` type
    pub fn into_inner(self) -> Type {
        self.inner
    }
}

impl<Type> Deref for Bee<Type> {
    type Target = Type;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Type> DerefMut for Bee<Type> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Type> From<Type> for Bee<Type> {
    fn from(t: Type) -> Self {
        Bee::wrap(t)
    }
}

impl<P: Packable> ColumnDecoder for Bee<P> {
    fn decode(slice: &[u8]) -> Self {
        P::unpack(&mut Cursor::new(slice)).unwrap().into()
    }
}

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
    pub fn new(input_tx_id: TransactionId, input_index: u16, unlock_block: UnlockBlock) -> Self {
        Self {
            input_tx_id,
            input_index,
            unlock_block,
        }
    }
}
impl Packable for UnlockData {
    type Error = Cow<'static, str>;
    fn packed_len(&self) -> usize {
        self.input_tx_id.packed_len() + self.input_index.packed_len() + self.unlock_block.packed_len()
    }
    fn pack<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        self.input_tx_id.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
        self.input_index.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
        self.unlock_block.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
        Ok(())
    }
    fn unpack<R: std::io::Read + ?Sized>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self {
            input_tx_id: TransactionId::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            input_index: u16::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            unlock_block: UnlockBlock::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
        })
    }
}

#[derive(Debug, Clone)]
pub enum InputData {
    UTXO(UTXOInput, UnlockBlock),
    Treasury(TreasuryInput),
}

impl InputData {
    pub fn utxo(utxo_input: UTXOInput, unlock_block: UnlockBlock) -> Self {
        Self::UTXO(utxo_input, unlock_block)
    }
    pub fn treasury(treasury_input: TreasuryInput) -> Self {
        Self::Treasury(treasury_input)
    }
}

impl Packable for InputData {
    type Error = Cow<'static, str>;
    fn packed_len(&self) -> usize {
        match self {
            InputData::UTXO(utxo_input, unlock_block) => {
                0u8.packed_len() + utxo_input.packed_len() + unlock_block.packed_len()
            }
            InputData::Treasury(treasury_input) => 0u8.packed_len() + treasury_input.packed_len(),
        }
    }
    fn pack<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        match self {
            InputData::UTXO(utxo_input, unlock_block) => {
                0u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                utxo_input.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                unlock_block.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
            }
            InputData::Treasury(treasury_input) => {
                1u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                treasury_input.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
            }
        }
        Ok(())
    }
    fn unpack<R: std::io::Read + ?Sized>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        match u8::unpack(reader).map_err(|e| Cow::from(e.to_string()))? {
            0 => Ok(InputData::UTXO(
                UTXOInput::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
                UnlockBlock::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            )),
            1 => Ok(InputData::Treasury(
                TreasuryInput::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            )),
            _ => Err("Tried to unpack an invalid inputdata variant!".into()),
        }
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
    type Error = Cow<'static, str>;

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
                0u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                input_data.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
            }
            TransactionData::Output(output) => {
                1u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                output.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
            }
            TransactionData::Unlock(block_data) => {
                2u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                block_data.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
            }
        }
        Ok(())
    }

    fn unpack<R: std::io::Read + ?Sized>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        match u8::unpack(reader).map_err(|e| Cow::from(e.to_string()))? {
            0 => Ok(TransactionData::Input(
                InputData::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            )),
            1 => Ok(TransactionData::Output(
                Output::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            )),
            2 => Ok(TransactionData::Unlock(
                UnlockData::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            )),
            _ => Err("Tried to unpack an invalid transaction variant!".into()),
        }
    }
}

impl ColumnDecoder for TransactionData {
    fn decode(slice: &[u8]) -> Self {
        Self::unpack(&mut Cursor::new(slice)).unwrap().into()
    }
}
/// MessageMetadata storage object
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageMetadataObj {
    #[serde(rename = "messageId")]
    pub message_id: MessageId,
    #[serde(rename = "parentMessageIds")]
    pub parent_message_ids: Vec<MessageId>,
    #[serde(rename = "isSolid")]
    pub is_solid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "referencedByMilestoneIndex")]
    pub referenced_by_milestone_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ledgerInclusionState")]
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "shouldPromote")]
    pub should_promote: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "shouldReattach")]
    pub should_reattach: Option<bool>,
}

/// Response of GET /api/v1/messages/{message_id}/metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageMetadata {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "parentMessageIds")]
    pub parent_message_ids: Vec<String>,
    #[serde(rename = "isSolid")]
    pub is_solid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "referencedByMilestoneIndex")]
    pub referenced_by_milestone_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ledgerInclusionState")]
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "shouldPromote")]
    pub should_promote: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "shouldReattach")]
    pub should_reattach: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum LedgerInclusionState {
    #[serde(rename = "conflicting")]
    Conflicting,
    #[serde(rename = "included")]
    Included,
    #[serde(rename = "noTransaction")]
    NoTransaction,
}

impl ColumnEncoder for LedgerInclusionState {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let bytes = bincode_config().serialize(self).unwrap();
        buffer.extend(&i32::to_be_bytes(bytes.len() as i32));
        buffer.extend(bytes)
    }
}

impl ColumnDecoder for LedgerInclusionState {
    fn decode(slice: &[u8]) -> Self {
        bincode_config().deserialize(slice).unwrap()
    }
}

impl ColumnEncoder for MessageMetadata {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let bytes = bincode_config().serialize(self).unwrap();
        buffer.extend(&i32::to_be_bytes(bytes.len() as i32));
        buffer.extend(bytes)
    }
}

impl ColumnDecoder for MessageMetadata {
    fn decode(slice: &[u8]) -> Self {
        bincode_config().deserialize(slice).unwrap()
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

#[derive(Debug, Clone)]
pub struct OutputData {
    pub output: CreatedOutput,
    pub unlock_blocks: Vec<UnlockRes>,
}

#[derive(Debug, Clone)]
pub struct UnlockRes {
    pub message_id: MessageId,
    pub block: UnlockBlock,
    pub inclusion_state: Option<LedgerInclusionState>,
}

pub type PartitionId = u16;

#[derive(Clone)]
pub struct Indexation(pub String);

#[derive(Clone)]
pub struct Hint {
    pub hint: String,
    pub variant: HintVariant,
}

impl Hint {
    pub fn index(index: String) -> Self {
        Self {
            hint: index,
            variant: HintVariant::Index,
        }
    }

    pub fn address(address: String) -> Self {
        Self {
            hint: address,
            variant: HintVariant::Address,
        }
    }

    pub fn parent(parent: String) -> Self {
        Self {
            hint: parent,
            variant: HintVariant::Parent,
        }
    }
}
#[derive(Clone)]
pub enum HintVariant {
    Address,
    Index,
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
