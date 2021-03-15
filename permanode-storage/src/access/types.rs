use std::{
    borrow::Cow,
    ops::{
        Deref,
        DerefMut,
    },
};

use bee_common::packable::Packable;
pub use bee_ledger::{
    balance::Balance,
    model::{
        OutputDiff,
        Unspent,
    },
};

pub use bee_message::{
    ledger_index::LedgerIndex,
    milestone::Milestone,
    prelude::{
        Address,
        ConsumedOutput,
        CreatedOutput,
        Ed25519Address,
        HashedIndex,
        MilestoneIndex,
        Output,
        OutputId,
        Payload,
        TransactionId,
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
/// Address type
pub type AddressType = u8;
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
/// Chrysalis transaction data
pub enum TransactionData {
    /// An unspent transaction input
    Input(UTXOInput),
    /// A transaction output
    Output(Output),
    /// A signed block which can be used to unlock an input
    Unlock(UnlockBlock),
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
            TransactionData::Input(utxo_input) => {
                0u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                utxo_input.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
            }
            TransactionData::Output(output) => {
                1u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                output.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
            }
            TransactionData::Unlock(block) => {
                2u8.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
                block.pack(writer).map_err(|e| Cow::from(e.to_string()))?;
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
                UTXOInput::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            )),
            1 => Ok(TransactionData::Output(
                Output::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
            )),
            2 => Ok(TransactionData::Unlock(
                UnlockBlock::unpack(reader).map_err(|e| Cow::from(e.to_string()))?,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LedgerInclusionState {
    #[serde(rename = "conflicting")]
    Conflicting,
    #[serde(rename = "included")]
    Included,
    #[serde(rename = "noTransaction")]
    NoTransaction,
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
pub struct MessageChildren {
    pub children: Vec<MessageId>,
}

#[derive(Debug, Clone)]
pub struct IndexMessages {
    pub messages: Vec<MessageId>,
}

#[derive(Debug, Clone)]
pub struct OutputIds {
    pub ids: Vec<OutputId>,
}

#[derive(Debug, Clone)]
pub struct Outputs {
    pub outputs: Vec<(MessageId, TransactionData)>,
}
