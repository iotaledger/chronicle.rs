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
use bee_message::prelude::{
    UTXOInput,
    UnlockBlock,
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
        HASHED_INDEX_LENGTH,
    },
    solid_entry_point::SolidEntryPoint,
    Message,
    MessageId,
};
pub use bee_snapshot::SnapshotInfo;
pub use bee_tangle::{
    metadata::MessageMetadata,
    unconfirmed_message::UnconfirmedMessage,
};
use scylla_cql::ColumnDecoder;
use serde::{
    Deserialize,
    Serialize,
};
use std::io::Cursor;

#[derive(Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Bee<Type> {
    inner: Type,
}

impl<Type> Bee<Type> {
    pub fn wrap(t: Type) -> Bee<Type> {
        Bee { inner: t }
    }

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

pub enum TransactionData {
    Input(UTXOInput),
    Output(Output),
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
