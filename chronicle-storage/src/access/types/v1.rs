use super::*;
use bee_common::packable::Packable;
use bee_message_v1::{
    payload::Payload,
    prelude::{
        Essence,
        Output,
        TransactionId,
        TreasuryInput,
        UnlockBlock,
        UtxoInput,
    },
    MessageId,
};

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
