use bee_message::*;
use packable::{
    packer::Packer,
    unpacker::Unpacker,
    Packable,
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
    type UnpackError = anyhow::Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        self.input_tx_id.pack(packer)?;
        self.input_index.pack(packer)?;
        self.unlock_block.pack(packer)?;
        Ok(())
    }
    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, UnpackError<Self::UnpackError, U::Error>> {
        Ok(Self {
            input_tx_id: TransactionId::unpack(unpacker)?,
            input_index: u16::unpack(unpacker)?,
            unlock_block: UnlockBlock::unpack(unpacker)?,
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
    type UnpackError = anyhow::Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        match self {
            InputData::Utxo(utxo_input, unlock_block) => {
                0u8.pack(packer)?;
                utxo_input.pack(packer)?;
                unlock_block.pack(packer)?;
            }
            InputData::Treasury(treasury_input) => {
                1u8.pack(packer)?;
                treasury_input.pack(packer)?;
            }
        }
        Ok(())
    }
    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, UnpackError<Self::UnpackError, U::Error>> {
        Ok(match u8::unpack(reader)? {
            0 => InputData::Utxo(UtxoInput::unpack(unpacker)?, UnlockBlock::unpack(unpacker)?),
            1 => InputData::Treasury(TreasuryInput::unpack(unpacker)?),
            _ => anyhow::bail!("Tried to unpack an invalid inputdata variant!"),
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
    type UnpackError = anyhow::Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        match self {
            TransactionData::Input(input_data) => {
                0u8.pack(packer)?;
                input_data.pack(packer)?;
            }
            TransactionData::Output(packer) => {
                1u8.pack(packer)?;
                output.pack(packer)?;
            }
            TransactionData::Unlock(block_data) => {
                2u8.pack(packer)?;
                block_data.pack(packer)?;
            }
        }
        Ok(())
    }
    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, UnpackError<Self::UnpackError, U::Error>> {
        Ok(match u8::unpack(reader)? {
            0 => TransactionData::Input(InputData::unpack(unpacker)?),
            1 => TransactionData::Output(Output::unpack(unpacker)?),
            2 => TransactionData::Unlock(UnlockData::unpack(unpacker)?),
            n => anyhow::bail!("Tried to unpack an invalid transaction variant!, tag: {}", n),
        })
    }
}

impl ColumnDecoder for TransactionData {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Self::unpack(&mut Cursor::new(slice)).map(Into::into)
    }
}
