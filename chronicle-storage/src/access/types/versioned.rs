/// Versioned transaction data
pub enum VersionedTransactionData {
    V1(v1::TransactionData),
    V2(v2::TransactionData),
}

impl ColumnEncoder for VersionedTransactionData {
    fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            V1(v1_data) => {
                let mut v1_buffer = vec![1];
                v1_data
                    .pack(&v1_buffer)
                    .expect("Unable to pack v1 invalid transaction data");
                buffer.extend(u32::to_be_bytes(v1_buffer.len()));
                buffer.extend(v1_buffer);
            }
            V2(v2_data) => {
                let mut v2_buffer = vec![1];
                let mut packer = packable::packer::IoPacker::new(v2_buffer);
                v2_data
                    .pack(packer)
                    .expect("Unable to pack v2 invalid transaction data");
                v2_buffer = packer.into_inner();
                buffer.extend(u32::to_be_bytes(v2_buffer.len()));
                buffer.extend(v2_buffer);
            }
        }
    }
}

impl Packable for VersionedTransactionData {
    type UnpackError = anyhow::Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        match self {
            V1(v1_tx_data) => {
                0u8.pack(packer)?;
                v1_tx_data.pack(packer)?
            }
            V2(v2_tx_data) => {}
        }
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
