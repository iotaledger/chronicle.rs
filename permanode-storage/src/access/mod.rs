pub use crate::keyspaces::PermanodeKeyspace;
use bee_common::packable::Packable;
pub use scylla::access::*;
use scylla_cql::{
    ColumnDecoder,
    ColumnEncoder,
    PreparedStatement,
    Rows,
    VoidDecoder,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    io::Cursor,
    ops::Deref,
};

use bincode::Options;
pub use types::*;

mod insert;
mod select;
mod token;
mod types;

impl VoidDecoder for PermanodeKeyspace {}

use bincode::config::*;
#[allow(unused)]
pub(crate) type BincodeOptions =
    WithOtherTrailing<WithOtherIntEncoding<WithOtherEndian<DefaultOptions, BigEndian>, FixintEncoding>, AllowTrailing>;
#[allow(unused)]
pub(crate) fn bincode_config() -> BincodeOptions {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

pub struct Record<T> {
    inner: T,
}

impl<T> Deref for Record<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Record<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn rows_iter(decoder: Decoder) -> scylla::access::Iter<Self>
    where
        Self: scylla::access::Row,
    {
        scylla::access::Iter::<Self>::new(decoder)
    }
}
pub struct Partitioned<T> {
    inner: T,
    partition_id: u16,
}

impl<T> Partitioned<T> {
    pub fn new(inner: T, partition_id: u16) -> Self {
        Self { inner, partition_id }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn partition_id(self) -> u16 {
        self.partition_id
    }
}

pub struct AddressRecord {
    transaction_id: TransactionId,
    index: Index,
    amount: Amount,
    address_type: AddressType,
}

impl AddressRecord {
    pub fn new(transaction_id: TransactionId, index: Index, amount: Amount, address_type: AddressType) -> Self {
        Self {
            transaction_id,
            index,
            amount,
            address_type,
        }
    }
}
impl From<(TransactionId, Index, Amount, AddressType)> for AddressRecord {
    fn from((transaction_id, index, amount, address_type): (TransactionId, Index, Amount, AddressType)) -> Self {
        Self::new(transaction_id, index, amount, address_type)
    }
}

pub struct TransactionRecord {
    transaction_id: TransactionId,
    index: Index,
    variant: TransactionVariant,
    ref_transaction_id: TransactionId,
    ref_index: Index,
    message_id: MessageId,
    data: TransactionData,
}

impl TransactionRecord {
    pub fn input(
        transaction_id: TransactionId,
        index: Index,
        ref_transaction_id: TransactionId,
        ref_index: Index,
        message_id: MessageId,
        data: UTXOInput,
    ) -> Self {
        Self {
            transaction_id,
            index,
            variant: TransactionVariant::Input,
            ref_transaction_id,
            ref_index,
            message_id,
            data: TransactionData::Input(data),
        }
    }
    pub fn output(
        transaction_id: TransactionId,
        index: Index,
        ref_transaction_id: TransactionId,
        ref_index: Index,
        message_id: MessageId,
        data: Output,
    ) -> Self {
        Self {
            transaction_id,
            index,
            variant: TransactionVariant::Output,
            ref_transaction_id,
            ref_index,
            message_id,
            data: TransactionData::Output(data),
        }
    }
    pub fn unlock(
        transaction_id: TransactionId,
        index: Index,
        ref_transaction_id: TransactionId,
        ref_index: Index,
        message_id: MessageId,
        data: UnlockBlock,
    ) -> Self {
        Self {
            transaction_id,
            index,
            variant: TransactionVariant::Output,
            ref_transaction_id,
            ref_index,
            message_id,
            data: TransactionData::Unlock(data),
        }
    }
}
#[repr(u8)]
pub enum TransactionVariant {
    Input = 0,
    Output = 1,
    Unlock = 2,
}
