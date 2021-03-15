pub use crate::keyspaces::PermanodeKeyspace;
use bee_common::packable::Packable;
pub use scylla::access::*;

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

#[derive(Debug, Clone)]
pub struct Partitioned<T> {
    inner: T,
    partition_id: PartitionId,
}

impl<T> Deref for Partitioned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Partitioned<T> {
    pub fn new(inner: T, partition_id: u16) -> Self {
        Self { inner, partition_id }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }
}

pub const MAX_TTL: u32 = 20 * 365 * 24 * 60 * 60;

pub struct TTL<T> {
    inner: T,
    ttl: u32,
}

impl<T> TTL<T> {
    pub fn new(inner: T, ttl: u32) -> Self {
        Self { inner, ttl }
    }
}

pub struct Hint<T: HintVariant> {
    inner: T,
}
pub struct Partition {
    id: u16,
    milestone_index: u32,
}

impl Partition {
    pub fn new(id: u16, milestone_index: u32) -> Self {
        Self { id, milestone_index }
    }
    pub fn id(&self) -> &u16 {
        &self.id
    }
    pub fn milestone_index(&self) -> &u32 {
        &self.milestone_index
    }
}

impl<T: HintVariant> Hint<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
    pub fn get_inner(&self) -> &T {
        &self.inner
    }
}

pub trait HintVariant {
    fn variant() -> &'static str;
    fn as_bytes(&self) -> &[u8];
}
impl HintVariant for Ed25519Address {
    fn variant() -> &'static str {
        "address"
    }
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}
impl HintVariant for MessageId {
    fn variant() -> &'static str {
        "parent"
    }
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl HintVariant for HashedIndex {
    fn variant() -> &'static str {
        "index"
    }
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
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

impl ColumnDecoder for TransactionVariant {
    fn decode(slice: &[u8]) -> Self {
        match std::str::from_utf8(slice).expect("Invalid string in variant column") {
            "input" => TransactionVariant::Input,
            "output" => TransactionVariant::Output,
            "unlock" => TransactionVariant::Unlock,
            _ => panic!("Unexpected variant type"),
        }
    }
}

impl ColumnEncoder for TransactionVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let variant;
        match self {
            TransactionVariant::Input => variant = "input",
            TransactionVariant::Output => variant = "output",
            TransactionVariant::Unlock => variant = "unlock",
        }
        buffer.extend(&i32::to_be_bytes(variant.len() as i32));
        buffer.extend(variant.as_bytes());
    }
}

#[derive(Clone)]
pub struct PagingState {
    page_size: usize,
    offset: usize,
}

impl PagingState {
    pub fn new(page_size: usize, offset: usize) -> Self {
        Self { page_size, offset }
    }

    pub fn increment(&mut self, n: usize) {
        self.offset += n
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        todo!()
    }
}
