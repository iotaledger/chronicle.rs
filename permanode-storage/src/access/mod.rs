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
#[derive(Clone)]
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

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
pub struct AddressRecord {
    milestone_index: MilestoneIndex,
    transaction_id: TransactionId,
    index: Index,
    amount: Amount,
    address_type: AddressType,
    ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl AddressRecord {
    pub fn new(
        milestone_index: MilestoneIndex,
        transaction_id: TransactionId,
        index: Index,
        amount: Amount,
        address_type: AddressType,
        ledger_inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            transaction_id,
            index,
            amount,
            address_type,
            ledger_inclusion_state,
        }
    }
}
impl
    From<(
        MilestoneIndex,
        TransactionId,
        Index,
        Amount,
        AddressType,
        Option<LedgerInclusionState>,
    )> for AddressRecord
{
    fn from(
        (milestone_index, transaction_id, index, amount, address_type, ledger_inclusion_state): (
            MilestoneIndex,
            TransactionId,
            Index,
            Amount,
            AddressType,
            Option<LedgerInclusionState>,
        ),
    ) -> Self {
        Self::new(
            milestone_index,
            transaction_id,
            index,
            amount,
            address_type,
            ledger_inclusion_state,
        )
    }
}

#[derive(Clone, Copy)]
pub struct HashedIndexRecord {
    milestone_index: MilestoneIndex,
    message_id: MessageId,
    ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl HashedIndexRecord {
    pub fn new(
        milestone_index: MilestoneIndex,
        message_id: MessageId,
        ledger_inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            message_id,
            ledger_inclusion_state,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ParentRecord {
    milestone_index: MilestoneIndex,
    message_id: MessageId,
    ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl ParentRecord {
    pub fn new(
        milestone_index: MilestoneIndex,
        message_id: MessageId,
        ledger_inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            message_id,
            ledger_inclusion_state,
        }
    }
}

#[derive(Clone)]
pub struct TransactionRecord {
    variant: TransactionVariant,
    message_id: MessageId,
    data: TransactionData,
    inclusion_state: Option<LedgerInclusionState>,
    milestone_index: Option<MilestoneIndex>,
}

impl TransactionRecord {
    pub fn input(
        message_id: MessageId,
        input_data: InputData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            variant: TransactionVariant::Input,
            message_id,
            data: TransactionData::Input(input_data),
            inclusion_state,
            milestone_index,
        }
    }
    pub fn output(
        message_id: MessageId,
        data: Output,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            variant: TransactionVariant::Output,
            message_id,
            data: TransactionData::Output(data),
            inclusion_state,
            milestone_index,
        }
    }
    pub fn unlock(
        message_id: MessageId,
        data: UnlockData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            variant: TransactionVariant::Output,
            message_id,
            data: TransactionData::Unlock(data),
            inclusion_state,
            milestone_index,
        }
    }
}
#[repr(u8)]
#[derive(Clone, Copy)]
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
