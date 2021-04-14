pub use crate::keyspaces::PermanodeKeyspace;
use anyhow::{
    anyhow,
    bail,
    ensure,
};
use bee_common::packable::Packable;
use bincode::Options;
pub use delete::{
    Ed25519AddressPK,
    IndexationPK,
    ParentPK,
};
pub use scylla::access::*;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    io::Cursor,
    ops::Deref,
};
pub use types::*;

mod delete;
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

/// A record, created from a database row
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
    /// Wraps an inner type as a Record
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
    /// Unwrap the inner type
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Creates an iterator over a set of records
    /// by decoding database rows using the `Row` impl
    pub fn rows_iter(decoder: Decoder) -> anyhow::Result<scylla::access::Iter<Self>>
    where
        Self: scylla::access::Row,
    {
        scylla::access::Iter::<Self>::new(decoder)
    }
}

/// A partitioned value marker. Wraps a key type to select
/// using the partition id and milestone index.
#[derive(Clone)]
pub struct Partitioned<T> {
    inner: T,
    partition: Partition,
}

impl<T> Deref for Partitioned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Partitioned<T> {
    /// Creates a new partition wrapper with a partition id.
    /// Does not contain a milestone index. Use `with_milestone_index` to add one.
    pub fn new(inner: T, partition_id: u16, milestone_index: u32) -> Self {
        Self {
            inner,
            partition: Partition::new(partition_id, milestone_index),
        }
    }
    /// Unwrap the inner type
    pub fn into_inner(self) -> T {
        self.inner
    }
    /// Get the partition id
    pub fn partition_id(&self) -> PartitionId {
        *self.partition.id()
    }
    /// Get the milestone index
    pub fn milestone_index(&self) -> u32 {
        *self.partition.milestone_index()
    }
}

/// Defines the max time-to-live for permanode records: 20 years
pub const MAX_TTL: u32 = 20 * 365 * 24 * 60 * 60;

/// A time-to-live specifier
pub struct TTL<T> {
    inner: T,
    ttl: u32,
}

impl<T> TTL<T> {
    /// Creates a new time-to-live
    pub fn new(inner: T, ttl: u32) -> Self {
        Self { inner, ttl }
    }
}

/// A partition key
#[derive(Clone, Copy)]
pub struct Partition {
    id: u16,
    milestone_index: u32,
}

impl Partition {
    /// Creates a new partition key from a partition id and milestone index
    pub fn new(id: u16, milestone_index: u32) -> Self {
        Self { id, milestone_index }
    }
    /// Get the partition id
    pub fn id(&self) -> &u16 {
        &self.id
    }
    /// Get the milestone index
    pub fn milestone_index(&self) -> &u32 {
        &self.milestone_index
    }
}
/// An 'sync' table row
#[derive(Clone, Copy, Debug)]
pub struct SyncRecord {
    pub milestone_index: MilestoneIndex,
    pub synced_by: Option<SyncedBy>,
    pub logged_by: Option<LoggedBy>,
}

impl SyncRecord {
    /// Creates a new sync row
    pub fn new(milestone_index: MilestoneIndex, synced_by: Option<SyncedBy>, logged_by: Option<LoggedBy>) -> Self {
        Self {
            milestone_index,
            synced_by,
            logged_by,
        }
    }
}
/// An `addresses` table row
#[derive(Clone, Copy, Debug)]
pub struct AddressRecord {
    pub output_type: OutputType,
    pub transaction_id: TransactionId,
    pub index: Index,
    pub amount: Amount,
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl AddressRecord {
    /// Creates a new addresses row
    pub fn new(
        output_type: u8,
        transaction_id: TransactionId,
        index: Index,
        amount: Amount,
        ledger_inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            output_type,
            transaction_id,
            index,
            amount,
            ledger_inclusion_state,
        }
    }
}
impl From<(OutputType, TransactionId, Index, Amount, Option<LedgerInclusionState>)> for AddressRecord {
    fn from(
        (output_type, transaction_id, index, amount, ledger_inclusion_state): (
            OutputType,
            TransactionId,
            Index,
            Amount,
            Option<LedgerInclusionState>,
        ),
    ) -> Self {
        Self::new(output_type, transaction_id, index, amount, ledger_inclusion_state)
    }
}

/// An `indexes` table row
#[derive(Clone, Copy, Debug)]
pub struct IndexationRecord {
    pub message_id: MessageId,
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl IndexationRecord {
    /// Creates a new index row
    pub fn new(message_id: MessageId, ledger_inclusion_state: Option<LedgerInclusionState>) -> Self {
        Self {
            message_id,
            ledger_inclusion_state,
        }
    }
}

/// A `parents` table row
#[derive(Clone, Copy, Debug)]
pub struct ParentRecord {
    pub message_id: MessageId,
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl ParentRecord {
    /// Creates a new parent row
    pub fn new(message_id: MessageId, ledger_inclusion_state: Option<LedgerInclusionState>) -> Self {
        Self {
            message_id,
            ledger_inclusion_state,
        }
    }
}

/// A `transactions` table row
#[derive(Clone)]
pub struct TransactionRecord {
    variant: TransactionVariant,
    message_id: MessageId,
    data: TransactionData,
    inclusion_state: Option<LedgerInclusionState>,
    milestone_index: Option<MilestoneIndex>,
}

impl TransactionRecord {
    /// Creates an input transactions record
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
    /// Creates an output transactions record
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
    /// Creates an unlock block transactions record
    pub fn unlock(
        message_id: MessageId,
        data: UnlockData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            variant: TransactionVariant::Unlock,
            message_id,
            data: TransactionData::Unlock(data),
            inclusion_state,
            milestone_index,
        }
    }
}
/// Transaction variants. Can be Input, Output, or Unlock.
#[repr(u8)]
#[derive(Clone, Copy)]
pub enum TransactionVariant {
    /// A transaction's Input, which spends a prior Output
    Input = 0,
    /// A transaction's Unspent Transaction Output (UTXO), specifying an address to receive the funds
    Output = 1,
    /// A transaction's Unlock Block, used to unlock an Input for verification
    Unlock = 2,
}

impl ColumnDecoder for TransactionVariant {
    fn try_decode(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(match std::str::from_utf8(slice)? {
            "input" => TransactionVariant::Input,
            "output" => TransactionVariant::Output,
            "unlock" => TransactionVariant::Unlock,
            _ => bail!("Unexpected variant type"),
        })
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
