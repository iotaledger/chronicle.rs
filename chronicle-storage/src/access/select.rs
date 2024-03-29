// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::SyncRange;
use std::{
    collections::{
        hash_map::Entry,
        BTreeMap,
        HashMap,
        VecDeque,
    },
    str::FromStr,
};

impl Select<MessageId, Message> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl RowsDecoder<MessageId, Message> for ChronicleKeyspace {
    type Row = Record<Option<Message>>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Message>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| row.into_inner())
            .flatten())
    }
}

impl Select<MessageId, MessageMetadata> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT metadata FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl RowsDecoder<MessageId, MessageMetadata> for ChronicleKeyspace {
    type Row = Record<Option<MessageMetadata>>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<MessageMetadata>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");

        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| row.into_inner())
            .flatten())
    }
}

impl Select<MessageId, (Option<Message>, Option<MessageMetadata>)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message, metadata FROM {}.messages WHERE message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl RowsDecoder<MessageId, (Option<Message>, Option<MessageMetadata>)> for ChronicleKeyspace {
    type Row = Record<(Option<Message>, Option<MessageMetadata>)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<(Option<Message>, Option<MessageMetadata>)>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        if let Some(row) = Self::Row::rows_iter(decoder)?.next() {
            let row = row.into_inner();
            Ok(Some((row.0, row.1)))
        } else {
            Ok(None)
        }
    }
}

impl Select<MessageId, FullMessage> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message, metadata FROM {}.messages WHERE message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl RowsDecoder<MessageId, FullMessage> for ChronicleKeyspace {
    type Row = Record<(Option<Message>, Option<MessageMetadata>)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<FullMessage>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        if let Some(row) = Self::Row::rows_iter(decoder)?.next() {
            if let (Some(message), Some(metadata)) = row.into_inner() {
                Ok(Some(FullMessage::new(message, metadata)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

impl Select<Partitioned<MessageId>, Paged<VecDeque<Partitioned<ParentRecord>>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index, message_id, inclusion_state
            FROM {}.parents
            WHERE parent_id = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &Partitioned<MessageId>) -> T::Return {
        builder
            .value(&message_id.to_string())
            .value(&message_id.partition_id())
            .value(&message_id.milestone_index())
    }
}

impl<K> RowsDecoder<Partitioned<K>, Paged<VecDeque<Partitioned<ParentRecord>>>> for ChronicleKeyspace {
    type Row = Record<(PartitionId, MilestoneIndex, MessageId, Option<LedgerInclusionState>)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Paged<VecDeque<Partitioned<ParentRecord>>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        let values = iter
            .map(|row| {
                let (partition_id, milestone_index, message_id, inclusion_state) = row.into_inner();
                Partitioned::new(
                    ParentRecord::new(message_id, inclusion_state),
                    partition_id,
                    milestone_index.0,
                )
            })
            .collect();
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<Partitioned<Indexation>, Paged<VecDeque<Partitioned<IndexationRecord>>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index, message_id, inclusion_state
            FROM {}.indexes
            WHERE indexation = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, index: &Partitioned<Indexation>) -> T::Return {
        builder
            .value(&index.0)
            .value(&index.partition_id())
            .value(&index.milestone_index())
    }
}

impl<K> RowsDecoder<Partitioned<K>, Paged<VecDeque<Partitioned<IndexationRecord>>>> for ChronicleKeyspace {
    type Row = Record<(PartitionId, MilestoneIndex, MessageId, Option<LedgerInclusionState>)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Paged<VecDeque<Partitioned<IndexationRecord>>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        let values = iter
            .map(|row| {
                let (partition_id, milestone_index, message_id, inclusion_state) = row.into_inner();
                Partitioned::new(
                    IndexationRecord::new(message_id, inclusion_state),
                    partition_id,
                    milestone_index.0,
                )
            })
            .collect();
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<Partitioned<Ed25519Address>, Paged<VecDeque<Partitioned<AddressRecord>>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index, output_type, transaction_id, idx, amount, inclusion_state
            FROM {}.addresses
            WHERE address = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, address: &Partitioned<Ed25519Address>) -> T::Return {
        builder
            .value(&address.to_string())
            .value(&address.partition_id())
            .value(&address.milestone_index())
    }
}

impl RowsDecoder<Partitioned<Ed25519Address>, Paged<VecDeque<Partitioned<AddressRecord>>>> for ChronicleKeyspace {
    type Row = Record<(
        PartitionId,
        MilestoneIndex,
        OutputType,
        TransactionId,
        Index,
        Amount,
        Option<LedgerInclusionState>,
    )>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Paged<VecDeque<Partitioned<AddressRecord>>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        let (_, values) = iter.fold(
            (
                HashMap::<OutputId, usize>::new(),
                VecDeque::<Partitioned<AddressRecord>>::new(),
            ),
            |(mut map, mut values), row| {
                let (partition_id, milestone_index, output_type, transaction_id, index, amount, inclusion_state) =
                    row.into_inner();
                let record = Partitioned::new(
                    AddressRecord::new(output_type, transaction_id, index, amount, inclusion_state),
                    partition_id,
                    milestone_index.0,
                );
                if let Ok(output_id) = OutputId::new(transaction_id, index) {
                    match map.entry(output_id) {
                        Entry::Occupied(v) => {
                            if let Some(prev) = values.get_mut(*v.get()) {
                                if prev.ledger_inclusion_state.is_none() {
                                    *prev = record;
                                }
                            }
                        }
                        Entry::Vacant(e) => {
                            e.insert(values.len());
                            values.push_back(record);
                        }
                    }
                }
                (map, values)
            },
        );
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<OutputId, OutputRes> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data, inclusion_state
            FROM {}.transactions
            WHERE transaction_id = ?
            AND idx = ?
            AND variant IN ('output', 'unlock')",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, output_id: &OutputId) -> T::Return {
        builder
            .value(&output_id.transaction_id().to_string())
            .value(&output_id.index())
    }
}

impl RowsDecoder<OutputId, OutputRes> for ChronicleKeyspace {
    type Row = Record<(MessageId, TransactionData, Option<LedgerInclusionState>)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<OutputRes>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut unlock_blocks = Vec::new();
        let mut output = None;
        for (message_id, transaction_data, inclusion_state) in
            Self::Row::rows_iter(decoder)?.map(|row| row.into_inner())
        {
            match transaction_data {
                TransactionData::Output(o) => output = Some((message_id, o)),
                TransactionData::Unlock(u) => unlock_blocks.push(UnlockRes {
                    message_id,
                    block: u.unlock_block,
                    inclusion_state,
                }),
                _ => (),
            }
        }
        Ok(output.map(|output| OutputRes {
            message_id: output.0,
            output: output.1,
            unlock_blocks,
        }))
    }
}

impl Select<TransactionId, TransactionRes> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data, idx, inclusion_state, milestone_index
            FROM {}.transactions
            WHERE transaction_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, txn_id: &TransactionId) -> T::Return {
        builder.value(&txn_id.to_string())
    }
}

impl RowsDecoder<TransactionId, TransactionRes> for ChronicleKeyspace {
    type Row = Record<(
        MessageId,
        TransactionData,
        u16,
        Option<LedgerInclusionState>,
        Option<MilestoneIndex>,
    )>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<TransactionRes>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut outputs = BTreeMap::new();
        let mut unlock_blocks = BTreeMap::new();
        let mut inputs = BTreeMap::new();
        let mut metadata = None;
        let mut inclusion_state = None;
        for (message_id, transaction_data, idx, ledger_inclusion_state, milestone_index) in
            Self::Row::rows_iter(decoder)?.map(|row| row.into_inner())
        {
            match transaction_data {
                TransactionData::Output(o) => {
                    outputs.insert(idx, o);
                    metadata = metadata.or(Some((message_id, milestone_index)));
                    inclusion_state = ledger_inclusion_state;
                }
                TransactionData::Unlock(u) => {
                    unlock_blocks.insert(
                        idx,
                        UnlockRes {
                            message_id,
                            block: u.unlock_block,
                            inclusion_state: ledger_inclusion_state,
                        },
                    );
                }
                TransactionData::Input(i) => {
                    inputs.insert(idx, i);
                }
            }
        }
        let outputs = outputs
            .into_iter()
            .map(|(idx, o)| (o, unlock_blocks.remove(&idx)))
            .collect();
        Ok(metadata.map(|(message_id, milestone_index)| TransactionRes {
            message_id,
            milestone_index,
            outputs,
            inputs: inputs.into_iter().map(|(_, i)| (i)).collect(),
            inclusion_state,
        }))
    }
}

impl Select<TransactionId, MessageId> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id FROM {}.transactions
            WHERE transaction_id = ? and inclusion_state = ? and variant = 'input'
            LIMIT 1 ALLOW FILTERING",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, transaction_id: &TransactionId) -> T::Return {
        builder
            .value(&transaction_id.to_string())
            .value(&LedgerInclusionState::Included)
    }
}

impl RowsDecoder<TransactionId, MessageId> for ChronicleKeyspace {
    type Row = Record<Option<MessageId>>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<MessageId>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");

        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| row.into_inner())
            .flatten())
    }
}

impl Select<MilestoneIndex, Milestone> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, timestamp FROM {}.milestones WHERE milestone_index = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, index: &MilestoneIndex) -> T::Return {
        builder.value(&index.0)
    }
}

impl RowsDecoder<MilestoneIndex, Milestone> for ChronicleKeyspace {
    type Row = Record<(MessageId, u64)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Milestone>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| Milestone::new(row.0, row.1)))
    }
}

impl Select<Hint, Vec<(MilestoneIndex, PartitionId)>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT milestone_index, partition_id
            FROM {}.hints
            WHERE hint = ? AND variant = ?",
            self.name()
        )
        .into()
    }

    fn bind_values<T: Values>(builder: T, hint: &Hint) -> T::Return {
        builder.value(&hint.hint).value(&hint.variant.to_string())
    }
}

impl<K> RowsDecoder<K, Vec<(MilestoneIndex, PartitionId)>> for ChronicleKeyspace {
    type Row = Record<(u32, u16)>;

    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Vec<(MilestoneIndex, PartitionId)>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        Ok(Some(
            Self::Row::rows_iter(decoder)?
                .map(|row| {
                    let (index, partition_id) = row.into_inner();
                    (MilestoneIndex(index), partition_id)
                })
                .collect(),
        ))
    }
}

impl Select<SyncRange, Iter<SyncRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT milestone_index, synced_by, logged_by FROM {}.sync WHERE key = ? AND milestone_index >= ? AND milestone_index < ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, sync_range: &SyncRange) -> T::Return {
        builder
            .value(&"permanode")
            .value(&sync_range.from)
            .value(&sync_range.to)
    }
}

impl RowsDecoder<SyncRange, Iter<SyncRecord>> for ChronicleKeyspace {
    type Row = SyncRecord;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Iter<SyncRecord>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let rows_iter = Self::Row::rows_iter(decoder)?;
        if rows_iter.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows_iter))
        }
    }
}

impl Select<SyncRange, Iter<AnalyticRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT milestone_index, message_count, transaction_count, transferred_tokens FROM {}.analytics WHERE key = ? AND milestone_index >= ? AND milestone_index < ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, sync_range: &SyncRange) -> T::Return {
        builder
            .value(&"permanode")
            .value(&sync_range.from)
            .value(&sync_range.to)
    }
}

impl RowsDecoder<SyncRange, Iter<AnalyticRecord>> for ChronicleKeyspace {
    type Row = AnalyticRecord;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Iter<AnalyticRecord>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let rows_iter = Self::Row::rows_iter(decoder)?;
        if rows_iter.is_empty() && !rows_iter.has_more_pages() {
            Ok(None)
        } else {
            // CQL specs states that the page result might be empty but has more pages to fetch.
            Ok(Some(rows_iter))
        }
    }
}

// ###############
// ROW DEFINITIONS
// ###############

impl Row for Record<Option<Message>> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        Ok(Record::new(rows.column_value::<Option<Cursor<Vec<u8>>>>().and_then(
            |bytes| Ok(bytes.map(|mut bytes| Message::unpack(&mut bytes)).transpose()?),
        )?))
    }
}

impl Row for Record<Option<MessageMetadata>> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        Ok(Record::new(rows.column_value::<Option<MessageMetadata>>()?))
    }
}

impl Row for Record<(Option<Message>, Option<MessageMetadata>)> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let message = rows
            .column_value::<Option<Cursor<Vec<u8>>>>()
            .and_then(|bytes| Ok(bytes.map(|mut bytes| Message::unpack(&mut bytes)).transpose()?))?;
        let metadata = rows.column_value::<Option<MessageMetadata>>()?;
        Ok(Record::new((message, metadata)))
    }
}

impl Row for Record<MessageId> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        Ok(Record::new(MessageId::from_str(&rows.column_value::<String>()?)?))
    }
}

impl Row for Record<Option<MessageId>> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        Ok(Record::new(rows.column_value::<Option<String>>().and_then(|id| {
            Ok(id.map(|id| MessageId::from_str(&id)).transpose()?)
        })?))
    }
}

impl Row for Record<(TransactionId, u16)> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let transaction_id = TransactionId::from_str(&rows.column_value::<String>()?)?;
        let index = rows.column_value::<u16>()?;
        Ok(Record::new((transaction_id, index)))
    }
}

impl Row for Record<(MessageId, TransactionData, Option<LedgerInclusionState>)> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let message_id = MessageId::from_str(&rows.column_value::<String>()?)?;
        let data = rows.column_value::<TransactionData>()?;
        let inclusion_state = rows.column_value::<Option<LedgerInclusionState>>()?;
        Ok(Record::new((message_id, data, inclusion_state)))
    }
}

impl Row
    for Record<(
        MessageId,
        TransactionData,
        u16,
        Option<LedgerInclusionState>,
        Option<MilestoneIndex>,
    )>
{
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let message_id = MessageId::from_str(&rows.column_value::<String>()?)?;
        let data = rows.column_value::<TransactionData>()?;
        let idx = rows.column_value::<u16>()?;
        let inclusion_state = rows.column_value::<Option<LedgerInclusionState>>()?;
        let milestone_index = rows.column_value::<Option<u32>>()?;
        Ok(Record::new((
            message_id,
            data,
            idx,
            inclusion_state,
            milestone_index.map(MilestoneIndex),
        )))
    }
}

impl Row for Record<(MessageId, u64)> {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let message_id = MessageId::from_str(&rows.column_value::<String>()?)?;
        let timestamp = rows.column_value::<u64>()?;
        Ok(Record::new((message_id, timestamp)))
    }
}

impl Row for Record<(u32, u16)> {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self> {
        Ok(Record::new((rows.column_value::<u32>()?, rows.column_value::<u16>()?)))
    }
}

impl Row for Record<(PartitionId, MilestoneIndex, MessageId, Option<LedgerInclusionState>)> {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self> {
        let partition_id = rows.column_value::<PartitionId>()?;
        let milestone_index = rows.column_value::<u32>()?;
        let message_id = MessageId::from_str(&rows.column_value::<String>()?)?;
        let inclusion_state = rows.column_value::<Option<LedgerInclusionState>>()?;
        Ok(Record::new((
            partition_id,
            MilestoneIndex(milestone_index),
            message_id,
            inclusion_state,
        )))
    }
}

impl Row
    for Record<(
        PartitionId,
        MilestoneIndex,
        OutputType,
        TransactionId,
        Index,
        Amount,
        Option<LedgerInclusionState>,
    )>
{
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self> {
        let partition_id = rows.column_value::<PartitionId>()?;
        let milestone_index = rows.column_value::<u32>()?;
        let output_type = rows.column_value::<OutputType>()?;
        let transaction_id = TransactionId::from_str(&rows.column_value::<String>()?)?;
        let index = rows.column_value::<u16>()?;
        let amount = rows.column_value::<Amount>()?;
        let inclusion_state = rows.column_value::<Option<LedgerInclusionState>>()?;
        Ok(Record::new((
            partition_id,
            MilestoneIndex(milestone_index),
            output_type,
            transaction_id,
            index,
            amount,
            inclusion_state,
        )))
    }
}

impl Row for SyncRecord {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let milestone_index = MilestoneIndex(rows.column_value::<u32>()?);
        let synced_by = rows.column_value::<Option<u8>>()?;
        let logged_by = rows.column_value::<Option<u8>>()?;
        Ok(SyncRecord::new(milestone_index, synced_by, logged_by))
    }
}

impl Row for AnalyticRecord {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let milestone_index = MilestoneIndex(rows.column_value::<u32>()?);
        let message_count = MessageCount(rows.column_value::<u32>()?);
        let transaction_count = TransactionCount(rows.column_value::<u32>()?);
        let transferred_tokens = TransferredTokens(rows.column_value::<u64>()?);
        Ok(AnalyticRecord::new(
            milestone_index,
            message_count,
            transaction_count,
            transferred_tokens,
        ))
    }
}
