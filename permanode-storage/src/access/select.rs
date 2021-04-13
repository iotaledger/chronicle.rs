use super::*;
use permanode_common::SyncRange;
use scylla_cql::Row;
use std::{
    collections::VecDeque,
    str::FromStr,
};

impl Select<MessageId, Message> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl RowsDecoder<MessageId, Message> for PermanodeKeyspace {
    type Row = Record<Option<Message>>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Message>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| row.into_inner())
            .flatten())
    }
}

impl Select<MessageId, MessageMetadata> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT metadata FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl RowsDecoder<MessageId, MessageMetadata> for PermanodeKeyspace {
    type Row = Record<Option<MessageMetadata>>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<MessageMetadata>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");

        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| row.into_inner())
            .flatten())
    }
}

impl Select<MessageId, (Option<Message>, Option<MessageMetadata>)> for PermanodeKeyspace {
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

impl RowsDecoder<MessageId, (Option<Message>, Option<MessageMetadata>)> for PermanodeKeyspace {
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

impl Select<Partitioned<MessageId>, Paged<VecDeque<Partitioned<ParentRecord>>>> for PermanodeKeyspace {
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

impl<K> RowsDecoder<Partitioned<K>, Paged<VecDeque<Partitioned<ParentRecord>>>> for PermanodeKeyspace {
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

impl Select<Partitioned<Indexation>, Paged<VecDeque<Partitioned<IndexationRecord>>>> for PermanodeKeyspace {
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

impl<K> RowsDecoder<Partitioned<K>, Paged<VecDeque<Partitioned<IndexationRecord>>>> for PermanodeKeyspace {
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

impl Select<Partitioned<Ed25519Address>, Paged<VecDeque<Partitioned<AddressRecord>>>> for PermanodeKeyspace {
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

impl RowsDecoder<Partitioned<Ed25519Address>, Paged<VecDeque<Partitioned<AddressRecord>>>> for PermanodeKeyspace {
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
        let values = iter
            .map(|row| {
                let (partition_id, milestone_index, output_type, transaction_id, index, amount, inclusion_state) =
                    row.into_inner();
                Partitioned::new(
                    AddressRecord::new(output_type, transaction_id, index, amount, inclusion_state),
                    partition_id,
                    milestone_index.0,
                )
            })
            .collect();
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<OutputId, OutputRes> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data, inclusion_state
            FROM {}.transactions
            WHERE transaction_id = ?
            AND idx = ?
            AND (variant = 'output' OR variant = 'unlock')",
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

impl RowsDecoder<OutputId, OutputRes> for PermanodeKeyspace {
    type Row = Record<(MessageId, TransactionData, Option<LedgerInclusionState>)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<OutputRes>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut unlock_blocks = Vec::new();
        let mut output = None;
        for (message_id, transaction_data, inclusion_state) in
            Self::Row::rows_iter(decoder)?.map(|row| row.into_inner())
        {
            match transaction_data {
                TransactionData::Output(o) => output = Some(CreatedOutput::new(message_id, o)),
                TransactionData::Unlock(u) => unlock_blocks.push(UnlockRes {
                    message_id,
                    block: u.unlock_block,
                    inclusion_state,
                }),
                _ => (),
            }
        }
        Ok(output.map(|output| OutputRes { output, unlock_blocks }))
    }
}

impl Select<TransactionId, MessageId> for PermanodeKeyspace {
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

impl RowsDecoder<TransactionId, MessageId> for PermanodeKeyspace {
    type Row = Record<Option<MessageId>>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<MessageId>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");

        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| row.into_inner())
            .flatten())
    }
}

impl Select<MilestoneIndex, Milestone> for PermanodeKeyspace {
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

impl RowsDecoder<MilestoneIndex, Milestone> for PermanodeKeyspace {
    type Row = Record<(MessageId, u64)>;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<Milestone>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        Ok(Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| Milestone::new(row.0, row.1)))
    }
}

impl Select<Hint, Vec<(MilestoneIndex, PartitionId)>> for PermanodeKeyspace {
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

impl<K> RowsDecoder<K, Vec<(MilestoneIndex, PartitionId)>> for PermanodeKeyspace {
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

impl Select<SyncRange, Iter<SyncRecord>> for PermanodeKeyspace {
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

impl RowsDecoder<SyncRange, Iter<SyncRecord>> for PermanodeKeyspace {
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
