use super::*;
use scylla_cql::Row;
use std::str::FromStr;

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
    fn try_decode(decoder: Decoder) -> Result<Option<Message>, CqlError> {
        if decoder.is_rows() {
            Ok(Self::Row::rows_iter(decoder)
                .next()
                .map(|row| row.into_inner())
                .flatten())
        } else {
            return Err(decoder.get_error());
        }
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
    fn try_decode(decoder: Decoder) -> Result<Option<MessageMetadata>, CqlError> {
        if decoder.is_rows() {
            Ok(Self::Row::rows_iter(decoder)
                .next()
                .map(|row| row.into_inner())
                .flatten())
        } else {
            return Err(decoder.get_error());
        }
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
    fn try_decode(decoder: Decoder) -> Result<Option<(Option<Message>, Option<MessageMetadata>)>, CqlError> {
        if decoder.is_rows() {
            if let Some(row) = Self::Row::rows_iter(decoder).next() {
                let row = row.into_inner();
                Ok(Some((row.0, row.1)))
            } else {
                Ok(None)
            }
        } else {
            return Err(decoder.get_error());
        }
    }
}

impl Select<Partitioned<MessageId>, Paged<Vec<(MessageId, MilestoneIndex)>>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, milestone_index
            FROM {}.parents
            WHERE parent_id = ? AND partition_id = ?
            ORDER BY milestone_index DESC",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &Partitioned<MessageId>) -> T::Return {
        builder.value(&message_id.to_string()).value(&message_id.partition_id())
    }
}

impl Select<Partitioned<Indexation>, Paged<Vec<(MessageId, MilestoneIndex)>>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, milestone_index
            FROM {}.indexes
            WHERE hashed_index = ? AND partition_id = ?
            ORDER BY milestone_index DESC",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, index: &Partitioned<Indexation>) -> T::Return {
        builder.value(&index.0).value(&index.partition_id())
    }
}

impl<K> RowsDecoder<Partitioned<K>, Paged<Vec<(MessageId, MilestoneIndex)>>> for PermanodeKeyspace {
    type Row = Record<(MessageId, MilestoneIndex)>;
    fn try_decode(decoder: Decoder) -> Result<Option<Paged<Vec<(MessageId, MilestoneIndex)>>>, CqlError> {
        if decoder.is_rows() {
            let mut iter = Self::Row::rows_iter(decoder);
            let paging_state = iter.take_paging_state();
            let values = iter.map(|row| row.into_inner()).collect();
            Ok(Some(Paged::new(values, paging_state)))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Select<Partitioned<Ed25519Address>, Paged<Vec<(OutputId, MilestoneIndex)>>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT transaction_id, idx
            FROM {}.addresses
            WHERE address = ? AND address_type = 0 AND partition_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, address: &Partitioned<Ed25519Address>) -> T::Return {
        builder.value(&address.to_string()).value(&address.partition_id())
    }
}

impl RowsDecoder<Partitioned<Ed25519Address>, Paged<Vec<(OutputId, MilestoneIndex)>>> for PermanodeKeyspace {
    type Row = Record<(TransactionId, u16, MilestoneIndex)>;
    fn try_decode(decoder: Decoder) -> Result<Option<Paged<Vec<(OutputId, MilestoneIndex)>>>, CqlError> {
        if decoder.is_rows() {
            let mut iter = Self::Row::rows_iter(decoder);
            let paging_state = iter.take_paging_state();
            let values = iter.map(|row| (OutputId::new(row.0, row.1).unwrap(), row.2)).collect();
            Ok(Some(Paged::new(values, paging_state)))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Select<OutputId, OutputData> for PermanodeKeyspace {
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

impl RowsDecoder<OutputId, OutputData> for PermanodeKeyspace {
    type Row = Record<(MessageId, TransactionData, Option<LedgerInclusionState>)>;
    fn try_decode(decoder: Decoder) -> Result<Option<OutputData>, CqlError> {
        if decoder.is_rows() {
            let mut unlock_blocks = Vec::new();
            let mut output = None;
            for (message_id, transaction_data, inclusion_state) in
                Self::Row::rows_iter(decoder).map(|row| row.into_inner())
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
            Ok(output.map(|output| OutputData { output, unlock_blocks }))
        } else {
            Err(decoder.get_error())
        }
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
    fn try_decode(decoder: Decoder) -> Result<Option<Milestone>, CqlError> {
        if decoder.is_rows() {
            Ok(Self::Row::rows_iter(decoder)
                .next()
                .map(|row| Milestone::new(row.0, row.1)))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Select<Hint, Vec<(MilestoneIndex, PartitionId)>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT milestone_index, partition_id FROM {}.hints WHERE hint = ? AND variant = ?",
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

    fn try_decode(decoder: Decoder) -> Result<Option<Vec<(MilestoneIndex, PartitionId)>>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(
                Self::Row::rows_iter(decoder)
                    .map(|row| {
                        let (index, partition_id) = row.into_inner();
                        (MilestoneIndex(index), partition_id)
                    })
                    .collect(),
            ))
        } else {
            Err(decoder.get_error())
        }
    }
}

// ###############
// ROW DEFINITIONS
// ###############

impl Row for Record<Option<Message>> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Record::new(
            rows.column_value::<Option<Cursor<Vec<u8>>>>()
                .and_then(|mut bytes| Message::unpack(&mut bytes).ok()),
        )
    }
}

impl Row for Record<Option<MessageMetadata>> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Record::new(rows.column_value::<Option<MessageMetadata>>())
    }
}

impl Row for Record<(Option<Message>, Option<MessageMetadata>)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message = rows
            .column_value::<Option<Cursor<Vec<u8>>>>()
            .as_mut()
            .map(|bytes| Message::unpack(bytes).unwrap());
        let metadata = rows.column_value::<Option<MessageMetadata>>();
        Record::new((message, metadata))
    }
}

impl Row for Record<MessageId> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Record::new(MessageId::from_str(&rows.column_value::<String>()).unwrap())
    }
}

impl Row for Record<(TransactionId, u16)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let transaction_id = TransactionId::from_str(&rows.column_value::<String>()).unwrap();
        let index = rows.column_value::<u16>();
        Record::new((transaction_id, index))
    }
}

impl Row for Record<(MessageId, TransactionData, Option<LedgerInclusionState>)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::from_str(&rows.column_value::<String>()).unwrap();
        let data = rows.column_value::<TransactionData>();
        let inclusion_state = rows.column_value::<Option<LedgerInclusionState>>();
        Record::new((message_id, data, inclusion_state))
    }
}

impl Row for Record<(MessageId, u64)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::from_str(&rows.column_value::<String>()).unwrap();
        let timestamp = rows.column_value::<u64>();
        Record::new((message_id, timestamp))
    }
}

impl Row for Record<(u32, u16)> {
    fn decode_row<R: Rows + ColumnValue>(rows: &mut R) -> Self {
        Record::new((rows.column_value::<u32>(), rows.column_value::<u16>()))
    }
}

impl Row for Record<(MessageId, MilestoneIndex)> {
    fn decode_row<R: Rows + ColumnValue>(rows: &mut R) -> Self {
        let message_id = MessageId::from_str(&rows.column_value::<String>()).unwrap();
        let index = rows.column_value::<u32>();
        Record::new((message_id, MilestoneIndex(index)))
    }
}

impl Row for Record<(TransactionId, u16, MilestoneIndex)> {
    fn decode_row<R: Rows + ColumnValue>(rows: &mut R) -> Self {
        let transaction_id = TransactionId::from_str(&rows.column_value::<String>()).unwrap();
        let index = rows.column_value::<u16>();
        let milestone_index = rows.column_value::<u32>();
        Record::new((transaction_id, index, MilestoneIndex(milestone_index)))
    }
}
