use super::*;
use scylla_cql::Row;

impl Select<MessageId, Message> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref())
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
        builder.value(&message_id.as_ref())
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
        builder.value(&message_id.as_ref())
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

impl Select<Partitioned<MessageId>, Vec<(MessageId, MilestoneIndex)>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id FROM {}.parents WHERE parent_id = ? AND partition_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &Partitioned<MessageId>) -> T::Return {
        builder.value(&message_id.as_ref()).value(&0u16)
    }
}

impl Select<Partitioned<HashedIndex>, Vec<(MessageId, MilestoneIndex)>> for PermanodeKeyspace {
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
    fn bind_values<T: Values>(builder: T, index: &Partitioned<HashedIndex>) -> T::Return {
        builder.value(&index.as_ref()).value(&index.partition_id())
    }
}

impl<K> RowsDecoder<Partitioned<K>, Vec<(MessageId, MilestoneIndex)>> for PermanodeKeyspace {
    type Row = Record<(MessageId, MilestoneIndex)>;
    fn try_decode(decoder: Decoder) -> Result<Option<Vec<(MessageId, MilestoneIndex)>>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(
                Self::Row::rows_iter(decoder).map(|row| row.into_inner()).collect(),
            ))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Select<Partitioned<Ed25519Address>, Vec<(OutputId, MilestoneIndex)>> for PermanodeKeyspace {
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
        builder.value(&address.as_ref()).value(&address.partition_id())
    }
}

impl RowsDecoder<Partitioned<Ed25519Address>, Vec<(OutputId, MilestoneIndex)>> for PermanodeKeyspace {
    type Row = Record<(TransactionId, u16, MilestoneIndex)>;
    fn try_decode(decoder: Decoder) -> Result<Option<Vec<(OutputId, MilestoneIndex)>>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(
                Self::Row::rows_iter(decoder)
                    .map(|row| (OutputId::new(row.0, row.1).unwrap(), row.2))
                    .collect(),
            ))
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
            .value(&output_id.transaction_id().as_ref())
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
                        inclusion_state: inclusion_state,
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

impl Select<HashedIndex, Vec<(MilestoneIndex, PartitionId)>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index FROM {}.hints WHERE hint = ? AND variant = ?",
            self.name()
        )
        .into()
    }

    fn bind_values<T: Values>(builder: T, index: &HashedIndex) -> T::Return {
        builder.value(&index.as_ref()).value(&"index")
    }
}

impl Select<Ed25519Address, Vec<(MilestoneIndex, PartitionId)>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index FROM {}.hints WHERE hint = ? AND variant = ?",
            self.name()
        )
        .into()
    }

    fn bind_values<T: Values>(builder: T, address: &Ed25519Address) -> T::Return {
        builder.value(&address.as_ref()).value(&"address")
    }
}

impl Select<MessageId, Vec<(MilestoneIndex, PartitionId)>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index FROM {}.hints WHERE hint = ? AND variant = ?",
            self.name()
        )
        .into()
    }

    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref()).value(&"parent")
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
        Record::new(MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap())
    }
}

impl Row for Record<(TransactionId, u16)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let transaction_id = TransactionId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let index = rows.column_value::<u16>();
        Record::new((transaction_id, index))
    }
}

impl Row for Record<(MessageId, TransactionData, Option<LedgerInclusionState>)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let data = rows.column_value::<TransactionData>();
        let inclusion_state = rows.column_value::<Option<LedgerInclusionState>>();
        Record::new((message_id, data, inclusion_state))
    }
}

impl Row for Record<(MessageId, u64)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
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
        let message_id = MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let index = rows.column_value::<u32>();
        Record::new((message_id, MilestoneIndex(index)))
    }
}

impl Row for Record<(TransactionId, u16, MilestoneIndex)> {
    fn decode_row<R: Rows + ColumnValue>(rows: &mut R) -> Self {
        let transaction_id = TransactionId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let index = rows.column_value::<u16>();
        let milestone_index = rows.column_value::<u32>();
        Record::new((transaction_id, index, MilestoneIndex(milestone_index)))
    }
}
