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

impl Row for Record<Option<Message>> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Record::new(
            rows.column_value::<Option<Cursor<Vec<u8>>>>()
                .and_then(|mut bytes| Message::unpack(&mut bytes).ok()),
        )
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

impl Row for Record<Option<MessageMetadata>> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Record::new(rows.column_value::<Option<MessageMetadata>>())
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

impl Select<MessageId, MessageChildren> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id FROM {}.parents WHERE parent_id = ? AND partition_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref()).value(&0u16)
    }
}

impl RowsDecoder<MessageId, MessageChildren> for PermanodeKeyspace {
    type Row = Record<MessageId>;
    fn try_decode(decoder: Decoder) -> Result<Option<MessageChildren>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(MessageChildren {
                children: Self::Row::rows_iter(decoder).map(|row| row.into_inner()).collect(),
            }))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Row for Record<MessageId> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Record::new(MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap())
    }
}

impl Select<HashedIndex, IndexMessages> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id from {}.indexes WHERE hashed_index = ? AND partition_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, hashed_index: &HashedIndex) -> T::Return {
        builder.value(&hashed_index.as_ref()).value(&0u16)
    }
}

impl RowsDecoder<HashedIndex, IndexMessages> for PermanodeKeyspace {
    type Row = Record<MessageId>;
    fn try_decode(decoder: Decoder) -> Result<Option<IndexMessages>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(IndexMessages {
                messages: Self::Row::rows_iter(decoder).map(|row| row.into_inner()).collect(),
            }))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Select<Ed25519Address, OutputIds> for PermanodeKeyspace {
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
    fn bind_values<T: Values>(builder: T, address: &Ed25519Address) -> T::Return {
        builder.value(&address.as_ref()).value(&0u16)
    }
}

impl RowsDecoder<Ed25519Address, OutputIds> for PermanodeKeyspace {
    type Row = Record<(TransactionId, u16)>;
    fn try_decode(decoder: Decoder) -> Result<Option<OutputIds>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(OutputIds {
                ids: Self::Row::rows_iter(decoder)
                    .map(|row| OutputId::new(row.0, row.1).unwrap())
                    .collect(),
            }))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Row for Record<(TransactionId, u16)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let transaction_id = TransactionId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let index = rows.column_value::<u16>();
        Record::new((transaction_id, index))
    }
}

impl Select<OutputId, Outputs> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data from {}.transactions WHERE transaction_id = ? AND idx = ? and variant = 'utxoinput'",
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

impl RowsDecoder<OutputId, Outputs> for PermanodeKeyspace {
    type Row = Record<(MessageId, TransactionData)>;
    fn try_decode(decoder: Decoder) -> Result<Option<Outputs>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(Outputs {
                outputs: Self::Row::rows_iter(decoder).map(|row| row.into_inner()).collect(),
            }))
        } else {
            Err(decoder.get_error())
        }
    }
}

impl Row for Record<(MessageId, TransactionData)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let data = rows.column_value::<TransactionData>();
        Record::new((message_id, data))
    }
}

impl Select<MilestoneIndex, Milestone> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, timestamp from {}.milestones WHERE milestone_index = ?",
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

impl Row for Record<(MessageId, u64)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let timestamp = rows.column_value::<u64>();
        Record::new((message_id, timestamp))
    }
}

/// Select Partitions from Hints table for a given Hint<H>
impl<H: HintVariant> Select<Hint<H>, Vec<Partition>> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index FROM {}.hints WHERE hint = ? AND variant = ? ",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, hint: &Hint<H>) -> T::Return {
        builder.value(&hint.get_inner().as_bytes()).value(&H::variant())
    }
}

impl<H: HintVariant> RowsDecoder<Hint<H>, Vec<Partition>> for PermanodeKeyspace {
    type Row = (u16, u32);
    fn try_decode(decoder: Decoder) -> Result<Option<Vec<Partition>>, CqlError> {
        if decoder.is_rows() {
            Ok(Some(
                Self::Row::rows_iter(decoder)
                    .map(|row| Partition::new(row.0, row.1))
                    .collect(),
            ))
        } else {
            Err(decoder.get_error())
        }
    }
}
