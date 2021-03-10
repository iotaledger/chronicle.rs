use super::*;

impl Select<MessageId, Message> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message FROM {}.messages WHERE key = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref())
    }
}

impl RowsDecoder<MessageId, Message> for PermanodeKeyspace {
    type Row = Row<Message>;
    fn try_decode(decoder: Decoder) -> Result<Option<Message>, CqlError> {
        if decoder.is_rows() {
            Ok(Self::Row::rows_iter(decoder).next().map(|row| row.into_inner()))
        } else {
            return Err(decoder.get_error());
        }
    }
}

impl scylla_cql::Row for Row<Message> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Row::new(Message::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap())
    }
}

impl Select<MessageId, MessageMetadata> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT metadata FROM {}.messages WHERE key = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref())
    }
}

impl RowsDecoder<MessageId, MessageMetadata> for PermanodeKeyspace {
    type Row = Row<MessageMetadata>;
    fn try_decode(decoder: Decoder) -> Result<Option<MessageMetadata>, CqlError> {
        if decoder.is_rows() {
            Ok(Self::Row::rows_iter(decoder).next().map(|row| row.into_inner()))
        } else {
            return Err(decoder.get_error());
        }
    }
}

impl scylla_cql::Row for Row<MessageMetadata> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Row::new(rows.column_value::<MessageMetadata>())
    }
}

impl Select<MessageId, (Option<Message>, Option<MessageMetadata>)> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message, metadata FROM {}.messages WHERE key = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref())
    }
}

impl RowsDecoder<MessageId, (Option<Message>, Option<MessageMetadata>)> for PermanodeKeyspace {
    type Row = Row<(Option<Message>, Option<MessageMetadata>)>;
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

impl scylla_cql::Row for Row<(Option<Message>, Option<MessageMetadata>)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message = rows
            .column_value::<Option<Cursor<Vec<u8>>>>()
            .as_mut()
            .map(|bytes| Message::unpack(bytes).unwrap());
        let metadata = rows.column_value::<Option<MessageMetadata>>();
        Row::new((message, metadata))
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
    type Row = Row<MessageId>;
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

impl scylla_cql::Row for Row<MessageId> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        Row::new(MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap())
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
    type Row = Row<MessageId>;
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
    type Row = Row<(TransactionId, u16)>;
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

impl scylla_cql::Row for Row<(TransactionId, u16)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let transaction_id = TransactionId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let index = rows.column_value::<u16>();
        Row::new((transaction_id, index))
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
    type Row = Row<(MessageId, TransactionData)>;
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

impl scylla_cql::Row for Row<(MessageId, TransactionData)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let data = rows.column_value::<TransactionData>();
        Row::new((message_id, data))
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
    type Row = Row<(MessageId, u64)>;
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

impl scylla_cql::Row for Row<(MessageId, u64)> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message_id = MessageId::unpack(&mut rows.column_value::<Cursor<Vec<u8>>>()).unwrap();
        let timestamp = rows.column_value::<u64>();
        Row::new((message_id, timestamp))
    }
}
