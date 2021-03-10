use super::*;

impl Select<MessageId, (Option<Message>, Option<MessageMetadata>)> for Mainnet {
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

impl RowsDecoder<MessageId, (Option<Message>, Option<MessageMetadata>)> for Mainnet {
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
        let message: Option<Message>;
        if let Some(mut message_bytes) = rows.column_value::<Option<Cursor<Vec<u8>>>>() {
            message = Some(Message::unpack(&mut message_bytes).unwrap());
        } else {
            message = None;
        };
        let metadata = rows.column_value::<Option<MessageMetadata>>();
        Record::new((message, metadata))
    }
}

/// Select only the message
impl Select<MessageId, Message> for Mainnet {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref())
    }
}

impl RowsDecoder<MessageId, Message> for Mainnet {
    type Row = Record<Option<Message>>;
    fn try_decode(decoder: Decoder) -> Result<Option<Message>, CqlError> {
        if decoder.is_rows() {
            if let Some(row) = Self::Row::rows_iter(decoder).next() {
                Ok(row.into_inner())
            } else {
                Ok(None)
            }
        } else {
            return Err(decoder.get_error());
        }
    }
}
impl Row for Record<Option<Message>> {
    fn decode_row<T: ColumnValue>(rows: &mut T) -> Self {
        let message: Option<Message>;
        if let Some(mut message_bytes) = rows.column_value::<Option<Cursor<Vec<u8>>>>() {
            message = Some(Message::unpack(&mut message_bytes).unwrap());
        } else {
            message = None;
        };
        Record::new(message)
    }
}

/// Select only the messagemetadata
impl Select<MessageId, MessageMetadata> for Mainnet {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT metadata FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.as_ref())
    }
}

impl RowsDecoder<MessageId, MessageMetadata> for Mainnet {
    type Row = Option<MessageMetadata>;
    fn try_decode(decoder: Decoder) -> Result<Option<MessageMetadata>, CqlError> {
        if decoder.is_rows() {
            if let Some(row) = Self::Row::rows_iter(decoder).next() {
                Ok(row)
            } else {
                Ok(None)
            }
        } else {
            return Err(decoder.get_error());
        }
    }
}
