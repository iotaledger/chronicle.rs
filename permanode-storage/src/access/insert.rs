use super::*;

impl Insert<MessageId, Message> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, message) VALUES (?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId, message: &Message) -> T::Return {
        let mut message_bytes = Vec::new();
        message
            .pack(&mut message_bytes)
            .expect("Error occurred packing Message");
        builder.value(&message_id.as_ref()).value(&message_bytes)
    }
}

impl Insert<MessageId, (Message, MessageMetadata)> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, message, metadata) VALUES (?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        message_id: &MessageId,
        (message, meta): &(Message, MessageMetadata),
    ) -> T::Return {
        // Encode the message bytes as
        let mut message_bytes = Vec::new();
        message
            .pack(&mut message_bytes)
            .expect("Error occurred packing Message");
        // Encode metadata using bincode
        let encoded: Vec<u8> = bincode_config().serialize(&meta).unwrap();
        builder
            .value(&message_id.as_ref())
            .value(&message_bytes)
            .value(&encoded.as_slice())
    }
}
