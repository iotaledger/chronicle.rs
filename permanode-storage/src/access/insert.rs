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
/// Insert Metadata
impl Insert<MessageId, MessageMetadata> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, metadata) VALUES (?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId, meta: &MessageMetadata) -> T::Return {
        // Encode metadata using bincode
        let encoded: Vec<u8> = bincode_config().serialize(&meta).unwrap();
        builder.value(&message_id.as_ref()).value(&encoded.as_slice())
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
/// Insert Address into addresses table
impl Insert<Partitioned<Ed25519Address>, AddressRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.addresses (address, partition_id, transaction_id, idx, amount, address_type) VALUES (?, ?, ?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Partitioned { inner, partition_id }: &Partitioned<Ed25519Address>,
        AddressRecord {
            transaction_id,
            index,
            amount,
            address_type,
        }: &AddressRecord,
    ) -> T::Return {
        builder
            .value(&inner.as_ref())
            .value(partition_id)
            .value(&transaction_id.as_ref())
            .value(index)
            .value(amount)
            .value(address_type)
    }
}

/// Insert Index into Indexes table
impl Insert<Partitioned<HashedIndex>, MessageId> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.indexes (hashed_index, partition_id, message_id) VALUES (?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Partitioned { inner, partition_id }: &Partitioned<HashedIndex>,
        message_id: &MessageId,
    ) -> T::Return {
        builder
            .value(&inner.as_ref())
            .value(partition_id)
            .value(&message_id.as_ref())
    }
}

/// Insert ParentId into Parents table
impl Insert<Partitioned<MessageId>, (ParentIndex, MessageId)> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.indexes (hashed_index, partition_id, message_id) VALUES (?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Partitioned { inner, partition_id }: &Partitioned<MessageId>,
        (parent_index, message_id): &(ParentIndex, MessageId),
    ) -> T::Return {
        builder
            .value(&inner.as_ref())
            .value(partition_id)
            .value(parent_index)
            .value(&message_id.as_ref())
    }
}

/// Insert Output into Transactions table
impl Insert<Partitioned<TransactionId>, (ParentIndex, MessageId)> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.indexes (hashed_index, partition_id, message_id) VALUES (?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Partitioned { inner, partition_id }: &Partitioned<TransactionId>,
        (parent_index, message_id): &(ParentIndex, MessageId),
    ) -> T::Return {
        builder
            .value(&inner.as_ref())
            .value(partition_id)
            .value(parent_index)
            .value(&message_id.as_ref())
    }
}
