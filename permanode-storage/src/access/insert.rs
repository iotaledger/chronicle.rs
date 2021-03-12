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
        builder.value(&message_id.as_ref()).value(&message_bytes.as_slice())
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
            "INSERT INTO {}.parents (parent_id, partition_id, parent_index, message_id) VALUES (?, ?, ?, ?)",
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
/// Insert Transaction into Transactions table
/// Note: This can be used to store:
/// -input variant: (InputTransactionId, InputIndex) -> UTXOInput data column
/// -output variant: (OutputTransactionId, OutputIndex) -> Output data column
/// -unlock variant: (UtxoInputTransactionId, UtxoInputOutputIndex) -> Unlock data column
impl Insert<(TransactionId, Index), TransactionRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.transactions (transaction_id, idx, variant, ref_transaction_id, ref_idx, message_id, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        (transaction_id, index): &(TransactionId, Index),
        transaction_record: &TransactionRecord,
    ) -> T::Return {
        builder
            .value(&transaction_id.as_ref())
            .value(index)
            .value(&transaction_record.variant)
            .value(&transaction_id.as_ref())
            .value(index)
            .value(&transaction_record.message_id.as_ref())
            .value(&transaction_record.data)
    }
}

/// Insert Output into Transactions table
impl Insert<OutputId, TransactionRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.transactions (transaction_id, idx, variant, ref_transaction_id, ref_idx, message_id, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, output_id: &OutputId, transaction_record: &TransactionRecord) -> T::Return {
        if let TransactionData::Output(_) = &transaction_record.data {
            builder
                .value(&output_id.transaction_id().as_ref())
                .value(&output_id.index())
                .value(&transaction_record.variant)
                .value(&output_id.transaction_id().as_ref())
                .value(&output_id.index())
                .value(&transaction_record.message_id.as_ref())
                .value(&transaction_record.data)
        } else {
            panic!("Provided invalid TransactionData for an output")
        }
    }
}

/// Insert Hint into Hints table
impl<H: HintVariant> Insert<Hint<H>, Partition> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.hints (hint, variant, partition_id, milestone_index) VALUES (?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, hint: &Hint<H>, partition: &Partition) -> T::Return {
        builder
            .value(&hint.get_inner().as_bytes())
            .value(&H::variant())
            .value(partition.id())
            .value(partition.milestone_index())
    }
}
