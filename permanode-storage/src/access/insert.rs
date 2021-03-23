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
        builder.value(&message_id.to_string()).value(&message_bytes.as_slice())
    }
}
/// Insert Metadata
impl Insert<MessageId, MessageMetadataObj> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, metadata) VALUES (?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId, meta: &MessageMetadataObj) -> T::Return {
        // Encode metadata using bincode
        let encoded: Vec<u8> = bincode_config().serialize(&meta).unwrap();
        builder.value(&message_id.to_string()).value(&encoded.as_slice())
    }
}

impl Insert<MessageId, (Message, MessageMetadataObj)> for PermanodeKeyspace {
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
        (message, meta): &(Message, MessageMetadataObj),
    ) -> T::Return {
        // Encode the message bytes as
        let mut message_bytes = Vec::new();
        message
            .pack(&mut message_bytes)
            .expect("Error occurred packing Message");
        // Encode metadata using bincode
        let encoded: Vec<u8> = bincode_config().serialize(&meta).unwrap();
        builder
            .value(&message_id.to_string())
            .value(&message_bytes.as_slice())
            .value(&encoded.as_slice())
    }
}
/// Insert Address into addresses table
impl Insert<Partitioned<Ed25519Address>, AddressRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.addresses (address, partition_id, milestone_index, output_type, transaction_id, idx, amount, address_type, inclusion_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Partitioned { inner, partition }: &Partitioned<Ed25519Address>,
        &AddressRecord {
            transaction_id,
            index,
            amount,
            ledger_inclusion_state,
            output_type,
        }: &AddressRecord,
    ) -> T::Return {
        builder
            .value(&inner.to_string())
            .value(partition.id())
            .value(partition.milestone_index())
            .value(&output_type)
            .value(&transaction_id.to_string())
            .value(&index)
            .value(&amount)
            .value(&Ed25519Address::KIND)
            .value(&ledger_inclusion_state)
    }
}

/// Insert Index into Indexes table
impl Insert<Partitioned<Indexation>, IndexationRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.indexes (hashed_index, partition_id, milestone_index, message_id, inclusion_state) VALUES (?, ?, ?, ?,?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Partitioned { inner, partition }: &Partitioned<Indexation>,
        &IndexationRecord {
            message_id,
            ledger_inclusion_state,
        }: &IndexationRecord,
    ) -> T::Return {
        builder
            .value(&inner.0)
            .value(partition.id())
            .value(partition.milestone_index())
            .value(&message_id.to_string())
            .value(&ledger_inclusion_state)
    }
}

/// Insert ParentId into Parents table
impl Insert<Partitioned<MessageId>, ParentRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.parents (parent_id, partition_id, milestone_index, message_id, inclusion_state) VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Partitioned { inner, partition }: &Partitioned<MessageId>,
        ParentRecord {
            message_id,
            ledger_inclusion_state,
        }: &ParentRecord,
    ) -> T::Return {
        builder
            .value(&inner.to_string())
            .value(partition.id())
            .value(partition.milestone_index())
            .value(&message_id.to_string())
            .value(ledger_inclusion_state)
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
            "INSERT INTO {}.transactions (transaction_id, idx, variant, message_id, data, inclusion_state, milestone_index) VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        (transaction_id, index): &(TransactionId, Index),
        transaction_record: &TransactionRecord,
    ) -> T::Return {
        let milestone_index;
        if let Some(ms) = transaction_record.milestone_index {
            milestone_index = Some(ms.0);
        } else {
            milestone_index = None;
        }
        builder
            .value(&transaction_id.to_string())
            .value(index)
            .value(&transaction_record.variant)
            .value(&transaction_record.message_id.to_string())
            .value(&transaction_record.data)
            .value(&transaction_record.inclusion_state)
            .value(&milestone_index)
    }
}

/// Insert Output into Transactions table
impl Insert<OutputId, TransactionRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.transactions (transaction_id, idx, variant, message_id, data, inclusion_state, milestone_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, output_id: &OutputId, transaction_record: &TransactionRecord) -> T::Return {
        if let TransactionData::Output(_) = &transaction_record.data {
            let milestone_index;
            if let Some(ms) = transaction_record.milestone_index {
                milestone_index = Some(ms.0);
            } else {
                milestone_index = None;
            }
            builder
                .value(&output_id.transaction_id().to_string())
                .value(&output_id.index())
                .value(&transaction_record.variant)
                .value(&transaction_record.message_id.to_string())
                .value(&transaction_record.data)
                .value(&transaction_record.inclusion_state)
                .value(&milestone_index)
        } else {
            panic!("Provided invalid TransactionData for an output")
        }
    }
}

/// Insert Hint into Hints table
impl Insert<Hint, Partition> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.hints (hint, variant, partition_id, milestone_index) VALUES (?, ?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, hint: &Hint, partition: &Partition) -> T::Return {
        builder
            .value(&hint.hint)
            .value(&hint.variant.to_string())
            .value(partition.id())
            .value(partition.milestone_index())
    }
}

impl Insert<MilestoneIndex, (Milestone, MilestonePayload)> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.milestones (milestone_index, message_id, timestamp) VALUES (?, ?, ?)",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        milestone_index: &MilestoneIndex,
        (milestone, payload): &(Milestone, MilestonePayload),
    ) -> T::Return {
        builder
            .value(&milestone_index.0)
            .value(&milestone.message_id().to_string())
            .value(&milestone.timestamp())
        //.value(&payload.to_string())
    }
}
