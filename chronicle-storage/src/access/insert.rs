// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use bee_message::Message;

impl Insert<Bee<MessageId>, Message> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.messages (message_id, message) VALUES (?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, message: &Message) -> B {
        let mut message_bytes = Vec::new();
        message
            .pack(&mut message_bytes)
            .expect("Error occurred packing Message");
        builder.value(message_id).value(&message_bytes.as_slice())
    }
}
/// Insert Metadata
impl Insert<Bee<MessageId>, MessageMetadata> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.messages (message_id, metadata) VALUES (?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, meta: &MessageMetadata) -> B {
        // Encode metadata using bincode
        builder.value(message_id).value(meta)
    }
}

impl Insert<Bee<MessageId>, (Message, MessageMetadata)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.messages (message_id, message, metadata) VALUES (?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        message_id: &Bee<MessageId>,
        (message, meta): &(Message, MessageMetadata),
    ) -> B {
        // Encode the message bytes as
        let mut message_bytes = Vec::new();
        message
            .pack(&mut message_bytes)
            .expect("Error occurred packing Message");
        builder.value(message_id).value(&message_bytes.as_slice()).value(meta)
    }
}
/// Insert Address into addresses table
impl Insert<Partitioned<Bee<Ed25519Address>>, AddressRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.addresses (address, partition_id, milestone_index, output_type, transaction_id, idx, amount, address_type, inclusion_state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        Partitioned { inner, partition_id }: &Partitioned<Bee<Ed25519Address>>,
        AddressRecord {
            milestone_index,
            transaction_id,
            index,
            amount,
            ledger_inclusion_state,
            output_type,
        }: &AddressRecord,
    ) -> B {
        builder
            .value(inner)
            .value(partition_id)
            .value(Bee(milestone_index))
            .value(output_type)
            .value(Bee(transaction_id))
            .value(index)
            .value(amount)
            .value(&Ed25519Address::KIND)
            .value(ledger_inclusion_state)
    }
}

/// Insert Index into Indexes table
impl Insert<Partitioned<Indexation>, IndexationRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.indexes (indexation, partition_id, milestone_index, message_id, inclusion_state)
            VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        Partitioned { inner, partition_id }: &Partitioned<Indexation>,
        IndexationRecord {
            milestone_index,
            message_id,
            ledger_inclusion_state,
        }: &IndexationRecord,
    ) -> B {
        builder
            .value(&inner.0)
            .value(partition_id)
            .value(Bee(milestone_index))
            .value(Bee(message_id))
            .value(ledger_inclusion_state)
    }
}

/// Insert ParentId into Parents table
impl Insert<Partitioned<Bee<MessageId>>, ParentRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.parents (parent_id, partition_id, milestone_index, message_id, inclusion_state)
            VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        Partitioned { inner, partition_id }: &Partitioned<Bee<MessageId>>,
        ParentRecord {
            milestone_index,
            message_id,
            ledger_inclusion_state,
        }: &ParentRecord,
    ) -> B {
        builder
            .value(inner)
            .value(partition_id)
            .value(Bee(milestone_index))
            .value(Bee(message_id))
            .value(ledger_inclusion_state)
    }
}
/// Insert Transaction into Transactions table
/// Note: This can be used to store:
/// -input variant: (InputTransactionId, InputIndex) -> UTXOInput data column
/// -output variant: (OutputTransactionId, OutputIndex) -> Output data column
/// -unlock variant: (UtxoInputTransactionId, UtxoInputOutputIndex) -> Unlock data column
impl Insert<Bee<TransactionId>, TransactionRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.transactions (transaction_id, idx, variant, message_id, data, inclusion_state, milestone_index)
            VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        transaction_id: &Bee<TransactionId>,
        transaction_record: &TransactionRecord,
    ) -> B {
        let milestone_index;
        if let Some(ms) = transaction_record.milestone_index {
            milestone_index = Some(ms.0);
        } else {
            milestone_index = None;
        }
        builder
            .value(transaction_id)
            .value(transaction_record.idx)
            .value(&transaction_record.variant)
            .value(&Bee(transaction_record.message_id))
            .value(&transaction_record.data)
            .value(&transaction_record.inclusion_state)
            .value(&milestone_index)
    }
}

/// Insert Hint into Hints table
impl Insert<Hint, Partition> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.hints (hint, variant, partition_id, milestone_index) VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, hint: &Hint, partition: &Partition) -> B {
        builder
            .value(&hint.hint)
            .value(&hint.variant.to_string())
            .value(partition.id())
            .value(partition.milestone_index())
    }
}

impl Insert<Bee<MilestoneIndex>, (Bee<MessageId>, Box<MilestonePayload>)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.milestones (milestone_index, message_id, timestamp, payload) VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        milestone_index: &Bee<MilestoneIndex>,
        (message_id, milestone_payload): &(Bee<MessageId>, Box<MilestonePayload>),
    ) -> B {
        let mut milestone_payload_bytes = Vec::new();
        milestone_payload
            .pack(&mut milestone_payload_bytes)
            .expect("Error occurred packing MilestonePayload");
        builder
            .value(&milestone_index.0 .0)
            .value(message_id)
            .value(&milestone_payload.essence().timestamp())
            .value(&milestone_payload_bytes.as_slice())
    }
}

impl Insert<String, SyncRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.sync (key, milestone_index, synced_by, logged_by) VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        keyspace: &String,
        SyncRecord {
            milestone_index,
            synced_by,
            logged_by,
        }: &SyncRecord,
    ) -> B {
        builder
            .value(keyspace)
            .value(&milestone_index.0)
            .value(synced_by)
            .value(logged_by)
    }
}

impl Insert<String, AnalyticRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.analytics (key, milestone_index, message_count, transaction_count, transferred_tokens) VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        keyspace: &String,
        AnalyticRecord {
            milestone_index,
            message_count,
            transaction_count,
            transferred_tokens,
        }: &AnalyticRecord,
    ) -> B {
        builder
            .value(keyspace)
            .value(&milestone_index.0)
            .value(&message_count.0)
            .value(&transaction_count.0)
            .value(&transferred_tokens.0)
    }
}
