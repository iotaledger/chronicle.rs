// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

/////////////////// Messages tables ////////////////////////////
impl Insert<Bee<MessageId>, MessageRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.messages (message_id, message, version, milestone_index, inclusion_state, conflict_reason, proof) VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, message: &MessageRecord) -> B {
        builder
            .value(message_id)
            .value(Bee(message.message()))
            .value(message.version())
            .value(message.milestone_index().and_then(|m| Some(m.0)))
            .value(message.inclusion_state())
            .value(message.conflict_reason())
            .value(message.proof())
    }
}

impl Insert<Bee<MessageId>, Proof> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!("INSERT INTO #.messages (message_id, proof) VALUES (?, ?)", self.name())
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, proof: &Proof) -> B {
        builder.value(message_id).value(proof)
    }
}

/////////////////// Parents tables ////////////////////////////
impl Insert<Bee<MessageId>, ParentRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.parents (parent_id, milestone_index, ms_timestamp, message_id, inclusion_state)
            VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        parent_id: &Bee<MessageId>,
        ParentRecord {
            milestone_index,
            ms_timestamp,
            message_id,
            ledger_inclusion_state,
        }: &ParentRecord,
    ) -> B {
        builder
            .value(parent_id)
            .value(milestone_index.and_then(|m| Some(m.0)))
            .value(&ms_timestamp)
            .value(Bee(message_id))
            .value(ledger_inclusion_state)
    }
}

////////////////////// Partition Hint table /////////////////////////
impl Insert<Hint, MsRangeId> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.hints (variant, key, ms_range_id) VALUES (?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, hint: &Hint, ms_range_id: &MsRangeId) -> B {
        builder.value(&hint.variant.to_string()).value(&ms_range_id)
    }
}

///////////////////// Sync table ///////////////////////////
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
////////////////////////////// Transactions table /////////////////////////////
/// Insert Transaction into Transactions table
/// Note: This can be used to store:
/// -input variant: (InputTransactionId, InputIndex) -> UTXOInput data column
/// -output variant: (OutputTransactionId, OutputIndex) -> Output data column
/// -unlock variant: (UtxoInputTransactionId, UtxoInputOutputIndex) -> Unlock data column
impl Insert<Bee<TransactionId>, TransactionRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.transactions (transaction_id, idx, variant, message_id, version, data, inclusion_state, milestone_index)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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

////////////////////// Outputs tables ////////////////////////////
/////////// Legacy output table /////////////

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
