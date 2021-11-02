// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

/// Delete Address record from addresses table
impl Delete<Bee<Ed25519Address>, (Partition, u8, Bee<TransactionId>, u16), AddressRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "DELETE FROM {}.addresses WHERE address = ? AND partition_id = ? AND milestone_index = ? AND output_type = ? AND transaction_id = ? AND idx = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<B: Binder>(
        builder: B,
        address: &Bee<Ed25519Address>,
        (partition, output_type, transaction_id, index): &(Partition, u8, Bee<TransactionId>, u16),
    ) -> B {
        builder
            .value(address)
            .value(partition.id())
            .value(partition.milestone_index())
            .value(output_type)
            .value(transaction_id)
            .value(index)
    }
}

/// Delete Index record from Indexes table
impl Delete<Indexation, (Partition, Bee<MessageId>), IndexationRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "DELETE FROM {}.indexes WHERE indexation = ? AND partition_id = ? AND milestone_index = ? AND message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<B: Binder>(
        builder: B,
        indexation: &Indexation,
        (partition, message_id): &(Partition, Bee<MessageId>),
    ) -> B {
        builder
            .value(&indexation.0)
            .value(partition.id())
            .value(partition.milestone_index())
            .value(message_id)
    }
}

/// Delete Parent record from Parents table
impl Delete<Bee<MessageId>, (Partition, Bee<MessageId>), ParentRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "DELETE FROM {}.parents WHERE parent_id = ? AND partition_id = ? AND milestone_index = ? AND message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<B: Binder>(
        builder: B,
        parent_id: &Bee<MessageId>,
        (partition, message_id): &(Partition, Bee<MessageId>),
    ) -> B {
        builder
            .value(parent_id)
            .value(partition.id())
            .value(partition.milestone_index())
            .value(message_id)
    }
}
