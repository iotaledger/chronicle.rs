// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

/// Delete Address record from addresses table
impl Delete<(Bee<Ed25519Address>, PartitionId), (Bee<MilestoneIndex>, u8, Bee<TransactionId>, Index), AddressRecord>
    for ChronicleKeyspace
{
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
        (address, partition_id): &(Bee<Ed25519Address>, PartitionId),
        (milestone_index, output_type, transaction_id, index): &(Bee<MilestoneIndex>, u8, Bee<TransactionId>, Index),
    ) -> B {
        builder
            .value(address)
            .value(partition_id)
            .value(milestone_index)
            .value(output_type)
            .value(transaction_id)
            .value(index)
    }
}

/// Delete Index record from Indexes table
impl Delete<(Indexation, PartitionId), (Bee<MilestoneIndex>, Bee<MessageId>), IndexationRecord> for ChronicleKeyspace {
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
        (indexation, partition_id): &(Indexation, PartitionId),
        (milestone_index, message_id): &(Bee<MilestoneIndex>, Bee<MessageId>),
    ) -> B {
        builder
            .value(&indexation.0)
            .value(partition_id)
            .value(milestone_index)
            .value(message_id)
    }
}

/// Delete Parent record from Parents table
impl Delete<(Bee<MessageId>, PartitionId), (Bee<MilestoneIndex>, Bee<MessageId>), ParentRecord> for ChronicleKeyspace {
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
        (parent_id, partition_id): &(Bee<MessageId>, PartitionId),
        (milestone_index, message_id): &(Bee<MilestoneIndex>, Bee<MessageId>),
    ) -> B {
        builder
            .value(parent_id)
            .value(partition_id)
            .value(milestone_index)
            .value(message_id)
    }
}
