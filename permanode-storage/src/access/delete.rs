use super::*;

#[derive(Clone)]
pub struct Ed25519AddressPK {
    address: Ed25519Address,
    partition_id: u16,
    milestone_index: MilestoneIndex,
    output_type: u8,
    transaction_id: TransactionId,
    index: u16,
}

impl Ed25519AddressPK {
    pub fn new(
        address: Ed25519Address,
        partition_id: u16,
        milestone_index: MilestoneIndex,
        output_type: u8,
        transaction_id: TransactionId,
        index: u16,
    ) -> Self {
        Self {
            address,
            partition_id,
            milestone_index,
            output_type,
            transaction_id,
            index,
        }
    }
}

/// Delete Address record from addresses table
impl Delete<Ed25519AddressPK, AddressRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "DELETE FROM {}.addresses WHERE address = ? AND partition_id = ? AND milestone_index = ? AND output_type = ? AND transaction_id = ? AND idx = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        Ed25519AddressPK {
            address,
            partition_id,
            milestone_index,
            output_type,
            transaction_id,
            index,
        }: &Ed25519AddressPK,
    ) -> T::Return {
        builder
            .value(&address.as_ref())
            .value(partition_id)
            .value(&transaction_id.as_ref())
            .value(&milestone_index.0)
            .value(output_type)
            .value(&transaction_id.as_ref())
            .value(index)
    }
}

#[derive(Clone)]
pub struct IndexationPK {
    indexation: Indexation,
    partition_id: u16,
    milestone_index: MilestoneIndex,
    message_id: MessageId,
}
impl IndexationPK {
    pub fn new(
        indexation: Indexation,
        partition_id: u16,
        milestone_index: MilestoneIndex,
        message_id: MessageId,
    ) -> Self {
        Self {
            indexation,
            partition_id,
            milestone_index,
            message_id,
        }
    }
}

/// Delete Index record from Indexes table
impl Delete<IndexationPK, IndexationRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "DELETE FROM {}.indexes WHERE hashed_index = ? AND partition_id = ? AND milestone_index = ? AND message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        IndexationPK {
            indexation,
            partition_id,
            milestone_index,
            message_id,
        }: &IndexationPK,
    ) -> T::Return {
        builder
            .value(&indexation.0)
            .value(partition_id)
            .value(&milestone_index.0)
            .value(&message_id.as_ref())
    }
}

#[derive(Clone)]
pub struct ParentPK {
    parent_id: MessageId,
    partition_id: u16,
    milestone_index: MilestoneIndex,
    message_id: MessageId,
}
impl ParentPK {
    pub fn new(
        parent_id: MessageId,
        partition_id: u16,
        milestone_index: MilestoneIndex,
        message_id: MessageId,
    ) -> Self {
        Self {
            parent_id,
            partition_id,
            milestone_index,
            message_id,
        }
    }
}

/// Delete Parent record from Parents table
impl Delete<ParentPK, ParentRecord> for PermanodeKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "DELETE FROM {}.parents WHERE parent_id = ? AND partition_id = ? AND milestone_index = ? AND message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(
        builder: T,
        ParentPK {
            parent_id,
            partition_id,
            milestone_index,
            message_id,
        }: &ParentPK,
    ) -> T::Return {
        builder
            .value(&parent_id.as_ref())
            .value(partition_id)
            .value(&milestone_index.0)
            .value(&message_id.as_ref())
    }
}
