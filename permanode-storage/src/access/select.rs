use super::*;
use crate::types::*;
use scylla::access::keyspace::Keyspace;
use scylla_cql::{
    Frame,
    RowsDecoder,
};

impl<'a> Select<'a, Bee<MessageId>, Bee<Message>> for Mainnet {
    fn get_request(&'a self, key: &Bee<MessageId>) -> SelectRequest<'a, Self, Bee<MessageId>, Bee<Message>> {
        let query = Query::new()
            .statement(&format!(
                "SELECT message from {}.messages WHERE message_id = ?",
                Self::name()
            ))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl<'a> Select<'a, Bee<MessageId>, MessageChildren> for Mainnet {
    fn get_request(&'a self, key: &Bee<MessageId>) -> SelectRequest<'a, Self, Bee<MessageId>, MessageChildren> {
        let query = Query::new()
            .statement(&format!(
                "SELECT m.message
                FROM {0}.edges e
                JOIN {0}.messages m ON e.children = m.id
                WHERE e.parent = ?
                AND e.partition_id = ?",
                Self::name()
            ))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            // TODO: .value(partition)
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl RowsDecoder<Bee<MessageId>, MessageChildren> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<MessageChildren>, CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            Ok(Some(MessageChildren::new(decoder)))
        }
    }
}

impl<'a> Select<'a, Bee<MessageId>, Bee<MessageMetadata>> for Mainnet {
    fn get_request(&self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, Bee<MessageMetadata>> {
        let query = Query::new()
            .statement(&format!(
                "SELECT metadata from {}.messages WHERE message_id = ?",
                Self::name()
            ))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl<'a> Select<'a, Bee<MessageId>, MessageRow> for Mainnet {
    fn get_request(&self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, MessageRow> {
        let query = Query::new()
            .statement(&format!("SELECT * from {}.messages WHERE message_id = ?", Self::name()))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl RowsDecoder<Bee<MessageId>, MessageRow> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<MessageRow>, CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            let mut rows = MessageRows::new(decoder);
            Ok(rows.next())
        }
    }
}

impl<'a> Select<'a, Bee<MilestoneIndex>, Bee<Milestone>> for Mainnet {
    fn get_request(&self, key: &Bee<MilestoneIndex>) -> SelectRequest<Self, Bee<MilestoneIndex>, Bee<Milestone>> {
        let query = Query::new()
            .statement(&format!(
                "SELECT milestone from {}.milestones WHERE milestone_index = ?",
                Self::name()
            ))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl RowsDecoder<Bee<HashedIndex>, IndexMessages> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<IndexMessages>, CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            Ok(Some(IndexMessages::new(decoder)))
        }
    }
}

impl<'a> Select<'a, Bee<HashedIndex>, IndexMessages> for Mainnet {
    fn get_request(&self, key: &Bee<HashedIndex>) -> SelectRequest<Self, Bee<HashedIndex>, IndexMessages> {
        let query = Query::new()
            .statement(&format!(
                "SELECT m.message
                FROM {0}.index_lookup i
                JOIN {0}.messages m ON i.message_id = m.id
                WHERE i.hashed_index = ?
                AND i.partition_id = ?",
                Self::name()
            ))
            .consistency(scylla_cql::Consistency::One)
            .value(key.as_ref())
            // TODO: .value(partition)
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

// impl_select!(Mainnet: <MessageId, Message> -> { todo!() }, { todo!() });
// impl_select!(Mainnet: <MessageId, Bee<MessageMetadata>> -> { todo!() }, { todo!() });
// impl_select!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
// impl_select!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
// impl_select!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
// impl_select!(Mainnet: <Unspent, ()> -> { todo!() });
// impl_select!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
// impl_select!(Mainnet: <(), LedgerIndex> -> { todo!() });
// impl_select!(Mainnet: <(), SnapshotInfo> -> { todo!() });
// impl_select!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
// impl_select!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
// impl_select!(Mainnet: <Address, Balance> -> { todo!() });
// impl_select!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
