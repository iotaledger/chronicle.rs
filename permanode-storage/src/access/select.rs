use scylla::access::keyspace::Keyspace;
use scylla_cql::RowsDecoder;

use super::*;

impl<'a> Select<'a, MessageId, Message> for Mainnet {
    fn get_request(&'a self, key: &MessageId) -> SelectRequest<'a, Self, MessageId, Message> {
        let query = Query::new()
            .statement(&format!(
                "SELECT message from {}.messages WHERE messsage_id = ?",
                Self::name()
            ))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl RowsDecoder<MessageId, Message> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<Message>, CqlError> {
        todo!()
    }
}

impl<'a> Select<'a, MessageId, MessageMetadata> for Mainnet {
    fn get_request(&self, key: &MessageId) -> SelectRequest<Self, MessageId, MessageMetadata> {
        let query = Query::new()
            .statement(&format!(
                "SELECT metadata from {}.messages WHERE messsage_id = ?",
                Self::name()
            ))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl RowsDecoder<MessageId, MessageMetadata> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<MessageMetadata>, CqlError> {
        todo!()
    }
}

// impl_select!(Mainnet: <MessageId, Message> -> { todo!() }, { todo!() });
// impl_select!(Mainnet: <MessageId, MessageMetadata> -> { todo!() }, { todo!() });
// impl_select!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
// impl_select!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
// impl_select!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
// impl_select!(Mainnet: <Unspent, ()> -> { todo!() });
// impl_select!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
// impl_select!(Mainnet: <(), LedgerIndex> -> { todo!() });
// impl_select!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
// impl_select!(Mainnet: <(), SnapshotInfo> -> { todo!() });
// impl_select!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
// impl_select!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
// impl_select!(Mainnet: <Address, Balance> -> { todo!() });
// impl_select!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
