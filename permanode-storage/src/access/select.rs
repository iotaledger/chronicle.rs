use super::*;

impl Select<MessageId, Message> for Mainnet {
    fn select(&self, key: &MessageId) -> SelectQuery<Self, MessageId, Message> {
        let query = Query::new()
            .statement("SELECT message from mainnet.messages WHERE messsage_id = ?")
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        SelectQuery::new(query)
    }

    fn decode(decoder: Decoder) -> Result<Option<Message>, CqlError> {
        todo!()
    }
}

impl Select<MessageId, MessageMetadata> for Mainnet {
    fn select(&self, key: &MessageId) -> SelectQuery<Self, MessageId, MessageMetadata> {
        let query = Query::new()
            .statement("SELECT metadata from mainnet.messages WHERE messsage_id = ?")
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        SelectQuery::new(query)
    }

    fn decode(decoder: Decoder) -> Result<Option<MessageMetadata>, CqlError> {
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
