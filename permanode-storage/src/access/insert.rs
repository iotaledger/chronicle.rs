use super::*;

impl<'a> Insert<'a, Bee<MessageId>, Bee<Message>> for Mainnet {
    fn insert_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, message) VALUES (?, ?)",
            Self::name()
        )
        .into()
    }

    fn get_request(
        &'a self,
        key: &Bee<MessageId>,
        value: &Bee<Message>,
    ) -> InsertRequest<Self, Bee<MessageId>, Bee<Message>>
    where
        Self: Insert<'a, Bee<MessageId>, Bee<Message>>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let mut message_bytes = Vec::new();
        value.pack(&mut message_bytes).expect("Error occurred packing Message");

        let query = Execute::new()
            .id(&Self::insert_id())
            .consistency(scylla_cql::Consistency::One)
            .value(message_id_bytes.as_slice())
            .value(message_bytes.as_slice())
            .build();

        let token = 1;

        InsertRequest::from_prepared(query, token, self)
    }
}

impl<'a> Insert<'a, Bee<MessageId>, Bee<MessageMetadata>> for Mainnet {
    fn insert_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, metadata) VALUES (?, ?)",
            Self::name()
        )
        .into()
    }

    fn get_request(
        &'a self,
        key: &Bee<MessageId>,
        value: &Bee<MessageMetadata>,
    ) -> InsertRequest<Self, Bee<MessageId>, Bee<MessageMetadata>>
    where
        Self: Insert<'a, Bee<MessageId>, Bee<MessageMetadata>>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let mut metadata_bytes = Vec::new();
        value
            .pack(&mut metadata_bytes)
            .expect("Error occurred packing MessageMetadata");

        let query = Execute::new()
            .id(&Self::insert_id())
            .consistency(scylla_cql::Consistency::One)
            .value(message_id_bytes.as_slice())
            .value(metadata_bytes.as_slice())
            .build();

        let token = 1;

        InsertRequest::from_prepared(query, token, self)
    }
}

impl<'a> Insert<'a, Bee<MessageId>, (Bee<Message>, Bee<MessageMetadata>)> for Mainnet {
    fn insert_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, message, metadata) VALUES (?, ?, ?)",
            Self::name()
        )
        .into()
    }

    fn get_request(
        &'a self,
        key: &Bee<MessageId>,
        (message, metadata): &(Bee<Message>, Bee<MessageMetadata>),
    ) -> InsertRequest<Self, Bee<MessageId>, (Bee<Message>, Bee<MessageMetadata>)>
    where
        Self: Insert<'a, Bee<MessageId>, (Bee<Message>, Bee<MessageMetadata>)>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let mut message_bytes = Vec::new();
        message
            .pack(&mut message_bytes)
            .expect("Error occurred packing Message");

        let mut metadata_bytes = Vec::new();
        metadata
            .pack(&mut metadata_bytes)
            .expect("Error occurred packing MessageMetadata");

        let query = Execute::new()
            .id(&Self::insert_id())
            .consistency(scylla_cql::Consistency::One)
            .value(message_id_bytes.as_slice())
            .value(message_bytes.as_slice())
            .value(metadata_bytes.as_slice())
            .build();

        let token = 1;

        InsertRequest::from_prepared(query, token, self)
    }
}

impl<'a> Insert<'a, Bee<MilestoneIndex>, Bee<Milestone>> for Mainnet {
    fn insert_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.milestones (milestone_index, milestone) VALUES (?, ?)",
            Self::name()
        )
        .into()
    }

    fn get_request(
        &'a self,
        key: &Bee<MilestoneIndex>,
        value: &Bee<Milestone>,
    ) -> InsertRequest<Self, Bee<MilestoneIndex>, Bee<Milestone>>
    where
        Self: Insert<'a, Bee<MilestoneIndex>, Bee<Milestone>>,
    {
        let mut index_bytes = Vec::new();
        value
            .pack(&mut index_bytes)
            .expect("Error occurred packing Milestone Index");

        let mut milestone_bytes = Vec::new();
        value
            .pack(&mut milestone_bytes)
            .expect("Error occurred packing Milestone");

        let query = Execute::new()
            .id(&Self::insert_id())
            .consistency(scylla_cql::Consistency::One)
            .value(index_bytes.as_slice())
            .value(milestone_bytes.as_slice())
            .build();

        let token = 1;

        InsertRequest::from_prepared(query, token, self)
    }
}

impl<'a> Insert<'a, Bee<HashedIndex>, Bee<MessageId>> for Mainnet {
    fn insert_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.indexes (hashed_index, partition_id, message_id) VALUES (?, ?, ?)",
            Self::name()
        )
        .into()
    }

    fn get_request(
        &'a self,
        key: &Bee<HashedIndex>,
        value: &Bee<MessageId>,
    ) -> InsertRequest<'a, Self, Bee<HashedIndex>, Bee<MessageId>>
    where
        Self: Insert<'a, Bee<HashedIndex>, Bee<MessageId>>,
    {
        let mut message_id_bytes = Vec::new();
        value
            .pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let query = Execute::new()
            .id(&Self::insert_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.as_ref())
            .value(0u16)
            .value(message_id_bytes.as_slice())
            .build();

        let token = 1;

        InsertRequest::from_prepared(query, token, self)
    }
}

// impl_insert!(Mainnet: <(MessageId, MessageId), ()> -> { todo!() });
// impl_insert!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
// impl_insert!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
// impl_insert!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
// impl_insert!(Mainnet: <Unspent, ()> -> { todo!() });
// impl_insert!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
// impl_insert!(Mainnet: <(), LedgerIndex> -> { todo!() });
// impl_insert!(Mainnet: <(), SnapshotInfo> -> { todo!() });
// impl_insert!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
// impl_insert!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
// impl_insert!(Mainnet: <Address, Balance> -> { todo!() });
// impl_insert!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
