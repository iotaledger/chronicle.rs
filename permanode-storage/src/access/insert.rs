use super::*;
impl Insert<MessageId, Message> for PermanodeKeyspace {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, message) VALUES (?, ?)",
            self.name()
        )
        .into()
    }

    fn get_request(&self, key: &MessageId, value: &Message) -> InsertRequest<Self, MessageId, Message>
    where
        Self: Insert<MessageId, Message>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let mut message_bytes = Vec::new();
        value.pack(&mut message_bytes).expect("Error occurred packing Message");

        let query = Query::new()
            .statement(&self.insert_statement::<MessageId, Message>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .value(&message_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Insert<Bee<MessageId>, Bee<Message>> for PermanodeKeyspace {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, message) VALUES (?, ?)",
            self.name()
        )
        .into()
    }

    fn get_request(
        &self,
        key: &Bee<MessageId>,
        value: &Bee<Message>,
    ) -> InsertRequest<Self, Bee<MessageId>, Bee<Message>>
    where
        Self: Insert<Bee<MessageId>, Bee<Message>>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let mut message_bytes = Vec::new();
        value.pack(&mut message_bytes).expect("Error occurred packing Message");

        let query = Query::new()
            .statement(&self.insert_statement::<Bee<MessageId>, Bee<Message>>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .value(&message_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Insert<Bee<MessageId>, Bee<MessageMetadata>> for PermanodeKeyspace {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, metadata) VALUES (?, ?)",
            self.name()
        )
        .into()
    }

    fn get_request(
        &self,
        key: &Bee<MessageId>,
        value: &Bee<MessageMetadata>,
    ) -> InsertRequest<Self, Bee<MessageId>, Bee<MessageMetadata>>
    where
        Self: Insert<Bee<MessageId>, Bee<MessageMetadata>>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let mut metadata_bytes = Vec::new();
        value
            .pack(&mut metadata_bytes)
            .expect("Error occurred packing MessageMetadata");

        let query = Query::new()
            .statement(&self.insert_statement::<Bee<MessageId>, Bee<MessageMetadata>>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .value(&metadata_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Insert<Bee<MessageId>, (Bee<Message>, Bee<MessageMetadata>)> for PermanodeKeyspace {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.messages (message_id, message, metadata) VALUES (?, ?, ?)",
            self.name()
        )
        .into()
    }

    fn get_request(
        &self,
        key: &Bee<MessageId>,
        (message, metadata): &(Bee<Message>, Bee<MessageMetadata>),
    ) -> InsertRequest<Self, Bee<MessageId>, (Bee<Message>, Bee<MessageMetadata>)>
    where
        Self: Insert<Bee<MessageId>, (Bee<Message>, Bee<MessageMetadata>)>,
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

        let query = Query::new()
            .statement(&self.insert_statement::<Bee<MessageId>, (Bee<Message>, Bee<MessageMetadata>)>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .value(&message_bytes.as_slice())
            .value(&metadata_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Insert<Bee<MilestoneIndex>, Bee<Milestone>> for PermanodeKeyspace {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.milestones (milestone_index, milestone) VALUES (?, ?)",
            self.name()
        )
        .into()
    }

    fn get_request(
        &self,
        key: &Bee<MilestoneIndex>,
        value: &Bee<Milestone>,
    ) -> InsertRequest<Self, Bee<MilestoneIndex>, Bee<Milestone>>
    where
        Self: Insert<Bee<MilestoneIndex>, Bee<Milestone>>,
    {
        let mut index_bytes = Vec::new();
        key.pack(&mut index_bytes)
            .expect("Error occurred packing Milestone Index");

        let mut milestone_bytes = Vec::new();
        value
            .pack(&mut milestone_bytes)
            .expect("Error occurred packing Milestone");

        let query = Query::new()
            .statement(&self.insert_statement::<Bee<MilestoneIndex>, Bee<Milestone>>())
            .consistency(scylla_cql::Consistency::One)
            .value(&index_bytes.as_slice())
            .value(&milestone_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Insert<Bee<HashedIndex>, Bee<MessageId>> for PermanodeKeyspace {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "INSERT INTO {}.indexes (hashed_index, partition_id, message_id) VALUES (?, ?, ?)",
            self.name()
        )
        .into()
    }

    fn get_request(
        &self,
        key: &Bee<HashedIndex>,
        value: &Bee<MessageId>,
    ) -> InsertRequest<Self, Bee<HashedIndex>, Bee<MessageId>>
    where
        Self: Insert<Bee<HashedIndex>, Bee<MessageId>>,
    {
        let mut message_id_bytes = Vec::new();
        value
            .pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let query = Query::new()
            .statement(&self.insert_statement::<Bee<HashedIndex>, Bee<MessageId>>())
            .consistency(scylla_cql::Consistency::One)
            .value(&key.as_ref())
            .value(&0u16)
            .value(&message_id_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}
