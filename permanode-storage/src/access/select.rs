use super::*;
use scylla_cql::{
    Frame,
    RowsDecoder,
};

impl Select<Bee<MessageId>, Bee<Message>> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message from {}.messages WHERE message_id = ?", self.name()).into()
    }

    fn get_request(&self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, Bee<Message>>
    where
        Self: Select<Bee<MessageId>, Bee<Message>>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let query = Execute::new()
            .id(&self.select_id::<Bee<MessageId>, Bee<Message>>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Select<Bee<MessageId>, MessageChildren> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id FROM {}.parents WHERE parent_id = ? AND partition_id = ?",
            self.name()
        )
        .into()
    }

    fn get_request(&self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, MessageChildren>
    where
        Self: Select<Bee<MessageId>, MessageChildren>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let query = Execute::new()
            .id(&self.select_id::<Bee<MessageId>, MessageChildren>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .value(&0u16)
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Select<Bee<MessageId>, Bee<MessageMetadata>> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT metadata from {}.messages WHERE message_id = ?", self.name()).into()
    }

    fn get_request(&self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, Bee<MessageMetadata>>
    where
        Self: Select<Bee<MessageId>, Bee<MessageMetadata>>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let query = Execute::new()
            .id(&self.select_id::<Bee<MessageId>, Bee<MessageMetadata>>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Select<Bee<MessageId>, MessageRow> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, message, metadata from {}.messages WHERE message_id = ?",
            self.name()
        )
        .into()
    }

    fn get_request(&self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, MessageRow>
    where
        Self: Select<Bee<MessageId>, MessageRow>,
    {
        let mut message_id_bytes = Vec::new();
        key.pack(&mut message_id_bytes)
            .expect("Error occurred packing Message ID");

        let query = Execute::new()
            .id(&self.select_id::<Bee<MessageId>, MessageRow>())
            .consistency(scylla_cql::Consistency::One)
            .value(&message_id_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
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

impl Select<Bee<MilestoneIndex>, SingleMilestone> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, timestamp from {}.milestones WHERE milestone_index = ?",
            self.name()
        )
        .into()
    }

    fn get_request(&self, key: &Bee<MilestoneIndex>) -> SelectRequest<Self, Bee<MilestoneIndex>, SingleMilestone>
    where
        Self: Select<Bee<MilestoneIndex>, SingleMilestone>,
    {
        let mut index_bytes = Vec::new();
        key.pack(&mut index_bytes)
            .expect("Error occurred packing Milestone Index");

        let query = Execute::new()
            .id(&self.select_id::<Bee<MilestoneIndex>, SingleMilestone>())
            .consistency(scylla_cql::Consistency::One)
            .value(&index_bytes.as_slice())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Select<Bee<HashedIndex>, IndexMessages> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id from {}.indexes WHERE hashed_index = ? AND partition_id = ?",
            self.name()
        )
        .into()
    }

    fn get_request(&self, key: &Bee<HashedIndex>) -> SelectRequest<Self, Bee<HashedIndex>, IndexMessages>
    where
        Self: Select<Bee<HashedIndex>, IndexMessages>,
    {
        let query = Execute::new()
            .id(&self.select_id::<Bee<HashedIndex>, IndexMessages>())
            .consistency(scylla_cql::Consistency::One)
            .value(&key.as_ref())
            .value(&0u16)
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Select<Bee<OutputId>, Outputs> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data from {}.transactions WHERE transaction_id = ? AND idx = ? and variant = 'utxoinput'",
            self.name()
        )
        .into()
    }

    fn get_request(&self, key: &Bee<OutputId>) -> SelectRequest<Self, Bee<OutputId>, Outputs>
    where
        Self: Select<Bee<OutputId>, Outputs>,
    {
        let query = Execute::new()
            .id(&self.select_id::<Bee<OutputId>, Outputs>())
            .consistency(scylla_cql::Consistency::One)
            .value(&key.transaction_id().to_string())
            .value(&key.index())
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}

impl Select<Bee<Ed25519Address>, OutputIds> for Mainnet {
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT transaction_id, idx 
            FROM {}.addresses 
            WHERE address = ? AND address_type = 0 AND partition_id = ?",
            self.name()
        )
        .into()
    }

    fn get_request(&self, key: &Bee<Ed25519Address>) -> SelectRequest<Self, Bee<Ed25519Address>, OutputIds>
    where
        Self: Select<Bee<Ed25519Address>, OutputIds>,
    {
        let query = Execute::new()
            .id(&self.select_id::<Bee<Ed25519Address>, OutputIds>())
            .consistency(scylla_cql::Consistency::One)
            .value(&key.as_ref())
            .value(&0u16)
            .build();

        let token = Self::token(key);

        self.create_request(query, token)
    }
}
