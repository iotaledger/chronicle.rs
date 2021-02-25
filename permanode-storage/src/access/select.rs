use super::*;
use scylla_cql::{
    Frame,
    RowsDecoder,
};

impl<'a> Select<'a, Bee<MessageId>, Bee<Message>> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!("SELECT message from {}.messages WHERE message_id = ?", Self::name()).into()
    }

    fn get_request(&'a self, key: &Bee<MessageId>) -> SelectRequest<'a, Self, Bee<MessageId>, Bee<Message>>
    where
        Self: Select<'a, Bee<MessageId>, Bee<Message>>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
    }
}

impl<'a> Select<'a, Bee<MessageId>, MessageChildren> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT m.message_id
            FROM {0}.edges e
            JOIN {0}.messages m ON e.children = m.id
            WHERE e.parent = ?
            AND e.partition_id = ?",
            Self::name()
        )
        .into()
    }

    fn get_request(&'a self, key: &Bee<MessageId>) -> SelectRequest<'a, Self, Bee<MessageId>, MessageChildren>
    where
        Self: Select<'a, Bee<MessageId>, MessageChildren>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            // TODO: .value(partition)
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
    }
}

impl<'a> Select<'a, Bee<MessageId>, Bee<MessageMetadata>> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!("SELECT metadata from {}.messages WHERE message_id = ?", Self::name()).into()
    }

    fn get_request(&'a self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, Bee<MessageMetadata>>
    where
        Self: Select<'a, Bee<MessageId>, Bee<MessageMetadata>>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
    }
}

impl<'a> Select<'a, Bee<MessageId>, MessageRow> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, message, metadata from {}.messages WHERE message_id = ?",
            Self::name()
        )
        .into()
    }

    fn get_request(&'a self, key: &Bee<MessageId>) -> SelectRequest<Self, Bee<MessageId>, MessageRow>
    where
        Self: Select<'a, Bee<MessageId>, MessageRow>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
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

impl<'a> Select<'a, Bee<MilestoneIndex>, SingleMilestone> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, timestamp from {}.milestones WHERE milestone_index = ?",
            Self::name()
        )
        .into()
    }

    fn get_request(&'a self, key: &Bee<MilestoneIndex>) -> SelectRequest<Self, Bee<MilestoneIndex>, SingleMilestone>
    where
        Self: Select<'a, Bee<MilestoneIndex>, SingleMilestone>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
    }
}

impl<'a> Select<'a, Bee<HashedIndex>, IndexMessages> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id from {}.indexes WHERE hashed_index = ? AND partition_id = ?",
            Self::name()
        )
        .into()
    }

    fn get_request(&'a self, key: &Bee<HashedIndex>) -> SelectRequest<Self, Bee<HashedIndex>, IndexMessages>
    where
        Self: Select<'a, Bee<HashedIndex>, IndexMessages>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.as_ref())
            // TODO: .value(partition)
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
    }
}

impl<'a> Select<'a, Bee<OutputId>, Outputs> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data from {}.transactions WHERE transaction_id = ? AND index = ? and variant = 'utxoinput'",
            Self::name()
        )
        .into()
    }

    fn get_request(&'a self, key: &Bee<OutputId>) -> SelectRequest<'a, Self, Bee<OutputId>, Outputs>
    where
        Self: Select<'a, Bee<OutputId>, Outputs>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.transaction_id().to_string())
            .value(key.index())
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
    }
}

impl<'a> Select<'a, Bee<Ed25519Address>, OutputIds> for Mainnet {
    fn select_statement() -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT transaction_id, index 
            FROM {}.addresses 
            WHERE address = ? AND address_type = 0 AND partition_id = ?",
            Self::name()
        )
        .into()
    }

    fn get_request(&'a self, key: &Bee<Ed25519Address>) -> SelectRequest<Self, Bee<Ed25519Address>, OutputIds>
    where
        Self: Select<'a, Bee<Ed25519Address>, OutputIds>,
    {
        let query = Execute::new()
            .id(&Self::select_id())
            .consistency(scylla_cql::Consistency::One)
            .value(key.as_ref())
            // TODO: .value(partition)
            .build();

        let token = 1;

        SelectRequest::from_prepared(query, token, self)
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
