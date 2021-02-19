use super::*;
use crate::types::MessageTable;
use bee_common::packable::Packable;
use scylla::access::keyspace::Keyspace;
use scylla_cql::{
    rows,
    ColumnDecoder,
    Metadata,
    Rows,
    RowsDecoder,
    TryInto,
};
use std::{
    io::Cursor,
    str::FromStr,
};

impl<'a> Select<'a, MessageId, Message> for Mainnet {
    fn get_request(&'a self, key: &MessageId) -> SelectRequest<'a, Self, MessageId, Message> {
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

impl RowsDecoder<MessageId, Message> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<Message>, CqlError> {
        todo!()
    }
}

impl<'a> Select<'a, MessageId, MessageMetadata> for Mainnet {
    fn get_request(&self, key: &MessageId) -> SelectRequest<Self, MessageId, MessageMetadata> {
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

impl RowsDecoder<MessageId, MessageMetadata> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<MessageMetadata>, CqlError> {
        todo!()
    }
}

rows!(
    rows: MessageRows,
    row: MessageRow {
        id: String,
        message: Option<Vec<u8>>,
        metadata: Option<Vec<u8>>,
    },
    row_into: MessageTable
);

impl From<MessageRow> for MessageTable {
    fn from(row: MessageRow) -> Self {
        MessageTable {
            id: MessageId::from_str(&row.id).unwrap(),
            message: row
                .message
                .and_then(|message| Message::unpack(&mut Cursor::new(message)).ok()),
            metadata: row
                .metadata
                .and_then(|metadata| MessageMetadata::unpack(&mut Cursor::new(metadata)).ok()),
        }
    }
}

impl<'a> Select<'a, MessageId, MessageTable> for Mainnet {
    fn get_request(&self, key: &MessageId) -> SelectRequest<Self, MessageId, MessageTable> {
        let query = Query::new()
            .statement(&format!("SELECT * from {}.messages WHERE message_id = ?", Self::name()))
            .consistency(scylla_cql::Consistency::One)
            .value(key.to_string())
            .build();

        let token = 1;

        SelectRequest::new(query, token, self)
    }
}

impl RowsDecoder<MessageId, MessageTable> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<MessageTable>, CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            let mut rows = MessageRows::new(decoder);
            Ok(rows.next())
        }
    }
}

impl<'a> Select<'a, MilestoneIndex, Milestone> for Mainnet {
    fn get_request(&self, key: &MilestoneIndex) -> SelectRequest<Self, MilestoneIndex, Milestone> {
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

impl RowsDecoder<MilestoneIndex, Milestone> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<Milestone>, CqlError> {
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
// impl_select!(Mainnet: <(), SnapshotInfo> -> { todo!() });
// impl_select!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
// impl_select!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
// impl_select!(Mainnet: <Address, Balance> -> { todo!() });
// impl_select!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
