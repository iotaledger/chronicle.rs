use super::*;
use scylla_cql::{
    rows,
    simple_rows,
};

rows!(
    rows: MessageRows,
    row: MessageRow {
        id: Bee<MessageId>,
        message: Option<Bee<Message>>,
        metadata: Option<Bee<MessageMetadata>>,
    },
    row_into: MessageRow
);

simple_rows!(rows: IndexMessages, row: Bee<Message>, row_into: Bee<Message>);

simple_rows!(rows: MessageChildren, row: Bee<MessageId>, row_into: Bee<MessageId>);
