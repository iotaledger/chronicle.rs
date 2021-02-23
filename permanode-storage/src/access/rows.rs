use std::cell::RefCell;

use super::*;
use scylla_cql::{
    rows,
    simple_rows,
};
use serde::Serialize;

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

rows!(
    rows: Outputs,
    row: OutputRow {
        transaction_id: Bee<TransactionId>,
        index: u16,
    },
    row_into: Bee<OutputId>
);

impl From<OutputRow> for Bee<OutputId> {
    fn from(row: OutputRow) -> Self {
        OutputId::new(row.transaction_id.into_inner(), row.index)
            .unwrap()
            .into()
    }
}

impl<K, V: Rows> RowsDecoder<K, V> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<V>, CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            Ok(Some(V::new(decoder)))
        }
    }
}

pub struct IteratorSerializer<I>(RefCell<I>);

impl<I> IteratorSerializer<I> {
    pub fn new(iter: I) -> Self {
        Self(RefCell::new(iter))
    }
}

impl<I> Serialize for IteratorSerializer<I>
where
    I: Iterator,
    I::Item: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.borrow_mut().by_ref())
    }
}
