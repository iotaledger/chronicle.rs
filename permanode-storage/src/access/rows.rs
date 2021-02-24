use std::cell::RefCell;

use super::*;
use scylla_cql::rows;
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

rows!(rows: IndexMessages, row: Bee<MessageId>, row_into: Bee<MessageId>);

rows!(rows: MessageChildren, row: Bee<MessageId>, row_into: Bee<MessageId>);

rows!(
    rows: OutputIds,
    row: OutputIdRow {
        transaction_id: Bee<TransactionId>,
        index: u16,
    },
    row_into: Bee<OutputId>
);

impl From<OutputIdRow> for Bee<OutputId> {
    fn from(row: OutputIdRow) -> Self {
        OutputId::new(row.transaction_id.into_inner(), row.index)
            .unwrap()
            .into()
    }
}

rows!(
    single_row: SingleMilestone,
    row: MilestoneRow {
        message_id: Bee<MessageId>,
        timestamp: u64,
    },
    row_into: Bee<Milestone>
);

impl From<MilestoneRow> for Bee<Milestone> {
    fn from(row: MilestoneRow) -> Self {
        Milestone::new(row.message_id.into_inner(), row.timestamp).into()
    }
}

rows!(
    rows: Outputs,
    row: OutputRow {
        message_id: Bee<MessageId>,
        data: TransactionData,
    },
    row_into: OutputRow,
);

impl<K, V: Rows> RowsDecoder<K, V> for Mainnet {
    fn try_decode(decoder: Decoder) -> Result<Option<V>, CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            Ok(Some(V::new(decoder)))
        }
    }
}
