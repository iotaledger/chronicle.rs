// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bee_message::{
    address::Address,
    Message,
};
use chronicle_common::SyncRange;
use chrono::{
    NaiveDate,
    NaiveDateTime,
};
use std::{
    collections::BTreeMap,
    ops::Range,
    str::FromStr,
};

impl Select<Bee<MessageId>, (), Bee<Message>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT message
            FROM #.messages
            WHERE message_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(message_id)
    }
}

impl Select<Bee<MessageId>, (), MessageRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT
                message_id,
                message,
                milestone_index,
                inclusion_state,
                conflict_reason,
                proof
            FROM #.messages
            WHERE message_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(message_id)
    }
}

impl Select<Bee<MessageId>, (), Paged<Iter<MessageRecord>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT
                message_id,
                message,
                milestone_index,
                inclusion_state,
                conflict_reason,
                proof
            FROM #.messages
            WHERE milestone_index = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(message_id)
    }
}

impl Select<Bee<MessageId>, (), Paged<Iter<ParentRecord>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT
                parent_id,
                message_id,
                milestone_index,
                ms_timestamp,
                inclusion_state
            FROM #.parents_by_ms
            WHERE parent_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, parent_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(parent_id)
    }
}

impl Select<Bee<MessageId>, Range<NaiveDateTime>, Paged<Iter<ParentRecord>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT
                parent_id,
                message_id,
                milestone_index,
                ms_timestamp,
                inclusion_state
            FROM #.parents_by_ms
            WHERE parent_id = ?
            AND ms_timestamp >= ?
            AND ms_timestamp < ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, parent_id: &Bee<MessageId>, time_range: &Range<NaiveDateTime>) -> B {
        builder.value(parent_id).value(time_range.start).value(time_range.end)
    }
}

impl Select<(Bee<Address>, MsRangeId), (), Paged<Vec<LegacyOutputRecord>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT *
            FROM #.legacy_outputs_by_address
            WHERE address = ?
            AND ms_range_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, (address, ms_range_id): &(Bee<Address>, MsRangeId), _: &()) -> B {
        builder.value(address).value(ms_range_id)
    }
}

impl Select<(Bee<Address>, MsRangeId), Range<NaiveDateTime>, Paged<Vec<LegacyOutputRecord>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT *
            FROM #.legacy_outputs_by_address
            WHERE address = ?
            AND ms_range_id = ?
            AND ms_timestamp >= ?
            AND ms_timestamp < ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        (address, ms_range_id): &(Bee<Address>, MsRangeId),
        time_range: &Range<NaiveDateTime>,
    ) -> B {
        builder
            .value(address)
            .value(ms_range_id)
            .value(time_range.start)
            .value(time_range.end)
    }
}

impl RowsDecoder for Paged<Vec<LegacyOutputRecord>> {
    type Row = LegacyOutputRecord;
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Paged<Vec<LegacyOutputRecord>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        Ok(Some(Paged::new(iter.into_iter().collect(), paging_state)))
    }
}

impl Select<(String, MsRangeId), Range<NaiveDateTime>, Paged<Vec<TagRecord>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT *
            FROM #.tags
            WHERE tag = ?
            AND ms_range_id = ?
            AND ms_timestamp >= ?
            AND ms_timestamp < ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        (tag, ms_range_id): &(String, MsRangeId),
        time_range: &Range<NaiveDateTime>,
    ) -> B {
        builder
            .value(tag)
            .value(ms_range_id)
            .value(time_range.start)
            .value(time_range.end)
    }
}

impl RowsDecoder for Paged<Vec<TagRecord>> {
    type Row = TagRecord;
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Paged<Vec<TagRecord>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        Ok(Some(Paged::new(iter.into_iter().collect(), paging_state)))
    }
}

impl Select<Bee<TransactionId>, u16, OutputRes> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT message_id, data, inclusion_state
            FROM #.transactions
            WHERE transaction_id = ?
            AND idx = ?
            AND variant IN ('output', 'unlock')",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, transaction_id: &Bee<TransactionId>, idx: &u16) -> B {
        builder.value(transaction_id).value(idx)
    }
}

impl RowsDecoder for OutputRes {
    type Row = (String, TransactionData, Option<LedgerInclusionState>);
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<OutputRes>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut unlock_blocks = Vec::new();
        let mut output = None;
        for (message_id, transaction_data, inclusion_state) in Self::Row::rows_iter(decoder)? {
            let message_id = MessageId::from_str(&message_id).map_err(|e| anyhow::Error::new(e))?;
            match transaction_data {
                TransactionData::Output(o) => output = Some((message_id, o)),
                TransactionData::Unlock(u) => unlock_blocks.push(UnlockRes {
                    message_id,
                    block: u.unlock_block,
                    inclusion_state,
                }),
                _ => (),
            }
        }
        Ok(output.map(|output| OutputRes {
            message_id: output.0,
            output: output.1,
            unlock_blocks,
        }))
    }
}

impl Select<Bee<TransactionId>, (), TransactionRes> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT message_id, data, idx, inclusion_state, milestone_index
            FROM #.transactions
            WHERE transaction_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, txn_id: &Bee<TransactionId>, _: &()) -> B {
        builder.value(txn_id)
    }
}

impl RowsDecoder for TransactionRes {
    type Row = (String, TransactionData, u16, Option<LedgerInclusionState>, Option<u32>);
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<TransactionRes>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut outputs = BTreeMap::new();
        let mut unlock_blocks = BTreeMap::new();
        let mut inputs = BTreeMap::new();
        let mut metadata = None;
        for (message_id, transaction_data, idx, inclusion_state, milestone_index) in Self::Row::rows_iter(decoder)? {
            let message_id = MessageId::from_str(&message_id)?;
            let milestone_index = milestone_index.map(MilestoneIndex);
            match transaction_data {
                TransactionData::Output(o) => {
                    outputs.insert(idx, o);
                    metadata = metadata.or(Some((message_id, milestone_index)));
                }
                TransactionData::Unlock(u) => {
                    unlock_blocks.insert(
                        idx,
                        UnlockRes {
                            message_id,
                            block: u.unlock_block,
                            inclusion_state,
                        },
                    );
                }
                TransactionData::Input(i) => {
                    inputs.insert(idx, i);
                }
            }
        }
        let outputs = outputs
            .into_iter()
            .map(|(idx, o)| (o, unlock_blocks.remove(&idx)))
            .collect();
        Ok(metadata.map(|(message_id, milestone_index)| TransactionRes {
            message_id,
            milestone_index,
            outputs,
            inputs: inputs.into_iter().map(|(_, i)| (i)).collect(),
        }))
    }
}

impl Select<Bee<TransactionId>, LedgerInclusionState, Option<Bee<MessageId>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT message_id FROM #.transactions
            WHERE transaction_id = ? and inclusion_state = ? and variant = 'input'
            LIMIT 1 ALLOW FILTERING",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        transaction_id: &Bee<TransactionId>,
        inclusion_state: &LedgerInclusionState,
    ) -> B {
        builder.value(transaction_id).value(inclusion_state)
    }
}

impl Select<Bee<MilestoneIndex>, (), Bee<Milestone>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT message_id, timestamp
            FROM #.milestones
            WHERE milestone_index = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, index: &Bee<MilestoneIndex>, _: &()) -> B {
        builder.value(index)
    }
}

impl Select<AddressHint, (), Iter<MsRangeId>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT ms_range_id
            FROM #.address_hints
            WHERE address = ?
            AND output_kind = ?
            AND variant = ?",
            self.name()
        )
    }

    fn bind_values<B: Binder>(builder: B, address_hint: &AddressHint, _: &()) -> B {
        builder.bind(address_hint)
    }
}

impl Select<TagHint, (), Iter<MsRangeId>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT ms_range_id
            FROM #.tag_hints
            WHERE tag = ?
            AND table_kind = ?",
            self.name()
        )
    }

    fn bind_values<B: Binder>(builder: B, tag_hint: &TagHint, _: &()) -> B {
        builder.bind(tag_hint)
    }
}

impl Select<(), (), Iter<SyncRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!("SELECT * FROM #.sync", self.name())
    }
    fn bind_values<B: Binder>(builder: B, _: &(), _: &()) -> B {
        builder
    }
}

impl Select<MsRangeId, SyncRange, Iter<SyncRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT *
            FROM #.sync
            WHERE ms_range_id = ?
            AND milestone_index >= ?
            AND milestone_index < ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, ms_range_id: &MsRangeId, sync_range: &SyncRange) -> B {
        builder
            .value(ms_range_id)
            .value(&sync_range.start())
            .value(&sync_range.end())
    }
}

impl Select<MsRangeId, (), Iter<MsAnalyticsRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT *
            FROM #.ms_analytics
            WHERE ms_range_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, ms_range_id: &MsRangeId, _: &()) -> B {
        builder.value(ms_range_id)
    }
}

impl Select<MsRangeId, Range<MilestoneIndex>, Iter<MsAnalyticsRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT *
            FROM #.ms_analytics
            WHERE ms_range_id = ?
            AND milestone_index >= ?
            AND milestone_index < ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, ms_range_id: &MsRangeId, ms_range: &Range<MilestoneIndex>) -> B {
        builder
            .value(ms_range_id)
            .value(Bee(&ms_range.start))
            .value(Bee(&ms_range.end))
    }
}

impl Select<u32, Range<NaiveDate>, Iter<DailyAnalyticsRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT *
            FROM #.daily_analytics
            WHERE year = ?
            AND date >= ?
            AND date <= ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, year: &u32, date_range: &Range<NaiveDate>) -> B {
        builder.value(year).value(&date_range.start).value(&date_range.end)
    }
}

impl Select<NaiveDate, (), DateCacheRecord> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT start_ms, end_ms
            FROM #.date_cache
            WHERE date = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, date: &NaiveDate, _: &()) -> B {
        builder.value(date)
    }
}

impl Select<NaiveDate, MetricsVariant, MetricsCacheCount> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT count(*)
            FROM #.metrics_cache
            WHERE date = ?
            AND variant = ?
            AND metric = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, date: &NaiveDate, variant: &MetricsVariant) -> B {
        builder.value(date).bind(variant)
    }
}
