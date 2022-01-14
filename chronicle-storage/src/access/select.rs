// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use chronicle_common::SyncRange;

use super::*;
use std::{
    collections::{
        hash_map::Entry,
        BTreeMap,
        HashMap,
        VecDeque,
    },
    str::FromStr,
};

impl Select<Bee<MessageId>, (), Bee<Message>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!("SELECT message FROM #.messages WHERE message_id = ?", self.name())
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(message_id)
    }
}

impl Select<Bee<MessageId>, (), Option<MessageMetadata>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!("SELECT metadata FROM #.messages WHERE message_id = ?", self.name())
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(message_id)
    }
}

impl Select<Bee<MessageId>, (), (Option<Bee<Message>>, Option<MessageMetadata>)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT message, metadata FROM #.messages WHERE message_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(message_id)
    }
}

impl Select<Bee<MessageId>, (), FullMessage> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        <Self as Select<Bee<MessageId>, (), (Option<Bee<Message>>, Option<MessageMetadata>)>>::statement(self)
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, _: &()) -> B {
        builder.value(message_id)
    }
}

impl Select<(Bee<MessageId>, PartitionId), Bee<MilestoneIndex>, Paged<VecDeque<Partitioned<ParentRecord>>>>
    for ChronicleKeyspace
{
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT partition_id, milestone_index, message_id, inclusion_state
            FROM #.parents
            WHERE parent_id = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        (message_id, partition_id): &(Bee<MessageId>, PartitionId),
        milestone_index: &Bee<MilestoneIndex>,
    ) -> B {
        builder.value(message_id).value(partition_id).value(milestone_index)
    }
}

impl RowsDecoder for Paged<VecDeque<Partitioned<ParentRecord>>> {
    type Row = (PartitionId, u32, String, Option<LedgerInclusionState>);
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Paged<VecDeque<Partitioned<ParentRecord>>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        let values = iter
            .map(|(partition_id, milestone_index, message_id, inclusion_state)| {
                let message_id = MessageId::from_str(&message_id).map_err(|e| anyhow::Error::new(e))?;
                Ok(Partitioned::new(
                    ParentRecord::new(MilestoneIndex(milestone_index), message_id, inclusion_state),
                    partition_id,
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<(Indexation, PartitionId), Bee<MilestoneIndex>, Paged<VecDeque<Partitioned<IndexationRecord>>>>
    for ChronicleKeyspace
{
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT partition_id, milestone_index, message_id, inclusion_state
            FROM #.indexes
            WHERE indexation = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        (index, partition_id): &(Indexation, PartitionId),
        milestone_index: &Bee<MilestoneIndex>,
    ) -> B {
        builder.value(&index.0).value(partition_id).value(milestone_index)
    }
}

impl RowsDecoder for Paged<VecDeque<Partitioned<IndexationRecord>>> {
    type Row = (PartitionId, u32, String, Option<LedgerInclusionState>);
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Paged<VecDeque<Partitioned<IndexationRecord>>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        let values = iter
            .map(|(partition_id, milestone_index, message_id, inclusion_state)| {
                let message_id = MessageId::from_str(&message_id).map_err(|e| anyhow::Error::new(e))?;
                Ok(Partitioned::new(
                    IndexationRecord::new(milestone_index.into(), message_id, inclusion_state),
                    partition_id,
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<(Bee<Ed25519Address>, PartitionId), Bee<MilestoneIndex>, Paged<VecDeque<Partitioned<AddressRecord>>>>
    for ChronicleKeyspace
{
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT partition_id, milestone_index, output_type,
             transaction_id, idx, amount, inclusion_state
             FROM #.addresses WHERE address = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        (address, partition_id): &(Bee<Ed25519Address>, PartitionId),
        milestone_index: &Bee<MilestoneIndex>,
    ) -> B {
        builder.value(address).value(partition_id).value(milestone_index)
    }
}

impl RowsDecoder for Paged<VecDeque<Partitioned<AddressRecord>>> {
    type Row = (
        PartitionId,
        u32,
        OutputType,
        String,
        Index,
        Amount,
        Option<LedgerInclusionState>,
    );
    fn try_decode_rows(decoder: Decoder) -> anyhow::Result<Option<Paged<VecDeque<Partitioned<AddressRecord>>>>> {
        ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        let mut iter = Self::Row::rows_iter(decoder)?;
        let paging_state = iter.take_paging_state();
        let mut map = HashMap::<OutputId, usize>::new();
        let mut values = VecDeque::<Partitioned<AddressRecord>>::new();
        for (partition_id, milestone_index, output_type, transaction_id, index, amount, inclusion_state) in iter {
            let transaction_id = TransactionId::from_str(&transaction_id).map_err(|e| anyhow::Error::new(e))?;
            let record = Partitioned::new(
                AddressRecord::new(
                    milestone_index.into(),
                    output_type,
                    transaction_id,
                    index,
                    amount,
                    inclusion_state,
                ),
                partition_id,
            );
            if let Ok(output_id) = OutputId::new(transaction_id, index) {
                match map.entry(output_id) {
                    Entry::Occupied(v) => {
                        if let Some(prev) = values.get_mut(*v.get()) {
                            if prev.ledger_inclusion_state.is_none() {
                                *prev = record;
                            }
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(values.len());
                        values.push_back(record);
                    }
                }
            }
        }
        Ok(Some(Paged::new(values, paging_state)))
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
            "SELECT message_id, timestamp FROM #.milestones WHERE milestone_index = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, index: &Bee<MilestoneIndex>, _: &()) -> B {
        builder.value(index)
    }
}

impl Select<String, HintVariant, Iter<(Bee<MilestoneIndex>, PartitionId)>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT milestone_index, partition_id
            FROM #.hints
            WHERE hint = ? AND variant = ?",
            self.name()
        )
    }

    fn bind_values<B: Binder>(builder: B, hint: &String, variant: &HintVariant) -> B {
        builder.value(hint).value(variant)
    }
}

impl Select<String, SyncRange, Iter<SyncRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT milestone_index, synced_by, logged_by FROM #.sync WHERE key = ? AND milestone_index >= ? AND milestone_index < ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, keyspace: &String, sync_key: &SyncRange) -> B {
        builder.value(keyspace).value(&sync_key.start()).value(&sync_key.end())
    }
}

impl Select<String, SyncRange, Iter<AnalyticRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> SelectStatement {
        parse_statement!(
            "SELECT milestone_index, message_count, transaction_count, transferred_tokens FROM #.analytics WHERE key = ? AND milestone_index >= ? AND milestone_index < ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, keyspace: &String, sync_key: &SyncRange) -> B {
        builder.value(keyspace).value(&sync_key.start()).value(&sync_key.end())
    }
}

// ###############
// ROW DEFINITIONS
// ###############

impl Row for SyncRecord {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let milestone_index = MilestoneIndex(rows.column_value::<u32>()?);
        let synced_by = rows.column_value::<Option<u8>>()?;
        let logged_by = rows.column_value::<Option<u8>>()?;
        Ok(SyncRecord::new(milestone_index, synced_by, logged_by))
    }
}

impl Row for AnalyticRecord {
    fn try_decode_row<T: ColumnValue>(rows: &mut T) -> anyhow::Result<Self> {
        let milestone_index = MilestoneIndex(rows.column_value::<u32>()?);
        let message_count = MessageCount(rows.column_value::<u32>()?);
        let transaction_count = TransactionCount(rows.column_value::<u32>()?);
        let transferred_tokens = TransferredTokens(rows.column_value::<u64>()?);
        Ok(AnalyticRecord::new(
            milestone_index,
            message_count,
            transaction_count,
            transferred_tokens,
        ))
    }
}

impl Row for Bee<OutputId> {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(OutputId::new(
            rows.column_value::<Bee<TransactionId>>()?.into_inner(),
            rows.column_value()?,
        )
        .map_err(|e| anyhow!("{:?}", e))?
        .into())
    }
}
impl Row for Bee<Milestone> {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Milestone::new(
            rows.column_value::<Bee<MessageId>>()?.into_inner(),
            rows.column_value()?,
        )
        .into())
    }
}
