// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::SyncRange;
use std::{
    collections::{
        hash_map::Entry,
        BTreeMap,
        HashMap,
        VecDeque,
    },
    str::FromStr,
};

impl Select<MessageId, Message> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT message FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl Select<MessageId, Option<MessageMetadata>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!("SELECT metadata FROM {}.messages WHERE message_id = ?", self.name()).into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl Select<MessageId, (Option<Message>, Option<MessageMetadata>)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message, metadata FROM {}.messages WHERE message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl Select<MessageId, (Option<Bee<Message>>, Option<MessageMetadata>)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message, metadata FROM {}.messages WHERE message_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &MessageId) -> T::Return {
        builder.value(&message_id.to_string())
    }
}

impl Select<Partitioned<MessageId>, Paged<VecDeque<Partitioned<ParentRecord>>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index, message_id, inclusion_state
            FROM {}.parents
            WHERE parent_id = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, message_id: &Partitioned<MessageId>) -> T::Return {
        builder
            .value(&message_id.to_string())
            .value(&message_id.partition_id())
            .value(&message_id.milestone_index())
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
                    ParentRecord::new(message_id, inclusion_state),
                    partition_id,
                    milestone_index,
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<Partitioned<Indexation>, Paged<VecDeque<Partitioned<IndexationRecord>>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index, message_id, inclusion_state
            FROM {}.indexes
            WHERE indexation = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, index: &Partitioned<Indexation>) -> T::Return {
        builder
            .value(&index.0)
            .value(&index.partition_id())
            .value(&index.milestone_index())
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
                    IndexationRecord::new(message_id, inclusion_state),
                    partition_id,
                    milestone_index,
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Some(Paged::new(values, paging_state)))
    }
}

impl Select<Partitioned<Ed25519Address>, Paged<VecDeque<Partitioned<AddressRecord>>>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT partition_id, milestone_index, output_type, transaction_id, idx, amount, inclusion_state
            FROM {}.addresses
            WHERE address = ? AND partition_id = ? AND milestone_index <= ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, address: &Partitioned<Ed25519Address>) -> T::Return {
        builder
            .value(&address.to_string())
            .value(&address.partition_id())
            .value(&address.milestone_index())
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
                AddressRecord::new(output_type, transaction_id, index, amount, inclusion_state),
                partition_id,
                milestone_index,
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

impl Select<OutputId, OutputRes> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data, inclusion_state
            FROM {}.transactions
            WHERE transaction_id = ?
            AND idx = ?
            AND variant IN ('output', 'unlock')",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, output_id: &OutputId) -> T::Return {
        builder
            .value(&output_id.transaction_id().to_string())
            .value(&output_id.index())
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

impl Select<TransactionId, TransactionRes> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, data, idx, inclusion_state, milestone_index
            FROM {}.transactions
            WHERE transaction_id = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, txn_id: &TransactionId) -> T::Return {
        builder.value(&txn_id.to_string())
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

impl Select<TransactionId, Option<MessageId>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id FROM {}.transactions
            WHERE transaction_id = ? and inclusion_state = ? and variant = 'input'
            LIMIT 1 ALLOW FILTERING",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, transaction_id: &TransactionId) -> T::Return {
        builder
            .value(&transaction_id.to_string())
            .value(&LedgerInclusionState::Included)
    }
}

impl Select<MilestoneIndex, Milestone> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT message_id, timestamp FROM {}.milestones WHERE milestone_index = ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, index: &MilestoneIndex) -> T::Return {
        builder.value(&index.0)
    }
}

impl Select<Hint, Vec<(MilestoneIndex, PartitionId)>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT milestone_index, partition_id
            FROM {}.hints
            WHERE hint = ? AND variant = ?",
            self.name()
        )
        .into()
    }

    fn bind_values<T: Values>(builder: T, hint: &Hint) -> T::Return {
        builder.value(&hint.hint).value(&hint.variant.to_string())
    }
}

impl Select<SyncKey, Iter<SyncRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT milestone_index, synced_by, logged_by FROM {}.sync WHERE key = ? AND milestone_index >= ? AND milestone_index < ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, sync_key: &SyncKey) -> T::Return {
        builder
            .value(&"permanode")
            .value(&sync_key.start())
            .value(&sync_key.end())
    }
}

impl Select<SyncRange, Iter<AnalyticRecord>> for ChronicleKeyspace {
    type QueryOrPrepared = QueryStatement;
    fn statement(&self) -> std::borrow::Cow<'static, str> {
        format!(
            "SELECT milestone_index, message_count, transaction_count, transferred_tokens FROM {}.analytics WHERE key = ? AND milestone_index >= ? AND milestone_index < ?",
            self.name()
        )
        .into()
    }
    fn bind_values<T: Values>(builder: T, sync_range: &SyncRange) -> T::Return {
        builder
            .value(&"permanode")
            .value(&sync_range.from)
            .value(&sync_range.to)
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
