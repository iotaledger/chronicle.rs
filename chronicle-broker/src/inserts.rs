// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::solidifier::{
    SolidifierEvent,
    SolidifierHandle,
};
use bee_message::{
    input::Input,
    output::{
        Output,
        OutputId,
    },
    payload::{
        transaction::TransactionEssence,
        Payload,
        TransactionPayload,
    },
};

/// Insert the parents' message ids of a given message id to the table
pub(crate) fn insert_parents<I: Inherent<ChronicleKeyspace, ParentRecord, ()>>(
    keyspace: &ChronicleKeyspace,
    inherent_worker: &I,
    message: &MessageRecord,
) -> anyhow::Result<()> {
    for parent_id in message.parents().iter() {
        let parent_record = ParentRecord::new(
            *parent_id,
            message.milestone_index,
            todo!(),
            message.message_id,
            message.inclusion_state,
        );
        insert(keyspace, inherent_worker, parent_record, ())?;
    }
    Ok(())
}

pub(crate) fn insert_payload<
    I: Inherent<ChronicleKeyspace, LegacyOutputRecord, ()>
        + Inherent<ChronicleKeyspace, LegacyOutputRecord, TTL>
        + Inherent<ChronicleKeyspace, TransactionRecord, ()>
        + Inherent<ChronicleKeyspace, MilestoneRecord, ()>
        + Inherent<ChronicleKeyspace, TagRecord, ()>
        + Inherent<ChronicleKeyspace, TagRecord, TTL>
        + Inherent<ChronicleKeyspace, TagHint, MsRangeId>
        + Inherent<ChronicleKeyspace, AddressHint, MsRangeId>,
>(
    keyspace: &ChronicleKeyspace,
    inherent_worker: &I,
    payload: &Payload,
    message: &MessageRecord,
    solidifier_handles: Option<&HashMap<u8, SolidifierHandle>>,
    selected_opt: Option<Selected>,
) -> anyhow::Result<()> {
    match payload {
        Payload::Indexation(indexation) => {
            insert_index(keyspace, inherent_worker, hex::encode(indexation.index()), message)?;
        }
        Payload::Transaction(transaction) => {
            insert_transaction(
                keyspace,
                inherent_worker,
                &*transaction,
                message,
                solidifier_handles,
                selected_opt,
            )?;
        }
        Payload::Milestone(milestone) => {
            let ms_index = milestone.essence().index();
            let parents_check = message.parents().eq(milestone.essence().parents());
            if message.inclusion_state.is_some() && parents_check {
                // push to the right solidifier
                let solidifier_id = solidifier_handles
                    .map(|h| message.milestone_index.unwrap_or_default().0 % h.len() as u32)
                    .unwrap_or_default() as u8;
                if let Some(solidifier_handle) = solidifier_handles.and_then(|h| h.get(&solidifier_id)) {
                    let ms_message = MilestoneMessage::new(message.clone());
                    let _ = solidifier_handle.send(SolidifierEvent::Milestone(ms_message, selected_opt));
                };
                insert(
                    keyspace,
                    inherent_worker,
                    MilestoneRecord::new(ms_index, message.message_id, (&**milestone).clone()),
                    (),
                )?
            }
        }

        // remaining payload types
        e => {
            warn!("Skipping unsupported payload variant: {:?}", e);
        }
    }
    Ok(())
}
/// Insert the `Indexation` of a given message id to the table
pub(crate) fn insert_index<
    I: Inherent<ChronicleKeyspace, TagRecord, ()>
        + Inherent<ChronicleKeyspace, TagRecord, TTL>
        + Inherent<ChronicleKeyspace, TagHint, MsRangeId>,
>(
    keyspace: &ChronicleKeyspace,
    inherent_worker: &I,
    index: String,
    message: &MessageRecord,
) -> anyhow::Result<()> {
    let milestone_index = message.milestone_index.unwrap_or_default();
    let ms_range_id = message
        .milestone_index
        .map(|ms| TagRecord::range_id(ms.0))
        .unwrap_or(u32::MAX);

    let index_record = TagRecord {
        tag: index,
        partition_data: PartitionData::new(ms_range_id, milestone_index, todo!()),
        message_id: message.message_id,
        inclusion_state: message.inclusion_state,
    };
    insert(
        keyspace,
        inherent_worker,
        index_record,
        todo!("Figure out how to handle TTL"),
    )?;
    // insert hint record
    let hint = TagHint {
        tag: index,
        table_kind: TagHintVariant::Regular,
    };
    insert(keyspace, inherent_worker, hint, ms_range_id)
}

/// Insert the transaction to the table
pub(crate) fn insert_transaction<
    I: Inherent<ChronicleKeyspace, LegacyOutputRecord, ()>
        + Inherent<ChronicleKeyspace, LegacyOutputRecord, TTL>
        + Inherent<ChronicleKeyspace, TagRecord, ()>
        + Inherent<ChronicleKeyspace, TagRecord, TTL>
        + Inherent<ChronicleKeyspace, TransactionRecord, ()>
        + Inherent<ChronicleKeyspace, TagHint, MsRangeId>
        + Inherent<ChronicleKeyspace, AddressHint, MsRangeId>
        + Inherent<ChronicleKeyspace, MilestoneRecord, ()>,
>(
    keyspace: &ChronicleKeyspace,
    inherent_worker: &I,
    transaction: &TransactionPayload,
    message: &MessageRecord,
    solidifier_handles: Option<&HashMap<u8, SolidifierHandle>>,
    selected_opt: Option<Selected>,
) -> anyhow::Result<()> {
    let transaction_id = transaction.id();
    let unlock_blocks = transaction.unlock_blocks();
    let TransactionEssence::Regular(regular) = transaction.essence();
    {
        for (idx, input) in regular.inputs().iter().enumerate() {
            // insert utxoinput row along with input row
            match input {
                Input::Utxo(utxo_input) => {
                    let unlock_block = &unlock_blocks[idx];
                    let input_data = InputData::utxo(utxo_input.clone(), unlock_block.clone());
                    // insert input row
                    insert_transaction_record(
                        keyspace,
                        inherent_worker,
                        TransactionRecord::input(
                            transaction_id,
                            idx as u16,
                            message.message_id,
                            input_data,
                            message.inclusion_state,
                            message.milestone_index,
                        ),
                        message,
                    )?;
                    // this is the spent_output which the input is spending from
                    let output_id = utxo_input.output_id();
                    // therefore we insert utxo_input.output_id() -> unlock_block to indicate that this output
                    // is_spent;
                    let unlock_data = UnlockData::new(transaction_id, idx as u16, unlock_block.clone());
                    insert_transaction_record(
                        keyspace,
                        inherent_worker,
                        TransactionRecord::unlock(
                            transaction_id,
                            idx as u16,
                            message.message_id,
                            unlock_data,
                            message.inclusion_state,
                            message.milestone_index,
                        ),
                        message,
                    )?;
                }
                Input::Treasury(treasury_input) => {
                    let input_data = InputData::treasury(treasury_input.clone());
                    // insert input row
                    insert_transaction_record(
                        keyspace,
                        inherent_worker,
                        TransactionRecord::input(
                            transaction_id,
                            idx as u16,
                            message.message_id,
                            input_data,
                            message.inclusion_state,
                            message.milestone_index,
                        ),
                        message,
                    )?;
                }
            }
        }
        for (idx, output) in regular.outputs().iter().enumerate() {
            // insert output row
            insert_transaction_record(
                keyspace,
                inherent_worker,
                TransactionRecord::output(
                    transaction_id,
                    idx as u16,
                    message.message_id,
                    output.clone(),
                    message.inclusion_state,
                    message.milestone_index,
                ),
                message,
            )?;
            // insert address row
            insert_legacy_output(
                keyspace,
                inherent_worker,
                OutputId::new(transaction_id, idx as u16).unwrap(),
                output.clone(),
                message,
            )?;
        }
        if let Some(payload) = regular.payload() {
            insert_payload(
                keyspace,
                inherent_worker,
                payload,
                message,
                solidifier_handles,
                selected_opt,
            )?
        }
    };
    Ok(())
}

pub(crate) fn insert_transaction_record<I: Inherent<ChronicleKeyspace, TransactionRecord, ()>>(
    keyspace: &ChronicleKeyspace,
    inherent_worker: &I,
    transaction: TransactionRecord,
    message: &MessageRecord,
) -> anyhow::Result<()> {
    insert(keyspace, inherent_worker, transaction, ())
}
/// Insert the `Address` to the table
pub(crate) fn insert_legacy_output<
    I: Inherent<ChronicleKeyspace, LegacyOutputRecord, ()>
        + Inherent<ChronicleKeyspace, LegacyOutputRecord, TTL>
        + Inherent<ChronicleKeyspace, AddressHint, MsRangeId>,
>(
    keyspace: &ChronicleKeyspace,
    inherent_worker: &I,
    output_id: OutputId,
    output: Output,
    message: &MessageRecord,
) -> anyhow::Result<()> {
    let milestone_index = message.milestone_index.unwrap_or_default();
    let ms_range_id = message
        .milestone_index
        .map(|ms| ms.0 / LegacyOutputRecord::MS_CHUNK_SIZE)
        .unwrap_or(u32::MAX);

    let hint = AddressHint {
        address: match output {
            Output::SignatureLockedSingle(o) => *o.address(),
            Output::SignatureLockedDustAllowance(o) => *o.address(),
            _ => Err(anyhow::anyhow!("Invalid legacy output type!"))?,
        },
        output_table: OutputTable::Legacy,
        variant: AddressHintVariant::Address,
    };
    let record = LegacyOutputRecord::created(
        output_id,
        PartitionData::new(ms_range_id, milestone_index, todo!()),
        message.inclusion_state,
        output,
        message.message_id,
    )?;
    insert(keyspace, inherent_worker, record, todo!("Handle TTL"))?;
    insert(keyspace, inherent_worker, hint, ms_range_id);
    Ok(())
}
/// The low-level insert function to insert a key/value pair through an inherent worker
pub(crate) fn insert<I, K, V>(keyspace: &ChronicleKeyspace, inherent_worker: &I, key: K, value: V) -> anyhow::Result<()>
where
    I: Inherent<ChronicleKeyspace, K, V>,
    ChronicleKeyspace: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
    V: 'static + Send + Sync + Clone + Debug,
{
    let insert_req = keyspace
        .insert(&key, &value)
        .consistency(Consistency::One)
        .build()
        .map_err(|e| ActorError::exit(e))?;
    let worker = inherent_worker.inherent_boxed(keyspace.clone(), key, value);
    if let Err(RequestError::Ring(r)) = insert_req.send_local_with_worker(worker) {
        let keyspace_name = keyspace.name();
        if let Err(worker) = retry_send(&keyspace_name, r, 2) {
            worker.handle_error(WorkerError::NoRing, None)?;
        };
    };
    Ok(())
}
