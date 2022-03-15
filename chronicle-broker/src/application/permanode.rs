use super::ChronicleKeyspace;
use crate::{
    filter::{
        FilterBuilder,
        *,
    },
    MilestoneDataSearch,
};

use async_trait::async_trait;
use backstage::core::{
    Actor,
    ActorResult,
    Rt,
    SupHandle,
};
use bee_message::{
    address::{
        Address,
        Ed25519Address,
    },
    input::Input,
    output::{
        Output,
        SignatureLockedSingleOutput,
    },
    payload::{
        transaction::TransactionEssence,
        Payload,
    },
    signature::Signature,
    unlock_block::{
        SignatureUnlockBlock,
        UnlockBlock,
    },
};
use chronicle_storage::access::{
    Bee,
    LedgerInclusionState,
    MessageRecord,
    MilestoneData,
    MilestoneDataBuilder,
    Selected,
    *,
};
use chrono::{
    DateTime,
    NaiveDateTime,
    TimeZone,
    Utc,
};
use crypto::hashes::{
    blake2b::Blake2b256,
    Digest,
};
use futures::stream::StreamExt;
use scylla_rs::prelude::*;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PermanodeConfig {
    keyspace: ChronicleKeyspace,
    retries: u8,
}

#[derive(Clone, Debug, Default)]
/// user-defined actor
pub struct Uda {
    keyspace: ChronicleKeyspace,
}

enum UdaEvent {
    Shutdown,
}

#[derive(Default, Clone, Copy, Debug, Eq, PartialEq)]
struct Sent(bool);
#[derive(Default, Clone, Copy, Debug, Eq, PartialEq)]
struct Recv(bool);
#[derive(Default, Clone, Copy, Debug, Eq, PartialEq)]
struct SentOrRecv(bool);

#[async_trait]
impl FilterBuilder for PermanodeConfig {
    type Actor = Uda;
    async fn build(&self) -> anyhow::Result<(Self::Actor, IntervalChannel<1000>)> {
        Ok((
            Uda {
                keyspace: self.keyspace.clone(),
            },
            IntervalChannel,
        ))
    }
    async fn filter_message(
        &self,
        _handle: &IntervalHandle,
        message: &MessageRecord,
    ) -> anyhow::Result<Option<Selected>> {
        // persist_new_message(&self.keyspace, message, &BasicHandle, self.retries, TTL::new(3600));
        Ok(Some(Selected::select()))
    }
    async fn process_milestone_data(
        &self,
        _handle: &IntervalHandle,
        milestone_data: MilestoneDataBuilder,
    ) -> anyhow::Result<MilestoneData> {
        println!("processing milestone data");
        if !milestone_data.valid() {
            anyhow::bail!("Cannot process milestone data using invalid milestone data builder")
        }
        println!("Valid milestone data");
        let milestone_index = milestone_data.milestone_index();
        let milestone_timestamp_secs = milestone_data
            .timestamp()
            .ok_or_else(|| anyhow::anyhow!("No milestone timestamp"))?;
        let ms_timestamp = NaiveDateTime::from_timestamp(milestone_timestamp_secs as i64, 0);
        // convert it to searchable struct
        let milestone = milestone_data.milestone().clone().unwrap();
        // Convert milestone data builder
        println!("converting milestone data into searchable");
        let mut milestone_data_search: MilestoneDataSearch = milestone_data.try_into()?;
        println!("converted milestone data into searchable");

        // The accumulators
        let mut transaction_count: u32 = 0;
        let mut message_count: u32 = 0;
        let mut transferred_tokens: u64 = 0;
        let mut addresses: HashMap<Address, (Sent, Recv, SentOrRecv)> = HashMap::new();
        // create onshot
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let handle = AtomicProcessHandle::new(sender, milestone_index);
        let mut milestone_data = MilestoneData::new(milestone);
        println!("created empty milestone data");
        while let Some(mut message) = milestone_data_search.next().await {
            // persist the message as confirmed
            persist_confirmed_message(
                &self.keyspace,
                message.clone(),
                milestone_index,
                ms_timestamp,
                &handle,
                self.retries,
            );
            // Accumulate the message count
            message_count += 1;
            // Accumulate confirmed(included) transaction value
            if let Some(LedgerInclusionState::Included) = message.inclusion_state.as_ref() {
                if let Some(Payload::Transaction(payload)) = message.payload() {
                    // Accumulate the transaction count
                    transaction_count += 1;
                    let TransactionEssence::Regular(regular_essence) = payload.essence();
                    {
                        for output in regular_essence.outputs() {
                            match output {
                                // Accumulate the transferred token amount
                                Output::SignatureLockedSingle(output) => {
                                    transferred_tokens += output.amount();
                                    let (_sent, recv, any) = addresses.entry(output.address().clone()).or_default();
                                    recv.0 = true;
                                    any.0 = true;
                                }
                                Output::SignatureLockedDustAllowance(output) => transferred_tokens += output.amount(),
                                // Note that the transaction payload don't have Treasury
                                _ => anyhow::bail!("Unexpected Output variant in transaction payload"),
                            }
                        }
                    }
                    let unlock_blocks = payload.unlock_blocks().iter();
                    for unlock in unlock_blocks {
                        if let UnlockBlock::Signature(sig) = unlock {
                            let Signature::Ed25519(s) = sig.signature();
                            let address =
                                Address::Ed25519(Ed25519Address::new(Blake2b256::digest(s.public_key()).into()));
                            let (sent, _recv, any) = addresses.entry(address).or_default();
                            sent.0 = true;
                            any.0 = true;
                        }
                    }
                }
            }
            // add it to milestone data
            milestone_data.messages.insert(message);
        }
        println!("awaiting receiver ms data");
        // insert cache records
        // todo!("insert cache records");
        // await till receiver is finialized
        // receiver.await?;
        Ok(milestone_data)
    }
}

/// The Lifecycle of selective permanode
#[async_trait]
impl<S> Actor<S> for Uda
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = IntervalChannel<1000>;

    async fn init(&mut self, _rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<()> {
        while let Some(_) = rt.inbox_mut().next().await {
            break;
        }
        Ok(())
    }
}

pub(crate) fn persist_confirmed_message<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    message: MessageRecord,
    milestone_index: u32,
    ms_timestamp: NaiveDateTime,
    handle: &I,
    retries: u8,
) -> anyhow::Result<()> {
    // insert message record into messages table
    insert_message(keyspace, handle, message.clone(), retries)

    // insert parents into parents table
    // insert_parents(keyspace, handle, &message, Some(ms_timestamp), retries)?;

    // insert payload
    // insert_payload(keyspace, handle, &message, Some(ms_timestamp), None, retries)
}

pub(crate) fn persist_new_message<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    message: &MessageRecord,
    handle: &I,
    retries: u8,
    ttl: TTL,
) {
    // insert message record into messages table
    insert_message_maybe_ttl(keyspace, handle, message.clone(), ttl, retries);
    // insert parents into parents table
    insert_parents(keyspace, handle, &message, None, retries);
    // insert payload
    insert_payload(keyspace, handle, &message, None, Some(ttl), retries);
}

/// Insert the message record
fn insert_message<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    handle: &I,
    message: MessageRecord,
    retries: u8,
) -> anyhow::Result<()> {
    create_and_send_request(keyspace, message, (), retries, handle)
}

/// Insert the message record with time to live duration
fn insert_message_maybe_ttl<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    handle: &I,
    mut message: MessageRecord,
    ttl: TTL,
    retries: u8,
) -> anyhow::Result<()> {
    if !message.inclusion_state().is_some() {
        // make sure message milestone exist
        message.milestone_index.replace(
            message
                .milestone_index
                .map(|m| m)
                .unwrap_or(bee_message::milestone::MilestoneIndex(u32::MAX)),
        );
        create_and_send_request(keyspace, message, ttl, retries, handle)?;
    } else {
        insert_message(keyspace, handle, message, retries)?;
    }
    Ok(())
}

/// Insert the message parents
fn insert_parents<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    handle: &I,
    message: &MessageRecord,
    ms_timestamp: Option<NaiveDateTime>,
    retries: u8,
) -> anyhow::Result<()> {
    for parent_id in message.parents().iter() {
        let parent_record = ParentRecord::new(
            *parent_id,
            message.milestone_index,
            ms_timestamp,
            message.message_id,
            message.inclusion_state,
        );
        create_and_send_request(keyspace, parent_record, (), retries, handle)?
    }
    Ok(())
}

/// Insert the message parents
fn insert_payload<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    handle: &I,
    message: &MessageRecord,
    ms_timestamp: Option<NaiveDateTime>,
    ttl: Option<TTL>,
    retries: u8,
) -> anyhow::Result<()> {
    if let Some(payload) = message.payload() {
        match payload {
            // todo handle tagged data.
            Payload::Indexation(indexation) => insert_index(
                keyspace,
                message,
                hex::encode(indexation.index()),
                ms_timestamp,
                ttl,
                handle,
                retries,
            )?,
            Payload::Transaction(transaction) => {
                insert_transaction(keyspace, message, transaction, ms_timestamp, ttl, handle, retries)?;
            }
            Payload::Milestone(milestone) => {
                let ms_index = milestone.essence().index();
                let parents_check = message.parents().eq(milestone.essence().parents());
                if message.inclusion_state.is_some() && parents_check {
                    let ms_record = MilestoneRecord::new(ms_index, message.message_id, (&**milestone).clone());
                    create_and_send_request(keyspace, ms_record, (), retries, handle)?;
                    // check if we have to insert receipt payload
                    if let Some(Payload::Receipt(receipt_payload)) = milestone.essence().receipt() {
                        let milestone_id = milestone.id();
                        let transaction_id = todo!("milestone_id.into()");
                        let legacy_ms_range_id = LegacyOutputRecord::range_id(ms_index.0);
                        let ms_timestamp = ms_timestamp
                            .unwrap_or(NaiveDateTime::from_timestamp(milestone.essence().timestamp() as i64, 0));

                        for (idx, entry) in receipt_payload.funds().iter().enumerate() {
                            // simulate legacy output record
                            let address = entry.address();
                            let amount = entry.amount();
                            let output =
                                Output::SignatureLockedSingle(SignatureLockedSingleOutput::new(*address, amount)?);
                            let output_record = LegacyOutputRecord::created(
                                bee_message::output::OutputId::new(transaction_id, idx as u16)?,
                                PartitionData::new(legacy_ms_range_id, ms_index, ms_timestamp),
                                message.inclusion_state,
                                output,
                                message.message_id,
                            )?;
                            create_and_send_request(keyspace, output_record, (), retries, handle)?;
                        }
                        let receipt_record = TransactionRecord::receipt(
                            milestone_id,
                            message.message_id,
                            **receipt_payload,
                            message.inclusion_state,
                            Some(ms_index),
                        );
                        create_and_send_request(keyspace, receipt_record, (), retries, handle)?;
                    };
                }
            }
            // remaining payload types
            e => {
                log::warn!("Skipping unsupported payload variant: {:?}", e);
            }
        }
    }
    Ok(())
}

/// Insert the `Indexation` of a given message id to the table
fn insert_index<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    message: &MessageRecord,
    index: String,
    timestamp: Option<NaiveDateTime>,
    mut ttl: Option<TTL>,
    handle: &I,
    retries: u8,
) -> anyhow::Result<()> {
    let milestone_index = message.milestone_index.unwrap_or_default();
    let ms_range_id = message
        .milestone_index
        .map(|ms| TagRecord::range_id(ms.0))
        .unwrap_or(u32::MAX);
    let index_record = TagRecord {
        tag: index.clone(),
        partition_data: PartitionData::new(
            ms_range_id,
            milestone_index,
            timestamp.unwrap_or(Utc::now().naive_utc()),
        ),
        message_id: message.message_id,
        inclusion_state: message.inclusion_state,
    };
    if let Some(ttl) = ttl.as_ref() {
        create_and_send_request(keyspace, index_record, ttl.clone(), retries, handle)?;
    } else {
        create_and_send_request(keyspace, index_record, (), retries, handle)?;
    }
    // insert hint record
    let hint = TagHint {
        tag: index,
        table_kind: TagHintVariant::Regular,
    };
    create_and_send_request(keyspace, hint, ms_range_id, retries, handle)
}

/// Insert the transaction to the table
fn insert_transaction<I: Inherent>(
    keyspace: &ChronicleKeyspace,
    message: &MessageRecord,
    transaction: &bee_message::payload::TransactionPayload,
    timestamp: Option<NaiveDateTime>,
    mut ttl: Option<TTL>,
    handle: &I,
    retries: u8,
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
                    let input_record = TransactionRecord::input(
                        transaction_id,
                        idx as u16,
                        message.message_id,
                        input_data,
                        message.inclusion_state,
                        message.milestone_index,
                    );
                    // this is the spent_output which the input is spending from
                    let output_id = utxo_input.output_id();
                    let spent_output_tx_id = output_id.transaction_id();
                    let spent_output_idx = output_id.index();
                    // therefore we insert utxo_input.output_id() -> unlock_block to indicate that this output
                    // is_spent;
                    let unlock_data = UnlockData::new(transaction_id, idx as u16, unlock_block.clone());
                    let unlock_record = TransactionRecord::unlock(
                        *spent_output_tx_id,
                        spent_output_idx,
                        message.message_id,
                        unlock_data,
                        message.inclusion_state,
                        message.milestone_index,
                    );
                    if let Some(ttl) = ttl.as_ref() {
                        create_and_send_request(keyspace, input_record, ttl.clone(), retries, handle)?;
                        create_and_send_request(keyspace, unlock_record, ttl.clone(), retries, handle)?;
                    } else {
                        create_and_send_request(keyspace, input_record, (), retries, handle)?;
                        create_and_send_request(keyspace, unlock_record, (), retries, handle)?;
                    }
                }
                Input::Treasury(treasury_input) => {
                    let input_data = InputData::treasury(treasury_input.clone());
                    let treasury_record = TransactionRecord::input(
                        transaction_id,
                        idx as u16,
                        message.message_id,
                        input_data,
                        message.inclusion_state,
                        message.milestone_index,
                    );
                    if let Some(ttl) = ttl.as_ref() {
                        create_and_send_request(keyspace, treasury_record, ttl.clone(), retries, handle)?;
                    } else {
                        create_and_send_request(keyspace, treasury_record, (), retries, handle)?;
                    }
                }
            }
        }

        let ms_range_id = message
            .milestone_index
            .map(|ms| LegacyOutputRecord::range_id(ms.0))
            .unwrap_or(u32::MAX);
        for (idx, output) in regular.outputs().iter().enumerate() {
            // generic output record in transactions table
            let output_record = TransactionRecord::output(
                transaction_id,
                idx as u16,
                message.message_id,
                output.clone(),
                message.inclusion_state,
                message.milestone_index,
            );
            // remaining output record, depends on the output type
            match output {
                Output::SignatureLockedSingle(o) => {
                    let record = LegacyOutputRecord::created(
                        bee_message::output::OutputId::new(transaction_id, idx as u16)?,
                        PartitionData::new(
                            ms_range_id,
                            message
                                .milestone_index
                                .unwrap_or(bee_message::milestone::MilestoneIndex(u32::MAX)),
                            timestamp.unwrap_or(Utc::now().naive_utc()),
                        ),
                        message.inclusion_state,
                        output.clone(),
                        message.message_id,
                    )?;
                    let hint = AddressHint {
                        address: *o.address(),
                        output_table: OutputTable::Legacy,
                        variant: AddressHintVariant::Address,
                    };

                    if let Some(ttl) = ttl.as_ref() {
                        create_and_send_request(keyspace, output_record, ttl.clone(), retries, handle)?;
                        create_and_send_request(keyspace, record, ttl.clone(), retries, handle)?;
                    } else {
                        create_and_send_request(keyspace, output_record, (), retries, handle)?;
                        create_and_send_request(keyspace, record, (), retries, handle)?;
                    }
                    create_and_send_request(keyspace, hint, ms_range_id, retries, handle)?;
                }
                Output::SignatureLockedDustAllowance(o) => {
                    let record = LegacyOutputRecord::created(
                        bee_message::output::OutputId::new(transaction_id, idx as u16)?,
                        PartitionData::new(
                            ms_range_id,
                            message
                                .milestone_index
                                .unwrap_or(bee_message::milestone::MilestoneIndex(u32::MAX)),
                            timestamp.unwrap_or(Utc::now().naive_utc()),
                        ),
                        message.inclusion_state,
                        output.clone(),
                        message.message_id,
                    )?;
                    let hint = AddressHint {
                        address: *o.address(),
                        output_table: OutputTable::Legacy,
                        variant: AddressHintVariant::Address,
                    };
                    if let Some(ttl) = ttl.as_ref() {
                        create_and_send_request(keyspace, record, ttl.clone(), retries, handle)?;
                    } else {
                        create_and_send_request(keyspace, record, (), retries, handle)?;
                    }
                    create_and_send_request(keyspace, hint, ms_range_id, retries, handle)?;
                }

                _ => todo!("Support remaining output types"),
            }
        }
        insert_payload(keyspace, handle, message, timestamp, ttl, retries)?;
    };
    Ok(())
}

fn create_and_send_request<I: Inherent, K, V>(
    keyspace: &ChronicleKeyspace,
    key: K,
    value: V,
    retries: u8,
    handle: &I,
) -> anyhow::Result<()>
where
    ChronicleKeyspace: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
    V: 'static + Send + Sync + Clone + Debug,
{
    match create_request(keyspace, key, value) {
        Ok(request) => {
            let worker = handle.clone().atomic_worker(request.clone(), retries);
            let token = request.token();
            let payload: Vec<u8> = request.into();
            if let Err(r) = send_local(Some(keyspace.as_str()), token, payload, worker) {
                if let Err(worker) = retry_send(Some(keyspace.as_str()), r, 1) {
                    worker.handle_error(WorkerError::NoRing, None)?
                };
            };
            Ok(())
        }
        Err(e) => {
            handle.set_error();
            anyhow::bail!("Unable to create request, error: {}", e)
        }
    }
}

/// The low-level create request function to insert a key/value pair through an inherent worker
fn create_request<K, V>(keyspace: &ChronicleKeyspace, key: K, value: V) -> anyhow::Result<CommonRequest>
where
    ChronicleKeyspace: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
    V: 'static + Send + Sync + Clone + Debug,
{
    Ok(keyspace
        .insert(&key, &value)
        .consistency(Consistency::Quorum)
        .build()?
        .into())
}
