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
    output::Output,
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
    async fn build(&self) -> anyhow::Result<(Self::Actor, AbortableUnboundedChannel<()>)> {
        Ok((
            Uda {
                keyspace: self.keyspace.clone(),
            },
            AbortableUnboundedChannel::new(),
        ))
    }
    async fn filter_message(
        &self,
        _handle: &AbortableUnboundedHandle<()>,
        _message: &MessageRecord,
    ) -> anyhow::Result<Option<Selected>> {
        Ok(Some(Selected::select()))
    }
    async fn process_milestone_data_builder(
        &self,
        _handle: &AbortableUnboundedHandle<()>,
        milestone_data: MilestoneDataBuilder,
    ) -> anyhow::Result<MilestoneData> {
        let milestone_index = milestone_data.milestone_index();
        let milestone_timestamp_secs = milestone_data
            .timestamp()
            .ok_or_else(|| anyhow::anyhow!("No milestone timestamp"))?;
        let dt = NaiveDateTime::from_timestamp(milestone_timestamp_secs as i64, 0);
        let date = DateTime::<Utc>::from_utc(dt, Utc);
        let epoch = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
        let diff = date.signed_duration_since(epoch);
        diff.num_days();
        // let s = Utc.from_utc_date(&dt);
        // convert it to searchable struct
        let mut milestone_data_search: MilestoneDataSearch = milestone_data.try_into()?;
        // The accumulators
        let mut transaction_count: u32 = 0;
        let mut message_count: u32 = 0;
        let mut transferred_tokens: u64 = 0;
        let mut addresses: HashMap<Address, (Sent, Recv, SentOrRecv)> = HashMap::new();
        while let Some((mut proof_opt, message)) = milestone_data_search.next().await {
            // Insert the proof(if any)
            if let Some(proof) = proof_opt.take() {
                let req = self
                    .keyspace
                    .insert(&Bee(message.message_id), &proof)
                    .consistency(Consistency::Quorum)
                    .build()?;
                let worker = AtomicProcessWorker::boxed(
                    atomic_handle.clone(),
                    self.keyspace.clone(),
                    Bee(message.message_id),
                    proof.clone(),
                    5,
                );
                let keyspace_name = self.keyspace.name();
                if let Err(RequestError::Ring(r)) = req.send_local_with_worker(worker) {
                    if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                        worker.handle_error(WorkerError::NoRing, None)?
                    };
                };
            }
            // Accumulate the message count
            message_count += 1;
            // Accumulate confirmed(included) transaction value
            if let Some(LedgerInclusionState::Included) = message.inclusion_state {
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
        }
        // insert cache records
        todo!("insert cache records");

        Ok(())
    }
}

/// The Lifecycle of selective permanode
#[async_trait]
impl<S> Actor<S> for Uda
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = AbortableUnboundedChannel<()>;

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
