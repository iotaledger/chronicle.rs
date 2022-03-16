// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    filter::FilterBuilder,
    MilestoneDataSearch,
};
use async_trait::async_trait;
use backstage::core::{
    Actor,
    ActorResult,
    IntervalChannel,
    IntervalHandle,
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
    unlock_block::UnlockBlock,
};
use chronicle_storage::{
    access::{
        LedgerInclusionState,
        MessageRecord,
        MilestoneData,
        MilestoneDataBuilder,
        Selected,
    },
    mongodb::{
        options::ClientOptions,
        Client,
        Database,
    },
};
use chrono::NaiveDateTime;
use crypto::hashes::{
    blake2b::Blake2b256,
    Digest,
};
use futures::stream::StreamExt;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fmt::Debug,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
pub struct PermanodeConfig {
    client_opts: ClientOptions,
    retries: u8,
}

// TODO
#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
pub struct SelectivePermanodeConfig;

#[derive(Clone, Debug)]
/// user-defined actor
pub struct Uda {
    database: Database,
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
impl FilterBuilder for SelectivePermanodeConfig {
    type Actor = Uda;
    async fn build(&self) -> anyhow::Result<(Self::Actor, IntervalChannel<1000>)> {
        todo!()
    }
    async fn filter_message(
        &self,
        _handle: &IntervalHandle,
        message: &MessageRecord,
    ) -> anyhow::Result<Option<Selected>> {
        todo!()
    }
    async fn process_milestone_data(
        &self,
        _handle: &IntervalHandle,
        milestone_data: MilestoneDataBuilder,
    ) -> anyhow::Result<MilestoneData> {
        todo!()
    }
}

#[async_trait]
impl FilterBuilder for PermanodeConfig {
    type Actor = Uda;
    async fn build(&self) -> anyhow::Result<(Self::Actor, IntervalChannel<1000>)> {
        let client = Client::with_options(self.client_opts.clone())?;
        let database = client.database("permanode");
        Ok((Uda { database }, IntervalChannel))
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
        // TODO: Probably need to reorganize this or something
        let client = Client::with_options(self.client_opts.clone())?;
        let database = client.database("permanode");
        let message_records = database.collection::<MessageRecord>("messages");
        let mut milestone_data = MilestoneData::new(milestone);
        println!("created empty milestone data");
        while let Some(mut message) = milestone_data_search.next().await {
            // persist the message as confirmed
            message_records
                .insert_one(&message, None)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
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
