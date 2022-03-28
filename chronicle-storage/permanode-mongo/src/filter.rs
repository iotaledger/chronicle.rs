// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::core::{
    AbortableUnboundedChannel,
    AbortableUnboundedHandle,
    Actor,
    ActorError,
    ActorResult,
    Rt,
    SupHandle,
};
use chronicle_broker::filter::{
    FilterBuilder,
    FilterHandle,
};
use chronicle_common::{
    config::mongo::MongoConfig,
    mongodb::{
        options::ClientOptions,
        Client,
    },
    types::{
        MessageRecord,
        MilestoneData,
        MilestoneDataBuilder,
        MilestoneDataSearch,
        Selected,
        SyncData,
        SyncRecord,
    },
};
use futures::{
    stream::StreamExt,
    TryStreamExt,
};
use mongodb::{
    bson::{
        doc,
        Document,
    },
    options::UpdateOptions,
};
use std::{
    ops::Range,
    time::Duration,
};

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
/// The permanode mongo config
pub struct PermanodeMongoConfig {
    database_name: String,
    mongo_config: MongoConfig,
}

#[derive(Debug, Clone)]
/// Permanode mongo filter handle
pub struct PermanodeMongoHandle {
    database_name: String,
    client: Client,
}

#[async_trait]
impl FilterBuilder for PermanodeMongoConfig {
    type Actor = Uda;
    type Handle = PermanodeMongoHandle;
    async fn build(&self) -> ActorResult<Self::Actor> {
        Ok(Uda)
    }
    async fn handle(&self, _handle: AbortableUnboundedHandle<()>) -> ActorResult<Self::Handle> {
        let client_opts: ClientOptions = self.mongo_config.clone().into();
        let client = Client::with_options(client_opts.clone())
            .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?;
        Ok(PermanodeMongoHandle {
            database_name: self.database_name.clone(),
            client,
        })
    }
}

#[async_trait]
impl FilterHandle for PermanodeMongoHandle {
    async fn filter_message(&self, _message: &MessageRecord) -> ActorResult<Option<Selected>> {
        Ok(Some(Selected::select()))
    }
    async fn export_milestone_data(&self, _milestone_index: u32) -> ActorResult<Option<MilestoneData>> {
        Ok(None)
    }
    async fn process_milestone_data(&self, milestone_data: MilestoneDataBuilder) -> ActorResult<Option<MilestoneData>> {
        if !milestone_data.valid() {
            return Err(ActorError::aborted_msg(
                "Cannot process milestone data using invalid milestone data builder",
            ))?;
        }
        // convert it to searchable struct
        let milestone = milestone_data.milestone().clone().unwrap();
        // Convert milestone data builder
        let mut milestone_data_search: MilestoneDataSearch = milestone_data.try_into()?;
        let mut milestone_data = MilestoneData::new(milestone);
        while let Some(message) = milestone_data_search.next().await {
            let message_doc: Document = (&message).into();
            // insert into mongo
            self.client
                .database(&self.database_name)
                .collection::<Document>("messages")
                .update_one(
                    doc! { "message_id": message.message_id().to_string() },
                    doc! { "$set": message_doc },
                    UpdateOptions::builder().upsert(true).build(),
                )
                .await
                .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?;
            // add it to milestone data
            milestone_data.messages.insert(message);
        }
        Ok(Some(milestone_data))
    }
    async fn synced(&self, milestone_index: u32) -> ActorResult<()> {
        self.client
            .database(&self.database_name)
            .collection::<SyncRecord>("sync")
            .update_one(
                doc! { "milestone_index": milestone_index },
                doc! { "$set": { "synced_by": 0 } },
                UpdateOptions::builder().upsert(true).build(),
            )
            .await
            .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?;
        Ok(())
    }
    async fn logged(&self, milestone_index: u32) -> ActorResult<()> {
        self.client
            .database(&self.database_name)
            .collection::<SyncRecord>("sync")
            .update_one(
                doc! { "milestone_index": milestone_index },
                doc! { "$set": { "logged_by": 0 } },
                UpdateOptions::builder().upsert(true).build(),
            )
            .await
            .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?;
        Ok(())
    }
    async fn sync_data(&self, range: Option<Range<u32>>) -> ActorResult<SyncData> {
        let sync_range = range.unwrap_or_else(|| 0..i32::MAX as u32);
        let mut res = self
            .client
            .database(&self.database_name)
            .collection::<SyncRecord>("sync")
            .find(
                doc! { "milestone_index": {
                    "$gte": sync_range.start as i32,
                    "$lte": sync_range.end as i32,
                }},
                None,
            )
            .await
            .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?;
        let mut sync_data = SyncData::default();

        if let Some(SyncRecord {
            milestone_index,
            logged_by,
            ..
        }) = res
            .try_next()
            .await
            .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?
        {
            // push missing row/gap (if any)
            sync_data.process_gaps(sync_range.end, milestone_index);
            sync_data.process_rest(&logged_by, milestone_index, &None);
            let mut pre_ms = milestone_index;
            let mut pre_lb = logged_by;
            // Generate and identify missing gaps in order to fill them
            while let Some(SyncRecord {
                milestone_index,
                logged_by,
                ..
            }) = res
                .try_next()
                .await
                .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?
            {
                // check if there are any missings
                sync_data.process_gaps(pre_ms, milestone_index);
                sync_data.process_rest(&logged_by, milestone_index, &pre_lb);
                pre_ms = milestone_index;
                pre_lb = logged_by;
            }
            // pre_ms is the most recent milestone we processed
            // it's also the lowest milestone index in the select response
            // so anything < pre_ms && anything >= (self.sync_range.from - 1)
            // (lower provided sync bound) are missing
            // push missing row/gap (if any)
            sync_data.process_gaps(pre_ms, sync_range.start - 1);
        } else {
            // Everything is missing as gaps
            sync_data.process_gaps(sync_range.end, sync_range.start - 1);
        }
        Ok(SyncData::default())
    }
}

/// User defined actor
pub struct Uda;

/// The Lifecycle of user defined actor
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
