// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    prelude::MilestonePayload,
    Message,
    MessageId,
};
use chronicle_storage::access::{
    AnalyticRecord,
    MessageMetadata,
};
#[cfg(feature = "scylla-rs")]
use scylla_rs::cql::Rows;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashMap,
    ops::Range,
    path::PathBuf,
};
use url::Url;

#[derive(Deserialize, Serialize)]
/// Defines a message to/from the Broker or its children
pub enum BrokerSocketMsg<T> {
    /// A message to/from the Broker
    ChronicleBroker(T),
}

#[derive(Deserialize, Serialize)]
/// It's the Interface of the broker app to dynamiclly configure the application during runtime
pub enum ChronicleBrokerThrough {
    /// Shutdown json to gracefully shutdown broker app
    Shutdown,
    /// Alter the topology of the broker app
    Topology(BrokerTopology),
    /// Exit the broker app
    ExitProgram,
}

/// Topology event
#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum BrokerTopology {
    /// Add new MQTT Messages feed source
    AddMqttMessages(Url),
    /// Add new MQTT Messages Referenced feed source
    AddMqttMessagesReferenced(Url),
    /// Remove a MQTT Messages feed source
    RemoveMqttMessages(Url),
    /// Remove a MQTT Messages Referenced feed source
    RemoveMqttMessagesReferenced(Url),
    /// Import a log file using the given url
    Import {
        /// File or dir path which supposed to contain LogFiles
        path: PathBuf,
        /// Resume the importing process
        resume: bool,
        /// Provide optional import range
        import_range: Option<Range<u32>>,
        /// The type of import requested
        import_type: ImportType,
    },
    /// Add Endpoint
    Requesters(RequesterTopology),
}

/// Import types
#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
pub enum ImportType {
    /// Import everything
    All,
    /// Import only Sync data
    Sync,
    /// Import only Analytics data
    Analytics,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Requester topology used by admins to add/remove IOTA api endpoints
pub enum RequesterTopology {
    /// Add new Api Endpoint
    AddEndpoint(Url),
    /// Remove existing Api Endpoint
    RemoveEndpoint(Url),
}

/// Milestone data
#[derive(Deserialize, Serialize)]
pub struct MilestoneData {
    pub(crate) milestone_index: u32,
    pub(crate) milestone: Option<Box<MilestonePayload>>,
    pub(crate) messages: HashMap<MessageId, FullMessage>,
    pub(crate) pending: HashMap<MessageId, ()>,
    pub(crate) created_by: CreatedBy,
}

impl MilestoneData {
    pub(crate) fn new(milestone_index: u32, created_by: CreatedBy) -> Self {
        Self {
            milestone_index,
            milestone: None,
            messages: HashMap::new(),
            pending: HashMap::new(),
            created_by,
        }
    }
    /// Get the milestone index from this milestone data
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
    pub(crate) fn set_milestone(&mut self, boxed_milestone_payload: Box<MilestonePayload>) {
        self.milestone.replace(boxed_milestone_payload);
    }
    /// Check if the milestone exists
    pub fn milestone_exist(&self) -> bool {
        self.milestone.is_some()
    }
    pub(crate) fn add_full_message(&mut self, full_message: FullMessage) {
        self.messages.insert(*full_message.message_id(), full_message);
    }
    pub(crate) fn remove_from_pending(&mut self, message_id: &MessageId) {
        self.pending.remove(message_id);
    }
    /// Get the milestone's messages
    pub fn messages(&self) -> &HashMap<MessageId, FullMessage> {
        &self.messages
    }
    /// Get the pending messages
    pub fn pending(&self) -> &HashMap<MessageId, ()> {
        &self.pending
    }
    /// Get the source this was created by
    pub fn created_by(&self) -> &CreatedBy {
        &self.created_by
    }
}

impl std::iter::IntoIterator for MilestoneData {
    type Item = (MessageId, FullMessage);
    type IntoIter = std::collections::hash_map::IntoIter<MessageId, FullMessage>;
    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

/// Created by sources
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[repr(u8)]
pub enum CreatedBy {
    /// Created by the new incoming messages from the network
    Incoming = 0,
    /// Created by the new expected messages from the network
    Expected = 1,
    /// Created by solidifiy/sync request from syncer
    Syncer = 2,
}

impl From<CreatedBy> for u8 {
    fn from(value: CreatedBy) -> u8 {
        value as u8
    }
}

/// A "full" message payload, including both message and metadata
#[derive(Debug, Deserialize, Serialize)]
pub struct FullMessage(pub Message, pub MessageMetadata);

impl FullMessage {
    /// Create a new full message
    pub fn new(message: Message, metadata: MessageMetadata) -> Self {
        Self(message, metadata)
    }
    /// Get the message ID
    pub fn message_id(&self) -> &MessageId {
        &self.1.message_id
    }
    /// Get the message's metadata
    pub fn metadata(&self) -> &MessageMetadata {
        &self.1
    }
    /// Get the message
    pub fn message(&self) -> &Message {
        &self.0
    }
    /// Get the milestone index that references this
    pub fn ref_ms(&self) -> Option<u32> {
        self.1.referenced_by_milestone_index
    }
}

/// Enum used by importer to keep the sockets up to date with most recent progress.
#[derive(Deserialize, Serialize, Debug)]
pub enum ImporterSession {
    /// Create/update progress bar state
    ProgressBar {
        /// Total size of the logfile
        log_file_size: u64,
        /// LogFile start range
        from_ms: u32,
        /// LogFile end range
        to_ms: u32,
        /// milestone data bytes size
        ms_bytes_size: usize,
        /// Milestone index
        milestone_index: u32,
        /// Identify whether it skipped/resume the milestone_index or imported.
        skipped: bool,
    },
    /// Finish the progress bar with message
    Finish {
        /// LogFile start range
        from_ms: u32,
        /// LogFile end range
        to_ms: u32,
        /// Finish the progress bar using this msg.
        msg: String,
    },
    /// Return error
    PathError {
        /// Invalid dir or file path
        path: PathBuf,
        /// Useful debug message
        msg: String,
    },
    /// Close session
    Close,
}

#[cfg(feature = "sync")]
pub use sync::*;
#[cfg(feature = "sync")]
mod sync {
    use super::*;
    use chronicle_common::SyncRange;
    use chronicle_storage::access::SyncRecord;
    use scylla_rs::prelude::{
        Consistency,
        GetSelectRequest,
        Iter,
        Select,
        ValueWorker,
    };
    use std::ops::Range;

    /// Representation of the database sync data
    #[derive(Debug, Clone, Default, Serialize)]
    pub struct SyncData {
        /// The completed(synced and logged) milestones data
        pub(crate) completed: Vec<Range<u32>>,
        /// Synced milestones data but unlogged
        pub(crate) synced_but_unlogged: Vec<Range<u32>>,
        /// Gaps/missings milestones data
        pub(crate) gaps: Vec<Range<u32>>,
    }

    impl SyncData {
        /// Try to fetch the sync data from the sync table for the provided keyspace and sync range
        pub async fn try_fetch<S: 'static + Select<SyncRange, Iter<SyncRecord>>>(
            keyspace: &S,
            sync_range: &SyncRange,
            retries: usize,
        ) -> anyhow::Result<SyncData> {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let _ = keyspace
                .select(sync_range)
                .consistency(Consistency::One)
                .build()?
                .send_local(ValueWorker::boxed(
                    tx,
                    keyspace.clone(),
                    sync_range.clone(),
                    retries,
                    std::marker::PhantomData,
                ));
            let select_response = rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("Expected Rx inbox to receive the sync data response"))??;
            let mut sync_data = SyncData::default();
            if let Some(mut sync_rows) = select_response {
                // Get the first row, note: the first row is always with the largest milestone_index
                let SyncRecord {
                    milestone_index,
                    logged_by,
                    ..
                } = sync_rows.next().unwrap();
                // push missing row/gap (if any)
                sync_data.process_gaps(sync_range.to, *milestone_index);
                sync_data.process_rest(&logged_by, *milestone_index, &None);
                let mut pre_ms = milestone_index;
                let mut pre_lb = logged_by;
                // Generate and identify missing gaps in order to fill them
                while let Some(SyncRecord {
                    milestone_index,
                    logged_by,
                    ..
                }) = sync_rows.next()
                {
                    // check if there are any missings
                    sync_data.process_gaps(*pre_ms, *milestone_index);
                    sync_data.process_rest(&logged_by, *milestone_index, &pre_lb);
                    pre_ms = milestone_index;
                    pre_lb = logged_by;
                }
                // pre_ms is the most recent milestone we processed
                // it's also the lowest milestone index in the select response
                // so anything < pre_ms && anything >= (self.sync_range.from - 1)
                // (lower provided sync bound) are missing
                // push missing row/gap (if any)
                sync_data.process_gaps(*pre_ms, sync_range.from - 1);
                Ok(sync_data)
            } else {
                // Everything is missing as gaps
                sync_data.process_gaps(sync_range.to, sync_range.from - 1);
                Ok(sync_data)
            }
        }
        /// Takes the lowest gap from the sync_data
        pub fn take_lowest_gap(&mut self) -> Option<Range<u32>> {
            self.gaps.pop()
        }
        /// Takes the lowest unlogged range from the sync_data
        pub fn take_lowest_unlogged(&mut self) -> Option<Range<u32>> {
            self.synced_but_unlogged.pop()
        }
        /// Takes the lowest unlogged or gap from the sync_data
        pub fn take_lowest_gap_or_unlogged(&mut self) -> Option<Range<u32>> {
            let lowest_gap = self.gaps.last();
            let lowest_unlogged = self.synced_but_unlogged.last();
            match (lowest_gap, lowest_unlogged) {
                (Some(gap), Some(unlogged)) => {
                    if gap.start < unlogged.start {
                        self.gaps.pop()
                    } else {
                        self.synced_but_unlogged.pop()
                    }
                }
                (Some(_), None) => self.gaps.pop(),
                (None, Some(_)) => self.synced_but_unlogged.pop(),
                _ => None,
            }
        }
        /// Takes the lowest uncomplete(mixed range for unlogged and gap) from the sync_data
        pub fn take_lowest_uncomplete(&mut self) -> Option<Range<u32>> {
            if let Some(mut pre_range) = self.take_lowest_gap_or_unlogged() {
                loop {
                    if let Some(next_range) = self.get_lowest_gap_or_unlogged() {
                        if next_range.start.eq(&pre_range.end) {
                            pre_range.end = next_range.end;
                            let _ = self.take_lowest_gap_or_unlogged();
                        } else {
                            return Some(pre_range);
                        }
                    } else {
                        return Some(pre_range);
                    }
                }
            } else {
                None
            }
        }
        fn get_lowest_gap_or_unlogged(&self) -> Option<&Range<u32>> {
            let lowest_gap = self.gaps.last();
            let lowest_unlogged = self.synced_but_unlogged.last();
            match (lowest_gap, lowest_unlogged) {
                (Some(gap), Some(unlogged)) => {
                    if gap.start < unlogged.start {
                        self.gaps.last()
                    } else {
                        self.synced_but_unlogged.last()
                    }
                }
                (Some(_), None) => self.gaps.last(),
                (None, Some(_)) => self.synced_but_unlogged.last(),
                _ => None,
            }
        }
        fn process_rest(&mut self, logged_by: &Option<u8>, milestone_index: u32, pre_lb: &Option<u8>) {
            if logged_by.is_some() {
                // process logged
                Self::proceed(&mut self.completed, milestone_index, pre_lb.is_some());
            } else {
                // process_unlogged
                let unlogged = &mut self.synced_but_unlogged;
                Self::proceed(unlogged, milestone_index, pre_lb.is_none());
            }
        }
        fn process_gaps(&mut self, pre_ms: u32, milestone_index: u32) {
            let gap_start = milestone_index + 1;
            if gap_start != pre_ms {
                // create missing gap
                let gap = Range {
                    start: gap_start,
                    end: pre_ms,
                };
                self.gaps.push(gap);
            }
        }
        fn proceed(ranges: &mut Vec<Range<u32>>, milestone_index: u32, check: bool) {
            let end_ms = milestone_index + 1;
            if let Some(Range { start, .. }) = ranges.last_mut() {
                if check && *start == end_ms {
                    *start = milestone_index;
                } else {
                    let range = Range {
                        start: milestone_index,
                        end: end_ms,
                    };
                    ranges.push(range)
                }
            } else {
                let range = Range {
                    start: milestone_index,
                    end: end_ms,
                };
                ranges.push(range);
            };
        }
    }
}

#[cfg(feature = "analytic")]
pub use analytic::*;
#[cfg(feature = "analytic")]
mod analytic {
    use super::*;
    use chronicle_common::SyncRange;
    use scylla_rs::prelude::{
        Consistency,
        GetSelectRequest,
        Iter,
        Select,
        ValueWorker,
    };
    use std::ops::Range;

    /// Representation of vector of analytic data
    #[derive(Debug, Clone, Default, Serialize)]
    pub struct AnalyticsData {
        /// Vector of sequential ranges of analytics data
        #[serde(flatten)]
        pub analytics: Vec<AnalyticData>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    /// AnalyticData representation for a continuous range in scylla
    pub struct AnalyticData {
        #[serde(flatten)]
        range: Range<u32>,
        message_count: u128,
        transaction_count: u128,
        transferred_tokens: u128,
    }
    impl From<AnalyticRecord> for AnalyticData {
        fn from(record: AnalyticRecord) -> Self {
            // create analytic
            let milestone_index = **record.milestone_index();
            let message_count = **record.message_count() as u128;
            let transaction_count = **record.transaction_count() as u128;
            let transferred_tokens = **record.transferred_tokens() as u128;
            let range = Range {
                start: milestone_index,
                end: milestone_index + 1,
            };
            AnalyticData::new(range, message_count, transaction_count, transferred_tokens)
        }
    }
    impl AnalyticData {
        pub(crate) fn new(
            range: Range<u32>,
            message_count: u128,
            transaction_count: u128,
            transferred_tokens: u128,
        ) -> Self {
            Self {
                range,
                message_count,
                transaction_count,
                transferred_tokens,
            }
        }
        async fn process(mut self, analytics_data: &mut AnalyticsData, records: &mut Iter<AnalyticRecord>) {
            while let Some(record) = records.next() {
                if self.start() - 1 == **record.milestone_index() {
                    self.acc(record);
                } else {
                    // there is gap, therefore we finish self
                    analytics_data.add_analytic_data(self);
                    // create new analytic_data
                    self = AnalyticData::from(record);
                }
            }
            analytics_data.add_analytic_data(self);
        }
        fn start(&self) -> u32 {
            self.range.start
        }
        fn acc(&mut self, record: AnalyticRecord) {
            self.range.start -= 1;
            self.message_count += **record.message_count() as u128;
            self.transaction_count += **record.transaction_count() as u128;
            self.transferred_tokens += **record.transferred_tokens() as u128;
        }
    }

    impl AnalyticsData {
        /// Try to fetch the analytics data from the analytics table for the provided keyspace and sync range
        pub async fn try_fetch<S: 'static + Select<SyncRange, Iter<AnalyticRecord>>>(
            keyspace: &S,
            sync_range: &SyncRange,
            retries: usize,
            page_size: i32,
        ) -> anyhow::Result<AnalyticsData> {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            Self::query_analytics_table(keyspace, sync_range, retries, tx.clone(), page_size, None)?;
            let mut analytics_data = AnalyticsData::default();
            while let Some(mut records) = rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("Unable to fetch the analytics response"))??
            {
                if records.has_more_pages() {
                    // request next page
                    let paging_state = records.take_paging_state();
                    // this will request the next page, and value worker will pass it to us through rx
                    Self::query_analytics_table(keyspace, sync_range, retries, tx.clone(), page_size, paging_state)?;
                    // Gets the first record in the page result, which is used to trigger accumulation
                    analytics_data.try_trigger(&mut records).await;
                } else {
                    // no more pages to fetch, therefore we process whatever we received, ..
                    analytics_data.try_trigger(&mut records).await;
                    // break the while
                    break;
                }
            }
            Ok(analytics_data)
        }
        async fn try_trigger(&mut self, analytics_rows: &mut Iter<AnalyticRecord>) {
            if let Some(analytic_record) = analytics_rows.next() {
                self.process(analytic_record, analytics_rows).await;
            }
        }
        async fn process(&mut self, record: AnalyticRecord, records: &mut Iter<AnalyticRecord>) {
            // check if there is an active analytic_data with continuous range
            if let Some(analytic_data) = self.try_pop_recent_analytic_data() {
                analytic_data.process(self, records).await;
            } else {
                let analytic_data = AnalyticData::from(record);
                analytic_data.process(self, records).await;
            }
        }
        fn query_analytics_table<S: 'static + Select<SyncRange, Iter<AnalyticRecord>>>(
            keyspace: &S,
            sync_range: &SyncRange,
            retries: usize,
            tx: tokio::sync::mpsc::UnboundedSender<Result<Option<Iter<AnalyticRecord>>, scylla_rs::app::WorkerError>>,
            page_size: i32,
            paging_state: Option<Vec<u8>>,
        ) -> anyhow::Result<()> {
            let req = keyspace
                .select(sync_range)
                .consistency(Consistency::One)
                .page_size(page_size)
                .paging_state(&paging_state)
                .build()?;
            let worker = ValueWorker::new(
                tx,
                keyspace.clone(),
                sync_range.clone(),
                retries,
                std::marker::PhantomData,
            )
            .with_paging(page_size, paging_state);
            req.send_local(Box::new(worker));
            Ok(())
        }
        fn try_pop_recent_analytic_data(&mut self) -> Option<AnalyticData> {
            self.analytics.pop()
        }
        fn add_analytic_data(&mut self, analytic_data: AnalyticData) {
            self.analytics.push(analytic_data);
        }
    }
}
