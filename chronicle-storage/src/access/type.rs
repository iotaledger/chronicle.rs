// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use pin_project_lite::pin_project;
use std::collections::VecDeque;
use chronicle_common::Wrapper;
use futures::{
    stream::Stream,
    task::{
        Context,
        Poll,
    },
};
use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    io::Cursor,
    ops::{
        Deref,
        DerefMut,
    },
    path::PathBuf,
    str::FromStr,
};


/// A "full" message payload, including both message and metadata
#[derive(Clone, Debug, Deserialize, Serialize)]
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

impl Row for FullMessage {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(FullMessage(
            rows.column_value::<Bee<Message>>()?.into_inner(),
            rows.column_value::<MessageMetadata>()?,
        ))
    }
}

/// A type alias for partition ids
pub type PartitionId = u16;

/// An index in plain-text, unhashed
#[derive(Clone, Debug)]
pub struct Indexation(pub String);

impl ColumnEncoder for Indexation {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.0.encode(buffer)
    }
}

impl TokenEncoder for Indexation {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}


#[derive(Deserialize, Serialize)]
/// Defines a message to/from the Broker or its children
pub enum BrokerSocketMsg<T> {
    /// A message to/from the Broker
    ChronicleBroker(T),
}

/// Milestone data
#[derive(Debug, Deserialize, Serialize)]
pub struct MilestoneData {
    pub(crate) milestone_index: u32,
    pub(crate) milestone: Option<Box<MilestonePayload>>,
    pub(crate) ms_message_id: Option<MessageId>,
    pub(crate) messages: BTreeMap<MessageId, FullMessage>,
    #[serde(skip)]
    pub(crate) selected_messages: std::collections::HashMap<MessageId, Selected>,
    #[serde(skip)]
    pub(crate) pending: HashSet<MessageId>,
    #[serde(skip)]
    pub(crate) created_by: CreatedBy,
}

impl MilestoneData {
    /// Create new milestone data
    pub fn new(milestone_index: u32, created_by: CreatedBy) -> Self {
        Self {
            milestone_index,
            milestone: None,
            ms_message_id: None,
            messages: BTreeMap::new(),
            selected_messages: HashMap::new(),
            pending: HashSet::new(),
            created_by,
        }
    }
    /// Get the milestone index from this milestone data
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
    /// Try to get milestone timestamp
    pub fn get_milestone_timestamp(&self) -> Option<u64> {
        self.milestone.as_ref().and_then(|m| Some(m.essence().timestamp()))
    }
    /// Set the milestone in the milestone data
    pub fn set_milestone(&mut self, milestone_message_id: MessageId, boxed_milestone_payload: Box<MilestonePayload>) {
        self.milestone.replace(boxed_milestone_payload);
        self.ms_message_id.replace(milestone_message_id);
    }
    /// Check if the milestone exists
    pub fn milestone_exist(&self) -> bool {
        self.milestone.is_some()
    }
    /// Add the full message
    pub fn add_full_message(&mut self, full_message: FullMessage, mut selected: Option<Selected>) {
        if let Some(selected) = selected.take() {
            self.selected_messages.insert(*full_message.message_id(), selected);
        }
        self.messages.insert(*full_message.message_id(), full_message);
    }
    pub fn add_pending(&mut self, parent_id: MessageId) {
        self.pending.insert(parent_id);
    }
    pub fn remove_from_pending(&mut self, message_id: &MessageId) {
        self.pending.remove(message_id);
    }
    /// Get the milestone's messages
    pub fn messages(&self) -> &BTreeMap<MessageId, FullMessage> {
        &self.messages
    }
    /// Get the pending messages
    pub fn pending(&self) -> &HashSet<MessageId> {
        &self.pending
    }
    /// Get the source this was created by
    pub fn created_by(&self) -> &CreatedBy {
        &self.created_by
    }
    /// Set created by
    pub fn set_created_by(&mut self, created_by: CreatedBy) {
        self.created_by = created_by
    }
    /// Check if the milestone data is completed
    pub fn check_if_completed(&self) -> bool {
        // Check if there are no pending at all to set complete to true
        let no_pending_left = self.pending().is_empty();
        let milestone_exist = self.milestone_exist();
        let has_messages = !self.messages.is_empty();
        if no_pending_left && milestone_exist && has_messages {
            // milestone data is complete now
            return true;
        }
        false
    }
}

impl std::iter::IntoIterator for MilestoneData {
    type Item = (MessageId, FullMessage);
    type IntoIter = std::collections::btree_map::IntoIter<MessageId, FullMessage>;
    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

impl std::convert::TryFrom<std::sync::Arc<MilestoneData>> for MilestoneDataSearch {
    type Error = anyhow::Error;

    fn try_from(data: std::sync::Arc<MilestoneData>) -> Result<Self, Self::Error> {
        if !data.check_if_completed() {
            anyhow::bail!("cannot make milestone data search struct for uncompleted milestone data")
        }
        if let Some(ms_message_id) = data.ms_message_id.as_ref() {
            let mut should_be_visited = VecDeque::new();
            // we start from the root
            should_be_visited.push_back(Proof {
                milestone_index: data.milestone_index(),
                path: vec![*ms_message_id],
            });
            Ok(Self {
                data,
                should_be_visited,
                visited: Default::default(),
                budget: 128,
                counter: 0,
            })
        } else {
            todo!("iterate all the messages to find the milestone msg id")
        }
    }
}

pin_project! {
    #[must_use = "futures/streams do nothing unless you poll them"]
    pub struct MilestoneDataSearch {
        #[pin]
        data: std::sync::Arc<MilestoneData>,
        #[pin]
        should_be_visited: VecDeque<Proof>,
        #[pin]
        visited: HashSet<MessageId>,
        budget: usize,
        counter: usize,
    }
}

impl futures::stream::Stream for MilestoneDataSearch {
    type Item = (Option<Proof>, MessageId, FullMessage);

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut project = self.as_mut().project();
        if project.counter == project.budget {
            *project.counter = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        while let Some(current_proof) = project.should_be_visited.pop_front() {
            *project.counter += 1;
            let message_id = *current_proof.path.last().expect("The path should never be empty");
            project.visited.insert(message_id);
            // iterate over its parents
            if let Some(full_message) = project.data.messages.get(&message_id) {
                let parents_iter = full_message.message().parents().iter();
                for parent_id in parents_iter {
                    if !project.visited.contains(parent_id) {
                        let mut vertex = current_proof.clone();
                        vertex.path.push(parent_id.clone());
                        project.should_be_visited.push_back(vertex);
                    }
                }
                // check if this message is selected
                if project.data.selected_messages.contains_key(&message_id) {
                    return Poll::Ready(Some((Some(current_proof), message_id, full_message.clone())));
                } else {
                    return Poll::Ready(Some((None, message_id, full_message.clone())));
                }
            } else {
                // reached the end of the branch, proceed to the next should_be_visited
                continue;
            }
        }
        Poll::Ready(None)
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
    /// Created by solidify/sync request from syncer
    Syncer = 2,
    /// Created by the exporter
    Exporter = 3,
}

impl Default for CreatedBy {
    fn default() -> Self {
        Self::Incoming
    }
}

impl From<CreatedBy> for u8 {
    fn from(value: CreatedBy) -> u8 {
        value as u8
    }
}

#[cfg(feature = "sync")]
pub use sync::*;
#[cfg(feature = "sync")]
mod sync {
    use super::*;
    use chronicle_common::SyncRange;
    use scylla_rs::prelude::{
        Consistency,
        GetStaticSelectRequest,
        Iter,
        Select,
    };
    use std::ops::Range;

    /// Representation of the database sync data
    #[derive(Debug, Clone, Default, Serialize)]
    pub struct SyncData {
        /// The completed(synced and logged) milestones data
        pub completed: Vec<Range<u32>>,
        /// Synced milestones data but unlogged
        pub synced_but_unlogged: Vec<Range<u32>>,
        /// Gaps/missings milestones data
        pub gaps: Vec<Range<u32>>,
    }

    impl SyncData {
        /// Try to fetch the sync data from the sync table for the provided keyspace and sync range
        pub async fn try_fetch<S: 'static + Select<String, SyncRange, Iter<SyncRecord>>>(
            keyspace: &S,
            sync_range: &SyncRange,
            retries: usize,
        ) -> anyhow::Result<SyncData> {
            let res = keyspace
                .select(&"permanode".to_string(), &sync_range.clone().into())
                .consistency(Consistency::One)
                .build()?
                .worker()
                .with_retries(retries)
                .get_local()
                .await?;
            let mut sync_data = SyncData::default();
            if let Some(mut sync_rows) = res {
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
