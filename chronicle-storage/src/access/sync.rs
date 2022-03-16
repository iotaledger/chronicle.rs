// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::SyncRange;
use futures::TryStreamExt;
use std::ops::Range;

/// A 'sync' table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SyncRecord {
    pub milestone_index: MilestoneIndex,
    pub synced_by: Option<SyncedBy>,
    pub logged_by: Option<LoggedBy>,
}

impl SyncRecord {
    /// Creates a new sync row
    pub fn new(milestone_index: MilestoneIndex, synced_by: Option<SyncedBy>, logged_by: Option<LoggedBy>) -> Self {
        Self {
            milestone_index,
            synced_by,
            logged_by,
        }
    }
}

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
    pub async fn try_fetch(collection: &Collection<SyncRecord>, sync_range: SyncRange) -> anyhow::Result<SyncData> {
        let mut res = collection.find(None, None).await?;
        let mut sync_data = SyncData::default();
        if let Some(SyncRecord {
            milestone_index,
            logged_by,
            ..
        }) = res.try_next().await?
        {
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
            }) = res.try_next().await?
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
        } else {
            // Everything is missing as gaps
            sync_data.process_gaps(sync_range.to, sync_range.from - 1);
        }

        Ok(sync_data)
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
