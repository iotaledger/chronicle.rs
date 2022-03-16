// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `parents` table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct ParentRecord {
    pub parent_id: MessageId,
    pub message_id: MessageId,
    pub milestone_index: Option<MilestoneIndex>,
    pub ms_timestamp: Option<NaiveDateTime>,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl ParentRecord {
    /// Creates a new parent row
    pub fn new(
        parent_id: MessageId,
        milestone_index: Option<MilestoneIndex>,
        ms_timestamp: Option<NaiveDateTime>,
        message_id: MessageId,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            parent_id,
            milestone_index,
            ms_timestamp,
            message_id,
            inclusion_state,
        }
    }
}
