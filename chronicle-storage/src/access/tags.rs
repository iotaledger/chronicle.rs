// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `tag` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct TagRecord {
    pub tag: String,
    pub partition_data: PartitionData,
    pub message_id: MessageId,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl TagRecord {
    /// Creates a new tag record
    pub fn new(
        tag: String,
        partition_data: PartitionData,
        message_id: MessageId,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            tag,
            partition_data,
            message_id,
            inclusion_state,
        }
    }
}
