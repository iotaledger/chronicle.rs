// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A milestone-based analytics record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MsAnalyticsRecord {
    #[serde(skip_serializing)]
    pub ms_range_id: u32,
    pub milestone_index: MilestoneIndex,
    pub message_count: u32,
    pub transaction_count: u32,
    pub transferred_tokens: u64,
}
