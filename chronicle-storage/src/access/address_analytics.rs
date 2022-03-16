// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A milestone-based analytics record
#[derive(Clone, Debug)]
pub struct AddressAnalyticsRecord {
    pub address: Address,
    pub milestone_index: MilestoneIndex,
    pub sent_tokens: u64,
    pub recv_tokens: u64,
}
