// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chrono::NaiveDate;

/// A milestone-based analytics record
#[derive(Clone, Debug)]
pub struct DateCacheRecord {
    pub date: NaiveDate,
    pub start_ms: MilestoneIndex,
    pub end_ms: MilestoneIndex,
}
