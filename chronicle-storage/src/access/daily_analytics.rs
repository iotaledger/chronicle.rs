// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chrono::NaiveDate;

/// A milestone-based analytics record
#[derive(Clone, Debug)]
pub struct DailyAnalyticsRecord {
    pub year: u32,
    pub date: NaiveDate,
    pub total_addresses: u32,
    pub send_addresses: u32,
    pub recv_addresses: u32,
}
