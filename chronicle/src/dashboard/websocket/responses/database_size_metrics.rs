// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DatabaseSizeMetricsResponse {
    pub total: u64,
    pub ts: u64,
}
