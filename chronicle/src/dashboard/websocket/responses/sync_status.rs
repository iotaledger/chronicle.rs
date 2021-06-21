// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SyncStatusResponse {
    pub(crate) lmi: u32,
    pub(crate) cmi: u32,
}
