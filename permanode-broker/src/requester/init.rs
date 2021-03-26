// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Init<CollectorHandle> for Requester {
    async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<CollectorHandle>) -> Result<(), Need> {
        status
    }
}
