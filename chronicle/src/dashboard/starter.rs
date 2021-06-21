// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use anyhow::anyhow;

#[async_trait]
impl<H: DashboardScope> Starter<H> for DashboardBuilder<H> {
    type Ok = DashboardSender<H>;

    type Error = anyhow::Error;

    type Input = Dashboard<H>;

    async fn starter(self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let dashboard = input.unwrap_or_else(|| self.build());

        let supervisor = dashboard
            .sender
            .clone()
            .ok_or_else(|| anyhow!("No supervisor for dashboard!"))?;

        tokio::spawn(dashboard.start(Some(handle)));

        Ok(supervisor)
    }
}
