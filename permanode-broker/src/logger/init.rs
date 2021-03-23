// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> Init<BrokerHandle<H>> for Logger {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        // create directory first
        tokio::fs::create_dir(self.dir_path.clone().into_boxed_path())
            .await
            .map_err(|e| {
                error!("Unable to create log directory, error: {}", e);
                Need::Abort
            })?;
        Ok(())
    }
}
