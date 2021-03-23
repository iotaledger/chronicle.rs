// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> Terminating<BrokerHandle<H>> for Logger {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = BrokerEvent::Children(BrokerChild::Logger(self.service.clone(), _status));
        let _ = _supervisor.as_mut().unwrap().send(event);
        // finialize in progress logs
        for log in self.logs.iter_mut() {
            if let Err(e) = log.finish().await {
                info!("Unable to finish in progress log file: {}, error: {}", log.filename, e);
            } else {
                info!("Finished in progress log file: {}", log.filename);
            };
        }
        _status
    }
}