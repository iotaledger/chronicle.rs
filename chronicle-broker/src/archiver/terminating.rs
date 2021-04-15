// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> Terminating<BrokerHandle<H>> for Archiver {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = BrokerEvent::Children(BrokerChild::Archiver(self.service.clone(), _status));
        let _ = supervisor.as_mut().unwrap().send(event);
        // finialize in progress logs
        for log in self.logs.iter_mut() {
            if let Err(e) = log.finish(&self.dir_path).await {
                info!("Unable to finish in progress log file: {}, error: {}", log.filename, e);
            } else {
                info!("Finished in progress log file: {}", log.filename);
            };
        }
        _status
    }
}
