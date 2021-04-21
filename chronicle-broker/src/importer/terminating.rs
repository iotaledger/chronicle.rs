// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> Terminating<BrokerHandle<H>> for Importer {
    async fn terminating(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        let msg;
        if status.is_ok() {
            msg = "done".into();
        } else {
            msg = "failed".into();
        }
        if let Some(log_file) = self.log_file.as_ref() {
            let importer_session = ImporterSession::Finish {
                from_ms: log_file.from_ms_index(),
                to_ms: log_file.to_ms_index(),
                msg,
            };
            let event = BrokerEvent::Importer(importer_session);
            supervisor.as_mut().expect("Expected BrokerHandle").send(event).ok();
        } else {
            let event = BrokerEvent::Importer(ImporterSession::PathError {
                path: self.file_path.clone(),
                msg: "Invalid LogFile path".into(),
            });
            supervisor.as_mut().expect("Expected BrokerHandle").send(event).ok();
        }
        self.service.update_status(ServiceStatus::Stopping);
        let event = BrokerEvent::Children(BrokerChild::Importer(self.service.clone(), status, self.parallelism));
        let _ = supervisor.as_mut().expect("Expected BrokerHandle").send(event);
        status
    }
}
