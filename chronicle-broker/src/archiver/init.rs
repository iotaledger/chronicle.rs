// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> Init<BrokerHandle<H>> for Archiver {
    async fn init(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        // create directory first
        if let Err(e) = tokio::fs::create_dir(self.dir_path.clone().into_boxed_path()).await {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                // do nothing
            } else {
                alert!("Unable to create log directory, error: {}", e);
                return Err(Need::Abort);
            }
        };
        self.service.update_status(ServiceStatus::Initializing);
        let event = BrokerEvent::Children(BrokerChild::Archiver(self.service.clone(), Ok(())));
        let _ = _supervisor
            .as_mut()
            .expect("Archiver expected BrokerHandle")
            .send(event);
        info!("Logger got initialized");
        Ok(())
    }
}
