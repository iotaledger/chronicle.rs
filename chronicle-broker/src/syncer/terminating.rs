// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> Terminating<BrokerHandle<H>> for Syncer {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        info!("Syncer is terminating");
        self.service.update_status(ServiceStatus::Stopping);
        let event = BrokerEvent::Children(BrokerChild::Syncer(self.service.clone(), _status));
        let _ = _supervisor.as_mut().expect("Syncer expected BrokerHandle").send(event);
        _status
    }
}
