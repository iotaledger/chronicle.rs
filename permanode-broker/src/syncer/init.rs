// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> Init<BrokerHandle<H>> for Syncer {
    async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        info!("Syncer is initializing");
        self.service.update_status(ServiceStatus::Initializing);
        let event = BrokerEvent::Children(BrokerChild::Syncer(self.service.clone(), Ok(())));
        let _ = _supervisor.as_mut().expect("Syncer expected BrokerHandle").send(event);
        status
    }
}
