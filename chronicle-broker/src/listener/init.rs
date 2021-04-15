// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> Init<BrokerHandle<H>> for Listener {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        let event = BrokerEvent::Children(BrokerChild::Listener(self.service.clone()));
        let _ = supervisor.as_mut().unwrap().send(event);
        status
    }
}
