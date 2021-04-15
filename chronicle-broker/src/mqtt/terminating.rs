// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<T: Topic, H: PermanodeBrokerScope> Terminating<BrokerHandle<H>> for Mqtt<T> {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = BrokerEvent::Children(BrokerChild::Mqtt(self.service.clone(), None, _status));
        let _ = _supervisor.as_mut().unwrap().send(event);
        _status
    }
}
