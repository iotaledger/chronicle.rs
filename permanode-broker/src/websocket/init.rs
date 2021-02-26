// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> Init<BrokerHandle<H>> for Websocket {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        // todo authenticator using static secert key and noise protocol
        if true {
            self.service.update_status(ServiceStatus::Initializing);
            let event = BrokerEvent::Children(BrokerChild::Websocket(self.service.clone(), self.opt_ws_tx.take()));
            let _ = supervisor.as_mut().unwrap().send(event);
            status
        } else {
            // drop supervisor handle if failed to authenticate,as it's unnecessary to aknowledge_shutdown
            supervisor.take().unwrap();
            return Err(Need::Abort);
        }
    }
}
