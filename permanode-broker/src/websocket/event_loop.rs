// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: BrokerScope> EventLoop<BrokerHandle<H>> for Websocket {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        // exit the websocket event_loop if status is Err
        status?;
        // socket running
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Websocket(self.service.clone(), None));
        supervisor.as_mut().unwrap().send(event).map_err(|_| Need::Abort)?;
        while let Some(Ok(msg)) = self.ws_rx.next().await {
            match msg {
                Message::Text(msg_txt) => {
                    let apps_events: H::AppsEvents = serde_json::from_str(&msg_txt).map_err(|_| Need::Abort)?;
                    let event = BrokerEvent::Passthrough(apps_events);
                    supervisor.as_mut().unwrap().send(event).map_err(|_| Need::Abort)?;
                }
                Message::Close(_) => {
                    self.service.update_status(ServiceStatus::Stopping);
                    let event = BrokerEvent::Children(BrokerChild::Websocket(self.service.clone(), None));
                    supervisor.as_mut().unwrap().send(event).map_err(|_| Need::Abort)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}
