// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: BrokerScope> EventLoop<BrokerHandle<H>> for Mqtt<Messages> {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        let inbox = self.inbox.as_mut().unwrap();
        while let Some(msg_opt) = inbox.stream.next().await {
            if let Some(msg) = msg_opt {
                // TODO handle Messages topic
                println!("{}", msg);
            } else {
                return Err(Need::Restart)
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: BrokerScope> EventLoop<BrokerHandle<H>> for Mqtt<Metadata> {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        let inbox = self.inbox.as_mut().unwrap();
        while let Some(msg_opt) = inbox.stream.next().await {
            if let Some(msg) = msg_opt {
                // TODO handle Metadata topic
                println!("{}", msg);
            } else {
                // None, we were disconnected, so we ask supervisor for reconnect
                return Err(Need::Restart)
            }
        }
        Ok(())
    }
}
