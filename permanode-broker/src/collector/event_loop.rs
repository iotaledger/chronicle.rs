// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

use bee_message::Message;
use bee_common::packable::Packable;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Mqtt<Messages> {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        status?;
        let inbox = self.inbox.as_mut().unwrap();
        while let Some(msg_opt) = inbox.stream.next().await {
            if let Some(msg) = msg_opt {
                if let Ok(msg) = Message::unpack(&mut msg.payload()){
                    trace!("{:?}", msg);
                    // publish msg to collector
                };
            } else {
                error!("Mqtt: {}, Lost connection", self.get_name());
                return Err(Need::Restart);
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Mqtt<Metadata> {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        status?;
        let inbox = self.inbox.as_mut().unwrap();
        while let Some(msg_opt) = inbox.stream.next().await {
            if let Some(msg) = msg_opt {
                // TODO handle Metadata topic
                println!("{}", msg);
            } else {
                // None, we were disconnected, so we ask supervisor for reconnect
                return Err(Need::Restart);
            }
        }
        Ok(())
    }
}
