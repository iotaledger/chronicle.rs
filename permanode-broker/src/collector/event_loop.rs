// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Collector {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        #[derive(Debug)]
        struct TestWorker<K, V>{
            key: K,
            value: V,
        }
        impl Worker for TestWorker<MessageId, Message> {
            fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
                trace!("{:?}", error);
            }
            fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
                trace!("{:?}", giveload);
            }
        }
        while let Some(event) = self.inbox.recv().await {
            match event {
                CollectorEvent::Message(message_id, message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        Mainnet.insert(&message_id, &message).send_local(Box::new(TestWorker {key: message_id, value: message}));
                    } else {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg.put(message_id, message);
                    }
                }
                CollectorEvent::MessageReferenced(msg_ref) => {
                    let message_id = msg_ref.message_id;
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg_ref.get(&message_id) {
                        // TODO store it as metadata
                    } else {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg_ref.put(message_id, msg_ref);
                    }
                }
            }
        }
        Ok(())
    }
}
