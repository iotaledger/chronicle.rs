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
        struct TestWorker<K, V> {
            key: K,
            value: V,
        }
        // testing block, to be removed
        impl Worker for TestWorker<MessageId, Message> {
            fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
                error!("{:?}, is_none {}", error, reporter.is_none());
            }
            fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
                info!("{:?}", giveload);
            }
        }
        let mainnet = Mainnet::new();
        while let Some(event) = self.inbox.recv().await {
            match event {
                CollectorEvent::Message(message_id, message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        mainnet.insert(&message_id, &message).send_local(Box::new(TestWorker {
                            key: message_id,
                            value: message,
                        }));
                    } else {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg.put(message_id, message);
                    }
                }
                CollectorEvent::MessageReferenced(msg_ref) => {
                    let partition_id = (msg_ref.referenced_by_milestone_index % (self.collectors_count as u64)) as u8;
                    let message_id = msg_ref.message_id;
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg_ref.get(&message_id) {
                        // TODO store it as metadata
                        // check if msg already exist in the cache, if so we pushed to solidifier

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
