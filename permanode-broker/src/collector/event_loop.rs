// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::str::FromStr;
#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Collector {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        while let Some(event) = self.inbox.recv().await {
            match event {
                #[allow(unused_mut)]
                CollectorEvent::Message(message_id, mut message) => {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        {
                            // store message
                            self.default_keyspace
                                .insert(&message_id, &message)
                                .consistency(Consistency::One)
                                .build();
                        }
                    } else {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg.put(message_id, message);
                    }
                }
                CollectorEvent::MessageReferenced(msg_ref) => {
                    let ref_ms = msg_ref.referenced_by_milestone_index.as_ref().unwrap();
                    let partition_id = (ref_ms % (self.collectors_count as u32)) as u8;
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

impl Collector {
    fn insert_message(&mut self, message_id: MessageId, message: Message) {}
}
