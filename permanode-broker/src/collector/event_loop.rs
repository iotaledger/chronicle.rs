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
        while let Some(event) = self.inbox.recv().await {
            match event {
                #[allow(unused_mut)]
                CollectorEvent::Message(message_id, mut message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        {
                            // store message
                            self.insert_message(message_id, message);
                        }
                    } else {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg.put(message_id, message);
                    }
                }
                CollectorEvent::MessageReferenced(msg_ref) => {
                    let ref_ms = msg_ref.referenced_by_milestone_index.as_ref().unwrap();
                    let _partition_id = (ref_ms % (self.collectors_count as u32)) as u8;

                    let message_id = msg_ref.message_id;
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg_ref.get(&message_id) {
                        // store it as metadata
                        self.insert_message_metadata(msg_ref);
                        // check if msg already exist in the cache, if so we push it to solidifier
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
    fn insert_message(&mut self, message_id: MessageId, message: Message) {
        // message without payload
        self.default_keyspace
            .insert(&message_id, &message)
            .consistency(Consistency::One)
            .build();
        // todo insert parents/children

        // todo send with the insert right worker

        // insert payload
        if let Some(payload) = &message.payload() {
            match payload {
                Payload::Indexation(index) => {}
                Payload::Transaction(transaction) => {}
                // remaining payload types
                _ => {}
            }
        } else {
        }
    }
    fn insert_message_metadata(&mut self, msg_ref: MessageMetadataObj) {}
}
