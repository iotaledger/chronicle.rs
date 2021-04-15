// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> EventLoop<BrokerHandle<H>> for Mqtt<Messages> {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        status?;
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Mqtt(self.service.clone(), None, status));
        let _ = supervisor.as_mut().unwrap().send(event);
        let inbox = self.inbox.as_mut().unwrap();
        while let Some(msg_opt) = inbox.stream.next().await {
            if let Some(msg) = msg_opt {
                if let Ok(msg) = Message::unpack(&mut msg.payload()) {
                    let (message_id, _) = msg.id();
                    // partitioning based on first byte of the message_id
                    let collector_partition_id = self.partitioner.partition_id(&message_id);
                    if let Some(collector_handle) = self.collectors_handles.get(&collector_partition_id) {
                        let _ = collector_handle.send(CollectorEvent::Message(message_id, msg));
                    }
                };
            } else {
                warn!("Mqtt: {}, lost connection", self.get_name());
                return Err(Need::Restart);
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> EventLoop<BrokerHandle<H>> for Mqtt<MessagesReferenced> {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        status?;
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Mqtt(self.service.clone(), None, status));
        let _ = supervisor.as_mut().unwrap().send(event);
        let inbox = self.inbox.as_mut().unwrap();
        while let Some(msg_ref_opt) = inbox.stream.next().await {
            if let Some(msg_ref) = msg_ref_opt {
                if let Ok(msg_ref) = serde_json::from_str::<MessageMetadata>(&msg_ref.payload_str()) {
                    // partitioning based on first byte of the message_id
                    let collector_partition_id = self.partitioner.partition_id(&msg_ref.message_id);
                    if let Some(collector_handle) = self.collectors_handles.get(&collector_partition_id) {
                        let _ = collector_handle.send(CollectorEvent::MessageReferenced(msg_ref));
                    }
                };
            } else {
                warn!("Mqtt: {}, lost connection", self.get_name());
                return Err(Need::Restart);
            }
        }
        Ok(())
    }
}
