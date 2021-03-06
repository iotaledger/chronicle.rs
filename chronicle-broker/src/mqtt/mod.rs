// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    collector::{
        CollectorEvent,
        CollectorHandle,
        MessageIdPartitioner,
    },
    *,
};
use futures::stream::StreamExt;
use std::{
    collections::HashMap,
    time::Duration,
};

mod event_loop;
mod init;
mod terminating;

// Mqtt builder
builder!(MqttBuilder<T> {
    url: Url,
    topic: T,
    collectors_handles: HashMap<u8, CollectorHandle>,
    stream_capacity: usize
});

/// MqttHandle to be passed to the supervisor in order to shutdown
#[derive(Clone)]
pub struct MqttHandle {
    client: std::sync::Arc<AsyncClient>,
}
/// MqttInbox is used to recv events from topic
pub struct MqttInbox {
    stream: futures::channel::mpsc::Receiver<Option<paho_mqtt::Message>>,
}

impl Shutdown for MqttHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.client.disconnect(None);
        None
    }
}
/// Mqtt state
pub struct Mqtt<T> {
    service: Service,
    url: Url,
    stream_capacity: usize,
    collectors_handles: HashMap<u8, CollectorHandle>,
    partitioner: MessageIdPartitioner,
    handle: Option<MqttHandle>,
    inbox: Option<MqttInbox>,
    _topic: T,
}

impl<T> Mqtt<T> {
    pub(crate) fn clone_service(&self) -> Service {
        self.service.clone()
    }
}

/// MQTT topics
pub enum Topics {
    /// Messages topic
    Messages,
    /// Messages Referenced topic
    MessagesReferenced,
}

impl TryFrom<&str> for Topics {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, String> {
        match value {
            "messages" => Ok(Topics::Messages),
            "messages/referenced" => Ok(Topics::MessagesReferenced),
            _ => Err(format!("Unsupported topic: {}", value).into()),
        }
    }
}

/// Trait to be implemented on the mqtt topics
pub trait Topic: Send + 'static {
    /// MQTT Topic name
    fn name() -> &'static str;
    /// MQTT Quality of service
    fn qos() -> i32;
}

/// Mqtt Messages topic
pub(crate) struct Messages;

impl Topic for Messages {
    fn name() -> &'static str {
        "messages"
    }
    fn qos() -> i32 {
        0
    }
}

/// Mqtt MessagesReferenced topic
pub(crate) struct MessagesReferenced;

impl Topic for MessagesReferenced {
    fn name() -> &'static str {
        "messages/referenced"
    }
    fn qos() -> i32 {
        0
    }
}

/// Mqtt "milestones/latest" topic
pub(crate) struct LatestMilestone;

impl Topic for LatestMilestone {
    fn name() -> &'static str {
        "milestones/latest"
    }
    fn qos() -> i32 {
        0
    }
}

impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for MqttBuilder<Messages> {}
impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for MqttBuilder<MessagesReferenced> {}

/// implementation of builder
impl<T: Topic> Builder for MqttBuilder<T> {
    type State = Mqtt<T>;
    fn build(self) -> Self::State {
        let collectors_handles = self.collectors_handles.expect("Expected collectors handles");
        let collector_count = collectors_handles.len() as u8;
        Self::State {
            service: Service::new(),
            url: self.url.unwrap(),
            collectors_handles,
            partitioner: MessageIdPartitioner::new(collector_count),
            stream_capacity: self.stream_capacity.unwrap_or(10000),
            handle: None,
            inbox: None,
            _topic: self.topic.unwrap(),
        }
        .set_name()
    }
}

/// impl name of the Mqtt<T>
impl<T: Topic> Name for Mqtt<T> {
    fn set_name(mut self) -> Self {
        let name = format!("{}@{}", T::name(), self.url.as_str());
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<T: Topic, H: ChronicleBrokerScope> AknShutdown<Mqtt<T>> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Mqtt<T>, status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Mqtt(_state.service.clone(), None, status));
        let _ = self.send(event);
    }
}
