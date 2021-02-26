// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::application::*;
use futures::stream::{
    Stream,
    StreamExt,
};
use paho_mqtt::{
    AsyncClient,
    Message,
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{
        Deref,
        DerefMut,
    },
    time::Duration,
};
pub(crate) use url::Url;
mod event_loop;
mod init;
mod terminating;

// Mqtt builder
builder!(MqttBuilder<T> {
    url: Url,
    topic: T,
    stream_capacity: usize
});

/// MqttHandle to be passed to the supervisor in order to shutdown
#[derive(Clone)]
pub struct MqttHandle {
    client: AsyncClient,
}
/// MqttInbox is used to recv events
pub struct MqttInbox {
    stream: Box<dyn Stream<Item = Option<Message>> + Send + std::marker::Unpin>,
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
// Mqtt state
pub struct Mqtt<T> {
    service: Service,
    url: Url,
    stream_capacity: usize,
    handle: Option<MqttHandle>,
    inbox: Option<MqttInbox>,
    topic: T,
}

impl<T> Mqtt<T> {
    pub(crate) fn clone_handle(&self) -> MqttHandle {
        self.handle.clone().unwrap()
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

/// Mqtt Metadata topic
pub(crate) struct Metadata;

impl Topic for Metadata {
    fn name() -> &'static str {
        "messages/{messageId}/metadata"
    }
    fn qos() -> i32 {
        0
    }
}

impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for MqttBuilder<Messages> {}
impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for MqttBuilder<Metadata> {}

/// implementation of builder
impl<T: Topic> Builder for MqttBuilder<T> {
    type State = Mqtt<T>;
    fn build(self) -> Self::State {
        let handle = None;
        let inbox = None;
        Self::State {
            service: Service::new(),
            url: self.url.unwrap(),
            stream_capacity: self.stream_capacity.unwrap_or(10000),
            handle,
            inbox,
            topic: self.topic.unwrap(),
        }
        .set_name()
    }
}

/// impl name of the Mqtt<T>
impl<T: Topic> Name for Mqtt<T> {
    fn set_name(mut self) -> Self {
        let name = format!("{}.{}", T::name(), self.url.as_str());
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<T: Topic, H: PermanodeBrokerScope> AknShutdown<Mqtt<T>> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Mqtt<T>, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Mqtt(_state.service.clone()));
        let _ = self.send(event);
    }
}
