// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    collector::{
        CollectorEvent,
        CollectorHandles,
        MessageIdPartitioner,
    },
    *,
};
use backstage::core::MqttChannel;
use futures::stream::StreamExt;
use std::time::Duration;

#[async_trait]
impl<T: Topic> ChannelBuilder<MqttChannel> for Mqtt<T> {
    async fn build_channel(&mut self) -> ActorResult<MqttChannel> {
        // create async client
        let random_id: u64 = rand::random();
        let create_opts = CreateOptionsBuilder::new()
            .server_uri(&self.url.as_str()[..])
            .client_id(&format!("{}|{}", T::name(), random_id))
            .persistence(None)
            .finalize();
        let mut client = AsyncClient::new(create_opts).map_err(|e| {
            error!("Unable to create AsyncClient: {}, error: {}", &self.url.as_str(), e);
            ActorError::exit(e)
        })?;
        info!("Created AsyncClient: {}, topic: {}", &self.url.to_string(), T::name());
        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(60))
            .mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .connect_timeout(Duration::from_secs(5))
            .finalize();
        let stream = client.get_stream(self.stream_capacity);
        // connect client with the remote broker
        client.connect(conn_opts).await.map_err(|e| {
            error!(
                "Unable to connect AsyncClient: {}, topic: {}, error: {}",
                &self.url.as_str(),
                T::name(),
                e
            );
            ActorError::restart(e, None)
        })?;
        info!("Connected AsyncClient: {}, topic: {}", &self.url.as_str(), T::name());
        // subscribe to T::name() topic with T::qos()
        client.subscribe(T::name(), T::qos()).await.map_err(|e| {
            error!(
                "Unable to subscribe AsyncClient: {}, topic: {}, error: {}",
                &self.url.as_str(),
                T::name(),
                e
            );
            ActorError::restart(e, None)
        })?;
        info!("Subscribed AsyncClient: {}, topic: {}", &self.url.as_str(), T::name());
        let mqtt_channel = MqttChannel::new(client, self.stream_capacity, stream);
        Ok(mqtt_channel)
    }
}

impl<T: Topic> Mqtt<T> {
    async fn initialize<S>(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<CollectorHandles>
    where
        S: SupHandle<Self>,
        Self: Actor<S>,
    {
        info!("{:?} is initializing", &rt.service().directory());
        let parent_id = rt.parent_id().ok_or_else(|| {
            log::error!("Invalid mqtt without parent");
            ActorError::exit_msg("invalid mqtt without parent")
        })?;
        let collector_handles = rt.depends_on(parent_id).await?;
        info!("{:?} got initialized", &rt.service().directory());
        Ok(collector_handles)
    }
    async fn reconnecting<S>(rt: &mut Rt<Self, S>) -> ActorResult<()>
    where
        S: SupHandle<Self>,
        Self: Actor<S, Channel = MqttChannel>,
    {
        warn!("Mqtt: {} topic, lost connection", T::name());
        rt.update_status(ServiceStatus::Outage).await;
        loop {
            if let Err(e) = rt.inbox_mut().reconnect_after(Duration::from_secs(5)).await? {
                error!("unable to {}", e);
            } else {
                // resubscribe
                if let Err(error) = rt.inbox_mut().subscribe(T::name(), T::qos()).await {
                    error!("unable to subscribe, error: {}", error);
                } else {
                    // update the status to running
                    rt.update_status(ServiceStatus::Running).await;
                    return Ok(());
                };
            }
        }
    }
}

#[async_trait]
impl<S: SupHandle<Self>> Actor<S> for Mqtt<Message> {
    type Data = CollectorHandles;
    type Channel = MqttChannel;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        self.initialize::<S>(rt).await
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, collectors_handles: Self::Data) -> ActorResult<()> {
        log::info!("{:?} is running", &rt.service().directory());
        while let Some(msg_opt) = rt.inbox_mut().stream().next().await {
            if let Some(msg) = msg_opt {
                if let Ok(msg) = Message::unpack(&mut msg.payload()) {
                    let (message_id, _) = msg.id();
                    // partitioning based on first byte of the message_id
                    let collector_partition_id = self.partitioner.partition_id(&message_id);
                    if let Some(collector_handle) = collectors_handles.get(&collector_partition_id) {
                        collector_handle.send(CollectorEvent::Message(message_id, msg)).ok();
                    }
                };
            } else {
                Self::reconnecting::<S>(rt).await?
            }
        }
        log::warn!("{:?} exited its event loop", &rt.service().directory());
        Ok(())
    }
}

#[async_trait]
impl<S: SupHandle<Self>> Actor<S> for Mqtt<MessageMetadata> {
    type Data = CollectorHandles;
    type Channel = MqttChannel;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        self.initialize(rt).await
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, collectors_handles: Self::Data) -> ActorResult<()> {
        log::info!("{:?} is running", &rt.service().directory());
        while let Some(msg_ref_opt) = rt.inbox_mut().stream().next().await {
            if let Some(msg_ref) = msg_ref_opt {
                if let Ok(msg_ref) = serde_json::from_str::<MessageMetadata>(&msg_ref.payload_str()) {
                    // partitioning based on first byte of the message_id
                    let collector_partition_id = self.partitioner.partition_id(&msg_ref.message_id);
                    if let Some(collector_handle) = collectors_handles.get(&collector_partition_id) {
                        collector_handle.send(CollectorEvent::MessageReferenced(msg_ref)).ok();
                    }
                };
            } else {
                Self::reconnecting(rt).await?
            }
        }
        Ok(())
    }
}

/// Mqtt state
pub struct Mqtt<T: Topic> {
    url: Url,
    stream_capacity: usize,
    partitioner: MessageIdPartitioner,
    _topic: std::marker::PhantomData<T>,
}

impl<T: Topic> Mqtt<T> {
    /// Create new mqtt actor struct
    pub(super) fn new(url: Url, stream_capacity: usize, partitioner: MessageIdPartitioner) -> Self {
        Self {
            url,
            stream_capacity,
            partitioner,
            _topic: std::marker::PhantomData,
        }
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

impl Topic for Message {
    fn name() -> &'static str {
        "messages"
    }
    fn qos() -> i32 {
        0
    }
}

impl Topic for MessageMetadata {
    fn name() -> &'static str {
        "messages/referenced"
    }
    fn qos() -> i32 {
        1
    }
}
