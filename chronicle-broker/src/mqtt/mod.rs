// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    collector::{
        Collector,
        CollectorEvent,
        MessageIdPartitioner,
    },
    *,
};
use futures::{
    stream::StreamExt,
    Stream,
};
use std::time::Duration;
/// Mqtt state
pub struct Mqtt<T> {
    url: Url,
    stream_capacity: usize,
    partitioner: MessageIdPartitioner,
    topic: T,
}

#[build]
pub fn build_mqtt<T>(url: Url, topic: T, collector_count: u8, stream_capacity: Option<usize>) -> Mqtt<T> {
    Mqtt::<T> {
        url,
        partitioner: MessageIdPartitioner::new(collector_count),
        stream_capacity: stream_capacity.unwrap_or(10000),
        topic,
    }
}

#[async_trait]
impl Actor for Mqtt<Messages> {
    type Dependencies = Pool<MapPool<Collector, u8>>;
    type Event = paho_mqtt::Message;
    type Channel = MqttChannel;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError> {
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        collectors_handles: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(msg) = rt.next_event().await {
            if let Ok(msg) = Message::unpack(&mut msg.payload()) {
                let (message_id, _) = msg.id();
                // partitioning based on first byte of the message_id
                let collector_partition_id = self.partitioner.partition_id(&message_id);
                if let Some(mut collector_handle) =
                    collectors_handles.read().await.get(&collector_partition_id).cloned()
                {
                    collector_handle
                        .send(CollectorEvent::Message(message_id, msg))
                        .await
                        .ok();
                }
            };
        }
        Ok(())
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Mqtt<Messages> ({})", self.url).into()
    }
}

#[async_trait]
impl Actor for Mqtt<MessagesReferenced> {
    type Dependencies = Pool<MapPool<Collector, u8>>;
    type Event = paho_mqtt::Message;
    type Channel = MqttChannel;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError> {
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        collectors_handles: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(msg_ref) = rt.next_event().await {
            if let Ok(msg_ref) = serde_json::from_str::<MessageMetadata>(&msg_ref.payload_str()) {
                // partitioning based on first byte of the message_id
                let collector_partition_id = self.partitioner.partition_id(&msg_ref.message_id);
                if let Some(mut collector_handle) =
                    collectors_handles.read().await.get(&collector_partition_id).cloned()
                {
                    collector_handle
                        .send(CollectorEvent::MessageReferenced(msg_ref))
                        .await
                        .ok();
                }
            };
        }
        Ok(())
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Mqtt<MessagesReferenced> ({})", self.url).into()
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
pub struct Messages;

impl Topic for Messages {
    fn name() -> &'static str {
        "messages"
    }
    fn qos() -> i32 {
        0
    }
}

/// Mqtt MessagesReferenced topic
pub struct MessagesReferenced;

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

pub struct MqttChannel;

#[async_trait]
impl<T> Channel<Mqtt<T>, paho_mqtt::Message> for MqttChannel
where
    Mqtt<T>: Actor,
    T: Topic,
{
    type Sender = ();
    type Receiver = MqttReceiver;

    async fn new(config: &Mqtt<T>) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
        let random_id: u64 = rand::random();
        let create_opts = CreateOptionsBuilder::new()
            .server_uri(&config.url.as_str()[..])
            .client_id(&format!("{}|{}", config.name(), random_id))
            .persistence(None)
            .finalize();
        let mut client = AsyncClient::new(create_opts)
            .map_err(|e| anyhow::anyhow!("Unable to create AsyncClient: {}, error: {}", config.url.as_str(), e))?;
        info!("Created AsyncClient: {}", &config.url.to_string());
        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(120))
            .mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .connect_timeout(Duration::from_secs(60))
            .finalize();
        let stream = client.get_stream(config.stream_capacity);
        // connect client with the remote broker
        client.connect(conn_opts).await.map_err(|e| {
            anyhow::anyhow!(
                "Unable to connect AsyncClient: {}, topic: {}, error: {}",
                &config.url.as_str(),
                T::name(),
                e
            )
        })?;
        info!("Connected AsyncClient: {}", &config.url.as_str());
        // subscribe to T::name() topic with T::qos()
        client.subscribe(T::name(), T::qos()).await.map_err(|e| {
            anyhow::anyhow!(
                "Unable to subscribe AsyncClient: {}, topic: {}, error: {}",
                &config.url.as_str(),
                T::name(),
                e
            )
        })?;
        Ok(((), MqttReceiver { client, stream }))
    }
}

pub struct MqttReceiver {
    client: AsyncClient,
    stream: futures::channel::mpsc::Receiver<Option<paho_mqtt::Message>>,
}

#[async_trait]
impl Receiver<paho_mqtt::Message> for MqttReceiver {
    async fn recv(&mut self) -> Option<paho_mqtt::Message> {
        self.stream.next().await.flatten()
    }
}

impl Stream for MqttReceiver {
    type Item = paho_mqtt::Message;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            std::task::Poll::Ready(m) => std::task::Poll::Ready(m.flatten()),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
