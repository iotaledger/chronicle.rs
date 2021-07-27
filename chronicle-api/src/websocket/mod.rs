// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use backstage::prefabs::websocket::WebsocketChildren;
use chronicle_broker::{
    application::{
        BrokerEvent,
        BrokerRequest,
        ChronicleBroker,
        ImportType,
    },
    requester::RequesterTopology,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    marker::PhantomData,
    net::SocketAddr,
    ops::Range,
    path::PathBuf,
};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub struct Websocket {
    pub listen_address: SocketAddr,
}

#[async_trait]
impl Actor for Websocket {
    type Dependencies = (
        Act<backstage::prefabs::websocket::Websocket<Self>>,
        Act<ChronicleBroker>,
    );
    type Event = WebsocketRequest;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let my_handle = rt.handle();
        let websocket = backstage::prefabs::websocket::WebsocketBuilder::new()
            .listen_address(self.listen_address)
            .supervisor_handle(my_handle)
            .build();
        rt.spawn_actor_unsupervised(websocket).await?;
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        (websocket, broker): Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(WebsocketRequest(addr, msg)) = rt.next_event().await {
            log::debug!("Received message {} from {}", msg, addr);
            if let Some(msg) = {
                if let Message::Text(t) = msg {
                    serde_json::from_str::<ChronicleRequest>(&t).ok()
                } else {
                    None
                }
            } {
                match msg {
                    ChronicleRequest::AddMqttMessages(url) => {
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        if broker
                            .send(BrokerEvent::Websocket(BrokerRequest::AddMqttMessages(url, sender)))
                            .is_ok()
                        {
                            if let Ok(res) = receiver.await {
                                websocket
                                    .send(WebsocketChildren::Response(addr, format!("{:?}", res).into()))
                                    .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                            }
                        }
                    }
                    ChronicleRequest::AddMqttMessagesReferenced(url) => {
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        if broker
                            .send(BrokerEvent::Websocket(BrokerRequest::AddMqttMessagesReferenced(
                                url, sender,
                            )))
                            .is_ok()
                        {
                            if let Ok(res) = receiver.await {
                                websocket
                                    .send(WebsocketChildren::Response(addr, format!("{:?}", res).into()))
                                    .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                            }
                        }
                    }
                    ChronicleRequest::RemoveMqttMessages(url) => {
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        if broker
                            .send(BrokerEvent::Websocket(BrokerRequest::RemoveMqttMessages(url, sender)))
                            .is_ok()
                        {
                            if let Ok(res) = receiver.await {
                                websocket
                                    .send(WebsocketChildren::Response(addr, format!("{:?}", res).into()))
                                    .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                            }
                        }
                    }
                    ChronicleRequest::RemoveMqttMessagesReferenced(url) => {
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        if broker
                            .send(BrokerEvent::Websocket(BrokerRequest::RemoveMqttMessagesReferenced(
                                url, sender,
                            )))
                            .is_ok()
                        {
                            if let Ok(res) = receiver.await {
                                websocket
                                    .send(WebsocketChildren::Response(addr, format!("{:?}", res).into()))
                                    .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                            }
                        }
                    }
                    ChronicleRequest::Requester(t) => match t {
                        RequesterTopologyRequest::AddEndpoint(url) => {
                            let (sender, receiver) = tokio::sync::oneshot::channel();
                            if broker
                                .send(BrokerEvent::Websocket(BrokerRequest::Requester(
                                    RequesterTopology::AddEndpoint(url, sender),
                                )))
                                .is_ok()
                            {
                                if let Ok(res) = receiver.await {
                                    websocket
                                        .send(WebsocketChildren::Response(addr, format!("{:?}", res).into()))
                                        .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                                }
                            }
                        }
                        RequesterTopologyRequest::RemoveEndpoint(url) => {
                            let (sender, receiver) = tokio::sync::oneshot::channel();
                            if broker
                                .send(BrokerEvent::Websocket(BrokerRequest::Requester(
                                    RequesterTopology::RemoveEndpoint(url, sender),
                                )))
                                .is_ok()
                            {
                                if let Ok(res) = receiver.await {
                                    websocket
                                        .send(WebsocketChildren::Response(addr, format!("{:?}", res).into()))
                                        .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                                }
                            }
                        }
                    },
                    ChronicleRequest::Import {
                        path,
                        resume,
                        import_range,
                        import_type,
                    } => {
                        let (responder, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                        if broker
                            .send(BrokerEvent::Websocket(BrokerRequest::Import {
                                path,
                                resume,
                                import_range,
                                import_type,
                                responder,
                            }))
                            .is_ok()
                        {
                            while let Some(res) = receiver.recv().await {
                                websocket
                                    .send(WebsocketChildren::Response(
                                        addr,
                                        serde_json::to_string(&res).unwrap().into(),
                                    ))
                                    .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                            }
                        }
                    }
                    ChronicleRequest::ExitProgram => {
                        let res = rt.shutdown_scope(&ROOT_SCOPE).await;
                        websocket
                            .send(WebsocketChildren::Response(addr, format!("{:?}", res).into()))
                            .map_err(|e| anyhow::anyhow!("Websocket error: {}", e))?;
                    }
                    ChronicleRequest::Config(req) => match req {
                        ConfigRequest::Rollback => {
                            // TODO
                        }
                    },
                }
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

pub struct WebsocketRequest(SocketAddr, Message);

impl From<(SocketAddr, Message)> for WebsocketRequest {
    fn from((addr, msg): (SocketAddr, Message)) -> Self {
        WebsocketRequest(addr, msg)
    }
}

#[derive(Serialize, Deserialize)]
pub enum ChronicleRequest {
    ExitProgram,
    /// Add new MQTT Messages feed source
    AddMqttMessages(Url),
    /// Add new MQTT Messages Referenced feed source
    AddMqttMessagesReferenced(Url),
    /// Remove a MQTT Messages feed source
    RemoveMqttMessages(Url),
    /// Remove a MQTT Messages Referenced feed source
    RemoveMqttMessagesReferenced(Url),
    Requester(RequesterTopologyRequest),
    Import {
        /// File or dir path which supposed to contain LogFiles
        path: PathBuf,
        /// Resume the importing process
        resume: bool,
        /// Provide optional import range
        import_range: Option<Range<u32>>,
        /// The type of import requested
        import_type: ImportType,
    },
    Config(ConfigRequest),
}

#[derive(Serialize, Deserialize)]
pub enum RequesterTopologyRequest {
    /// Add new Api Endpoint
    AddEndpoint(Url),
    /// Remove existing Api Endpoint
    RemoveEndpoint(Url),
}

#[derive(Serialize, Deserialize)]
pub enum ConfigRequest {
    Rollback,
}
