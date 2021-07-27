// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    collector::{
        Collector,
        CollectorEvent,
    },
    *,
};
use bee_rest_api::types::{
    dtos::MessageDto,
    responses::MilestoneResponse,
};
use chronicle_common::Wrapper;
use rand::{
    prelude::SliceRandom,
    thread_rng,
};
use reqwest::Client;
use serde_json::Value;
use std::{
    collections::VecDeque,
    convert::TryFrom,
    iter::FromIterator,
    str::FromStr,
};
use tokio::sync::oneshot;
use url::Url;

pub(crate) type RequesterId = u8;

/// Requester events
pub enum RequesterEvent {
    /// Requesting MessageId in order to solidifiy u32 MilestoneIndex
    RequestFullMessage(MessageId, u32),
    /// Requesting Milestone for u32 milestone index;
    RequestMilestone(u32),
    /// RequesterTopology event, to update the api endpoints
    Topology(RequesterTopology),
}

/// Requester topology used by admins to add/remove IOTA api endpoints
pub enum RequesterTopology {
    /// Add new Api Endpoint
    AddEndpoint(Url, oneshot::Sender<anyhow::Result<()>>),
    /// Remove existing Api Endpoint
    RemoveEndpoint(Url, oneshot::Sender<anyhow::Result<()>>),
}

/// Requester state
pub struct Requester {
    requester_id: u8,
    api_endpoints: VecDeque<Url>,
    reqwest_client: Client,
    retries: usize,
}

#[build]
#[derive(Clone)]
pub fn build_requester(
    requester_id: u8,
    api_endpoints: VecDeque<Url>,
    reqwest_client: Client,
    retries_per_endpoint: Option<usize>,
) -> Requester {
    // we retry up to N times per api endpoint for a given request
    let retries_per_endpoint = retries_per_endpoint.unwrap_or(5);
    let retries = api_endpoints.len() * retries_per_endpoint;
    Requester {
        requester_id,
        api_endpoints,
        reqwest_client,
        retries,
    }
}

#[async_trait]
impl Actor for Requester {
    type Dependencies = Act<Collector>;
    type Event = RequesterEvent;
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
        self.shuffle();
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        collector_handle: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(event) = rt.next_event().await {
            match event {
                RequesterEvent::RequestFullMessage(message_id, try_ms_index) => {
                    self.request_full_message_with_retries(&collector_handle, message_id, try_ms_index)
                        .await;
                }
                RequesterEvent::RequestMilestone(milestone_index) => {
                    self.request_milestone_message_with_retries(&collector_handle, milestone_index)
                        .await;
                }
                RequesterEvent::Topology(topology) => match topology {
                    RequesterTopology::AddEndpoint(url, responder) => {
                        info!("Trying to AddEndpoint: {}", url);
                        if self.api_endpoints.iter().all(|u| u != &url) {
                            info!("AddedEndpoint: {}", url);
                            self.api_endpoints.push_front(url);
                        }
                        self.shuffle();
                        responder.send(Ok(()));
                    }
                    RequesterTopology::RemoveEndpoint(url, responder) => {
                        info!("Trying to RemoveEndpoint: {}", url);
                        if let Some(p) = self.api_endpoints.iter().position(|u| u == &url) {
                            info!("RemovedEndpoint: {}", url);
                            self.api_endpoints.remove(p);
                        }
                        responder.send(Ok(()));
                    }
                },
            }
        }
        Ok(())
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Requester ({})", self.requester_id).into()
    }
}

impl Requester {
    /// shuffle the api_endpoints
    pub(crate) fn shuffle(&mut self) {
        let mut vec_api_endpoints = self.api_endpoints.iter().map(|e| e.clone()).collect::<Vec<_>>();
        vec_api_endpoints.shuffle(&mut thread_rng());
        self.api_endpoints = VecDeque::from_iter(vec_api_endpoints);
    }

    async fn request_full_message_with_retries(
        &mut self,
        collector_handle: &Act<Collector>,
        message_id: MessageId,
        try_ms_index: u32,
    ) {
        let mut retries = self.retries;
        loop {
            if retries > 0 {
                if let Some(remote_url) = self.api_endpoints.pop_front() {
                    if let Ok(full_message) = self.request_message_and_metadata(&remote_url, message_id).await {
                        self.respond_to_collector(collector_handle, try_ms_index, Some(message_id), Some(full_message))
                            .await;
                        self.api_endpoints.push_front(remote_url);
                        break;
                    } else {
                        self.api_endpoints.push_back(remote_url);
                        retries -= 1;
                        // keep retrying, but yield to keep the system responsive
                        tokio::task::yield_now().await;
                        continue;
                    }
                } else {
                    self.respond_to_collector(collector_handle, try_ms_index, None, None)
                        .await;
                    break;
                };
            } else {
                self.respond_to_collector(collector_handle, try_ms_index, None, None)
                    .await;
                break;
            }
        }
    }
    async fn request_milestone_message_with_retries(
        &mut self,
        collector_handle: &Act<Collector>,
        milestone_index: u32,
    ) {
        let mut retries = self.retries;
        loop {
            if retries > 0 {
                if let Some(remote_url) = self.api_endpoints.pop_front() {
                    if let Ok(full_message) = self.request_milestone_message(&remote_url, milestone_index).await {
                        self.respond_to_collector(
                            collector_handle,
                            milestone_index,
                            Some(full_message.metadata().message_id),
                            Some(full_message),
                        )
                        .await;
                        self.api_endpoints.push_front(remote_url);
                        break;
                    } else {
                        self.api_endpoints.push_back(remote_url);
                        retries -= 1;
                        // keep retrying, but yield to keep the system responsive
                        tokio::task::yield_now().await;
                        continue;
                    }
                } else {
                    self.respond_to_collector(collector_handle, milestone_index, None, None)
                        .await;
                    break;
                };
            } else {
                self.respond_to_collector(collector_handle, milestone_index, None, None)
                    .await;
                break;
            }
        }
    }

    async fn respond_to_collector(
        &self,
        collector_handle: &Act<Collector>,
        ms_index: u32,
        opt_message_id: Option<MessageId>,
        opt_full_message: Option<FullMessage>,
    ) {
        collector_handle
            .send(CollectorEvent::MessageAndMeta(
                self.requester_id,
                ms_index,
                opt_message_id,
                opt_full_message,
            ))
            .ok();
    }

    async fn request_milestone_message(&mut self, remote_url: &Url, milestone_index: u32) -> Result<FullMessage, ()> {
        let get_milestone_url = remote_url.join(&format!("milestones/{}", milestone_index)).unwrap();
        let milestone_response = self
            .reqwest_client
            .get(get_milestone_url)
            .send()
            .await
            .map_err(|e| error!("Error sending request for milestone: {}", e));
        if let Ok(milestone_response) = milestone_response {
            if milestone_response.status().is_success() {
                let milestone = milestone_response
                    .json::<JsonData<MilestoneResponse>>()
                    .await
                    .map_err(|e| error!("Error deserializing milestone: {}", e));
                if let Ok(milestone) = milestone {
                    let milestone = milestone.into_inner();
                    let message_id = MessageId::from_str(&milestone.message_id).expect("Expected message_id as string");
                    return self.request_message_and_metadata(remote_url, message_id).await;
                }
            } else {
                if !milestone_response.status().is_success() {
                    let url = milestone_response.url().clone();
                    let err = milestone_response.json::<Value>().await;
                    error!("Received error requesting milestone from {}:\n {:#?}", url, err);
                }
            }
        }
        Err(())
    }
    async fn request_message_and_metadata(
        &mut self,
        remote_url: &Url,
        message_id: MessageId,
    ) -> Result<FullMessage, ()> {
        let get_message_url = remote_url.join(&format!("messages/{}", message_id)).unwrap();
        let get_metadata_url = remote_url.join(&format!("messages/{}/metadata", message_id)).unwrap();
        let message_response = self
            .reqwest_client
            .get(get_message_url)
            .send()
            .await
            .map_err(|e| error!("Error sending request for message: {}", e));
        let metadata_response = self
            .reqwest_client
            .get(get_metadata_url)
            .send()
            .await
            .map_err(|e| error!("Error sending request for metadata: {}", e));
        if let (Ok(message_response), Ok(metadata_response)) = (message_response, metadata_response) {
            if message_response.status().is_success() && metadata_response.status().is_success() {
                let message = message_response
                    .json::<JsonData<MessageDto>>()
                    .await
                    .map_err(|e| error!("Error deserializing message: {}", e));
                let metadata = metadata_response
                    .json::<JsonData<MessageMetadata>>()
                    .await
                    .map_err(|e| error!("Error deserializing metadata: {}", e));
                if let (Ok(message), Ok(metadata)) = (message, metadata) {
                    let message_dto = message.into_inner();
                    let message = Message::try_from(&message_dto).unwrap();
                    let metadata = metadata.into_inner();
                    if metadata.referenced_by_milestone_index.is_some() {
                        let full_message = FullMessage::new(message, metadata);
                        return Ok(full_message);
                    }
                }
            } else {
                if !message_response.status().is_success() {
                    let url = message_response.url().clone();
                    let err = message_response.json::<Value>().await;
                    error!("Received error requesting message from {}:\n {:#?}", url, err);
                }
                if !metadata_response.status().is_success() {
                    let url = metadata_response.url().clone();
                    let err = metadata_response.json::<Value>().await;
                    error!("Received error requesting metadata from {}:\n {:#?}", url, err);
                }
            }
        }
        Err(())
    }
}
