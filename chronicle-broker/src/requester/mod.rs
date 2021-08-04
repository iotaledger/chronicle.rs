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
    collections::{
        HashSet,
        VecDeque,
    },
    convert::TryFrom,
    iter::FromIterator,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::RwLock;
use url::Url;

pub(crate) type RequesterId = u8;

/// Requester events
pub enum RequesterEvent {
    /// Requesting MessageId in order to solidifiy u32 MilestoneIndex
    RequestFullMessage(MessageId, u32),
    /// Requesting Milestone for u32 milestone index;
    RequestMilestone(u32),
    /// RequesterTopology event, to update the api endpoints
    TopologyChange,
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
pub fn build_requester(requester_id: u8, reqwest_client: Client, retries_per_endpoint: Option<usize>) -> Requester {
    // we retry up to N times per api endpoint for a given request
    Requester {
        requester_id,
        api_endpoints: Default::default(),
        reqwest_client,
        retries: retries_per_endpoint.unwrap_or(5),
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
                    debug!("Requesting full message {}", message_id);
                    self.request_full_message_with_retries(&collector_handle, message_id, try_ms_index)
                        .await;
                }
                RequesterEvent::RequestMilestone(milestone_index) => {
                    debug!("Requesting milestone {}", milestone_index);
                    self.request_milestone_message_with_retries(&collector_handle, milestone_index)
                        .await;
                }
                RequesterEvent::TopologyChange => {
                    self.api_endpoints = match rt.resource::<Arc<RwLock<HashSet<Url>>>>().await {
                        Some(e) => e.read().await.iter().cloned().collect(),
                        None => VecDeque::new(),
                    };
                    self.shuffle();
                }
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
        ms_index: u32,
    ) {
        let mut full_message = None;
        if !self.api_endpoints.is_empty() {
            for _ in 0..self.api_endpoints.len() {
                let endpoint = self.api_endpoints.pop_front().unwrap();
                let mut retries = self.retries;
                while retries > 0 {
                    match self.request_message_and_metadata(&endpoint, message_id).await {
                        Ok(msg) => {
                            full_message = Some(msg);
                            break;
                        }
                        Err(e) => {
                            error!("{}", e);
                            retries -= 1;
                        }
                    }
                }
                self.api_endpoints.push_back(endpoint);
                if full_message.is_some() {
                    break;
                }
            }
        } else {
            error!("No endpoints to request from! {}", self.name());
        }
        collector_handle
            .send(CollectorEvent::MessageAndMeta(ms_index, Some(message_id), full_message))
            .ok();
    }

    async fn request_milestone_message_with_retries(
        &mut self,
        collector_handle: &Act<Collector>,
        milestone_index: u32,
    ) {
        let mut full_message = None;
        if !self.api_endpoints.is_empty() {
            for _ in 0..self.api_endpoints.len() {
                let endpoint = self.api_endpoints.pop_front().unwrap();
                let mut retries = self.retries;
                while retries > 0 {
                    match self.request_milestone_message(&endpoint, milestone_index).await {
                        Ok(msg) => {
                            full_message = Some(msg);
                            break;
                        }
                        Err(e) => {
                            error!("{}", e);
                            retries -= 1;
                        }
                    }
                }
                self.api_endpoints.push_back(endpoint);
                if full_message.is_some() {
                    break;
                }
            }
        } else {
            error!("No endpoints to request from! {}", self.name());
        }
        collector_handle
            .send(CollectorEvent::MessageAndMeta(
                milestone_index,
                full_message.as_ref().map(|f| f.metadata().message_id),
                full_message,
            ))
            .ok();
    }

    async fn request_milestone_message(
        &mut self,
        remote_url: &Url,
        milestone_index: u32,
    ) -> anyhow::Result<FullMessage> {
        let get_milestone_url = remote_url.join(&format!("milestones/{}", milestone_index)).unwrap();
        let milestone_response = self
            .reqwest_client
            .get(get_milestone_url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Error sending request for milestone: {}", e))?;

        if milestone_response.status().is_success() {
            let milestone = milestone_response
                .json::<JsonData<MilestoneResponse>>()
                .await
                .map_err(|e| anyhow::anyhow!("Error deserializing milestone: {}", e))?
                .into_inner();
            let message_id = MessageId::from_str(&milestone.message_id).expect("Expected message_id as string");
            return self.request_message_and_metadata(remote_url, message_id).await;
        } else {
            let url = milestone_response.url().clone();
            let err = milestone_response.json::<Value>().await;
            anyhow::bail!("Received error requesting milestone from {}:\n {:#?}", url, err);
        }
    }

    async fn request_message_and_metadata(
        &mut self,
        remote_url: &Url,
        message_id: MessageId,
    ) -> anyhow::Result<FullMessage> {
        let get_message_url = remote_url.join(&format!("messages/{}", message_id)).unwrap();
        let get_metadata_url = remote_url.join(&format!("messages/{}/metadata", message_id)).unwrap();
        let message_response = self
            .reqwest_client
            .get(get_message_url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Error sending request for message: {}", e))?;
        let metadata_response = self
            .reqwest_client
            .get(get_metadata_url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Error sending request for metadata: {}", e))?;
        if message_response.status().is_success() && metadata_response.status().is_success() {
            let message = message_response
                .json::<JsonData<MessageDto>>()
                .await
                .map_err(|e| anyhow::anyhow!("Error deserializing message: {}", e))?;
            let metadata = metadata_response
                .json::<JsonData<MessageMetadata>>()
                .await
                .map_err(|e| anyhow::anyhow!("Error deserializing metadata: {}", e))?;

            let message_dto = message.into_inner();
            let message = Message::try_from(&message_dto).unwrap();
            let metadata = metadata.into_inner();
            if metadata.referenced_by_milestone_index.is_some() {
                let full_message = FullMessage::new(message, metadata);
                return Ok(full_message);
            } else {
                anyhow::bail!("Found unreferenced message!");
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
            anyhow::bail!("Error requesting message or metadata!");
        }
    }
}
