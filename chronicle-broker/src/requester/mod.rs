// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    application::ChronicleBroker,
    collector::{
        CollectorEvent,
        CollectorHandle,
        CollectorId,
    },
    *,
};
use backstage::core::Event;
use bee_rest_api::types::{
    dtos::MessageDto,
    responses::MilestoneResponse,
};
use chronicle_common::Wrapper;
use chronicle_filter::SelectiveBuilder;
use collector::CollectorHandles;
use rand::{
    prelude::SliceRandom,
    thread_rng,
};
use reqwest::Client;
use serde_json::Value;
use std::{
    collections::VecDeque,
    iter::FromIterator,
    str::FromStr,
};
use url::Url;
pub(crate) type RequesterHandle<T: SelectiveBuilder> = UnboundedHandle<RequesterEvent<T>>;

/// Requester events
#[derive(Debug)]
pub enum RequesterEvent<T: SelectiveBuilder> {
    /// Collector requesting MessageId in order to solidifiy u32 MilestoneIndex
    RequestFullMessage(CollectorId, MessageId, u32),
    /// Requesting Milestone for u32 milestone index;
    RequestMilestone(CollectorId, u32),
    /// Subscribed backstage event
    ChronicleBroker(Event<ChronicleBroker<T>>),
    /// Shutdown the requester variant
    Shutdown,
}

impl<T: SelectiveBuilder> ShutdownEvent for RequesterEvent<T> {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}
/// Requester state
#[derive(Debug)]
pub struct Requester<T: SelectiveBuilder> {
    api_endpoints: VecDeque<Url>,
    reqwest_client: Client,
    retries: u8,
    _marker: std::marker::PhantomData<T>,
}

#[derive(Clone, Debug)]
pub struct RequesterHandles<T: SelectiveBuilder> {
    inner: VecDeque<RequesterHandle<T>>,
}

impl<T: SelectiveBuilder> RequesterHandles<T> {
    pub(super) fn new() -> Self {
        Self { inner: VecDeque::new() }
    }
    pub(super) fn push_front(&mut self, requester_handle: RequesterHandle<T>) {
        self.inner.push_front(requester_handle)
    }
    pub fn send(&mut self, event: RequesterEvent<T>) -> Option<()> {
        self.inner.pop_front().and_then(|h| {
            let r = h.send(event).ok();
            self.inner.push_back(h);
            r
        })
    }
}
impl<T: SelectiveBuilder> From<Event<ChronicleBroker<T>>> for RequesterEvent<T> {
    fn from(event: Event<ChronicleBroker<T>>) -> Self {
        Self::ChronicleBroker(event)
    }
}

#[async_trait]
impl<S: SupHandle<Self>, T: SelectiveBuilder> Actor<S> for Requester<T> {
    type Data = (ChronicleBroker<T>, CollectorHandles);
    type Channel = UnboundedChannel<RequesterEvent<T>>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("Requester without parent"))?;
        let chronicle_broker = rt
            .subscribe(parent_id, "ChronicleBroker".to_string())
            .await?
            .ok_or_else(|| ActorError::exit_msg("Unable to get the first ChronicleBroker copy"))?;
        self.update_endpoints(&chronicle_broker);
        let collector_handles = rt.depends_on(parent_id).await?;
        Ok((chronicle_broker, collector_handles))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (mut broker, collector_handles): Self::Data) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                RequesterEvent::RequestFullMessage(collector_id, message_id, try_ms_index) => {
                    if let Some(collector_handle) = collector_handles.get(&collector_id) {
                        self.request_full_message_with_retries(collector_handle, message_id, try_ms_index)
                            .await;
                    } else {
                        error!("Invalid collector_id, unable to request full message {}", message_id);
                    }
                }
                RequesterEvent::RequestMilestone(collector_id, milestone_index) => {
                    if let Some(collector_handle) = collector_handles.get(&collector_id) {
                        self.request_milestone_message_with_retries(collector_handle, milestone_index)
                            .await;
                    } else {
                        error!("Invalid collector_id, unable to request milestone {}", milestone_index);
                    }
                }
                RequesterEvent::ChronicleBroker(backstage_event) => {
                    if let Event::Published(_, _, updated_chronicle_broker) = backstage_event {
                        self.update_endpoints(&updated_chronicle_broker);
                        broker = updated_chronicle_broker;
                    }
                }
                RequesterEvent::Shutdown => break,
            }
        }
        Ok(())
    }
}

impl<T: SelectiveBuilder> Requester<T> {
    /// Create new request
    pub(super) fn new(reqwest_client: Client, retries: u8) -> Self {
        Self {
            reqwest_client,
            api_endpoints: VecDeque::new(),
            retries,
            _marker: std::marker::PhantomData,
        }
    }
    fn update_endpoints(&mut self, broker: &ChronicleBroker<T>) {
        self.api_endpoints = VecDeque::from_iter(broker.api_endpoints.clone().into_iter());
        self.shuffle()
    }
    /// shuffle the api_endpoints
    fn shuffle(&mut self) {
        let mut vec_api_endpoints = self.api_endpoints.iter().map(|e| e.clone()).collect::<Vec<_>>();
        vec_api_endpoints.shuffle(&mut thread_rng());
        self.api_endpoints = VecDeque::from_iter(vec_api_endpoints);
    }
    async fn request_full_message_with_retries(
        &mut self,
        collector_handle: &CollectorHandle,
        message_id: MessageId,
        try_ms_index: u32,
    ) {
        let mut retries = self.retries;
        loop {
            if retries > 0 {
                if let Some(remote_url) = self.api_endpoints.pop_front() {
                    if let Ok(full_message) = self.request_message_and_metadata(&remote_url, message_id).await {
                        self.respond_to_collector(collector_handle, try_ms_index, Some(message_id), Some(full_message));
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
                    self.respond_to_collector(collector_handle, try_ms_index, None, None);
                    break;
                };
            } else {
                self.respond_to_collector(collector_handle, try_ms_index, None, None);
                break;
            }
        }
    }
    async fn request_milestone_message_with_retries(
        &mut self,
        collector_handle: &CollectorHandle,
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
                        );
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
                    self.respond_to_collector(collector_handle, milestone_index, None, None);
                    break;
                };
            } else {
                self.respond_to_collector(collector_handle, milestone_index, None, None);
                break;
            }
        }
    }
    fn respond_to_collector(
        &self,
        collector_handle: &CollectorHandle,
        ms_index: u32,
        opt_message_id: Option<MessageId>,
        opt_full_message: Option<FullMessage>,
    ) {
        let collector_event = CollectorEvent::MessageAndMeta(ms_index, opt_message_id, opt_full_message);
        let _ = collector_handle.send(collector_event);
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
