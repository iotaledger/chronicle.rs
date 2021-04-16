// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::Wrapper;
use serde_json::Value;

#[async_trait::async_trait]
impl EventLoop<CollectorHandle> for Requester {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<CollectorHandle>,
    ) -> Result<(), Need> {
        let collector_handle = _supervisor.as_mut().expect("Requester expected collector handle");
        while let Some(event) = self.inbox.recv().await {
            match event {
                RequesterEvent::RequestFullMessage(message_id, try_ms_index) => {
                    self.request_full_message_with_retries(collector_handle, message_id, try_ms_index)
                        .await;
                }
                RequesterEvent::RequestMilestone(milestone_index) => {
                    self.request_milestone_message_with_retries(collector_handle, milestone_index)
                        .await;
                }
            }
        }
        Ok(())
    }
}
use std::str::FromStr;

impl Requester {
    async fn request_full_message_with_retries(
        &mut self,
        collector_handle: &mut CollectorHandle,
        message_id: MessageId,
        try_ms_index: u32,
    ) {
        let mut retries = self.retries;
        loop {
            if retries > 0 {
                if let Ok(full_message) = self.request_message_and_metadata(message_id).await {
                    self.respond_to_collector(collector_handle, try_ms_index, Some(message_id), Some(full_message));
                    break;
                } else {
                    retries -= 1;
                    // keep retrying, but yield to keep the system responsive
                    tokio::task::yield_now().await;
                    continue;
                }
            } else {
                self.respond_to_collector(collector_handle, try_ms_index, None, None);
                break;
            }
        }
    }
    async fn request_milestone_message_with_retries(
        &mut self,
        collector_handle: &mut CollectorHandle,
        milestone_index: u32,
    ) {
        let mut retries = self.retries;
        loop {
            if retries > 0 {
                if let Ok(full_message) = self.request_milestone_message(milestone_index).await {
                    self.respond_to_collector(
                        collector_handle,
                        milestone_index,
                        Some(full_message.metadata().message_id),
                        Some(full_message),
                    );
                    break;
                } else {
                    retries -= 1;
                    // keep retrying, but yield to keep the system responsive
                    tokio::task::yield_now().await;
                    continue;
                }
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
        let collector_event =
            CollectorEvent::MessageAndMeta(self.requester_id, ms_index, opt_message_id, opt_full_message);
        let _ = collector_handle.send(collector_event);
    }
    async fn request_milestone_message(&mut self, milestone_index: u32) -> Result<FullMessage, ()> {
        let remote_url = self.api_endpoints.pop_back().unwrap();
        self.api_endpoints.push_front(remote_url.clone());
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
                    return self.request_message_and_metadata(message_id).await;
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
    async fn request_message_and_metadata(&mut self, message_id: MessageId) -> Result<FullMessage, ()> {
        let remote_url = self.api_endpoints.pop_back().unwrap();
        self.api_endpoints.push_front(remote_url.clone());
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
