// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use serde_json::Value;

use super::*;
#[async_trait::async_trait]
impl EventLoop<CollectorHandle> for Requester {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<CollectorHandle>,
    ) -> Result<(), Need> {
        let collector_handle = _supervisor.as_mut().unwrap();
        while let Some(event) = self.inbox.recv().await {
            match event {
                RequesterEvent::RequestFullMessage(message_id, try_ms_index) => {
                    self.request_message_and_metadata(collector_handle, message_id, try_ms_index)
                        .await
                }
            }
        }
        Ok(())
    }
}

impl Requester {
    async fn request_message_and_metadata(
        &mut self,
        collector_handle: &CollectorHandle,
        message_id: MessageId,
        try_ms_index: u32,
    ) {
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
                    let message_dto = message.into_data();
                    let message = Message::try_from(&message_dto).unwrap();
                    let metadata = metadata.into_data();
                    if metadata.referenced_by_milestone_index.is_some() {
                        let full_message = FullMessage::new(message, metadata);
                        let collector_event = CollectorEvent::MessageAndMeta(
                            self.requester_id,
                            try_ms_index,
                            message_id,
                            Some(full_message),
                        );
                        let _ = collector_handle.send(collector_event);
                        return ();
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
        let collector_event = CollectorEvent::MessageAndMeta(self.requester_id, try_ms_index, message_id, None);
        let _ = collector_handle.send(collector_event);
    }
}
