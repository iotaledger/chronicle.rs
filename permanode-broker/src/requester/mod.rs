// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    application::*,
    collector::*,
    solidifier::FullMessage,
};
use bee_rest_api::types::{
    dtos::MessageDto,
    responses::MilestoneResponse,
};
use std::{
    collections::VecDeque,
    convert::TryFrom,
    ops::{
        Deref,
        DerefMut,
    },
};

mod event_loop;
mod init;
mod terminating;
use reqwest::Client;
use url::Url;
// Requester builder
builder!(RequesterBuilder {
    requester_id: u8,
    inbox: RequesterInbox,
    api_endpoints: VecDeque<Url>,
    reqwest_client: Client,
    retries: usize
});
pub type RequesterId = u8;
pub enum RequesterEvent {
    /// Requesting MessageId in order to solidifiy u32 MilestoneIndex
    RequestFullMessage(MessageId, u32),
    /// Requesting Milestone for u32 milestone index;
    RequestMilestone(u32),
}

#[derive(Clone)]
pub struct RequesterHandle {
    pub(crate) id: RequesterId,
    pub(crate) processed_count: u64,
    pub(crate) abort_handle: futures::future::AbortHandle,
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<RequesterEvent>,
}

impl RequesterHandle {
    pub(crate) fn decrement(&mut self) {
        self.processed_count -= 1;
    }
    pub(crate) fn send_event(&mut self, event: RequesterEvent) {
        let _ = self.tx.send(event);
        self.processed_count += 1;
    }
}
impl std::cmp::Ord for RequesterHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.processed_count.cmp(&self.processed_count)
    }
}
impl std::cmp::PartialOrd for RequesterHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.processed_count.cmp(&self.processed_count))
    }
}
impl std::cmp::PartialEq for RequesterHandle {
    fn eq(&self, other: &Self) -> bool {
        if self.id == other.id {
            true
        } else {
            false
        }
    }
}
impl std::cmp::Eq for RequesterHandle {}
/// RequesterInbox is used to recv requests from collector
pub struct RequesterInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<RequesterEvent>,
}

impl Deref for RequesterInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<RequesterEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for RequesterInbox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Shutdown for RequesterHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.abort_handle.abort();
        None
    }
}

// Requester state
pub struct Requester {
    service: Service,
    requester_id: u8,
    inbox: RequesterInbox,
    api_endpoints: VecDeque<Url>,
    reqwest_client: Client,
    retries: usize,
}

impl ActorBuilder<CollectorHandle> for RequesterBuilder {}

/// implementation of builder
impl Builder for RequesterBuilder {
    type State = Requester;
    fn build(self) -> Self::State {
        let api_endpoints = self.api_endpoints.unwrap();
        // we retry up to 5 times per api endpoint for a given request
        let retries = api_endpoints.len() * 5 as usize;
        Self::State {
            service: Service::new(),
            inbox: self.inbox.unwrap(),
            requester_id: self.requester_id.unwrap(),
            api_endpoints,
            reqwest_client: self.reqwest_client.unwrap(),
            retries,
        }
        .set_name()
    }
}

/// impl name of the Requester
impl Name for Requester {
    fn set_name(mut self) -> Self {
        let name = format!("Requester_{}", self.requester_id);
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl AknShutdown<Requester> for CollectorHandle {
    async fn aknowledge_shutdown(self, mut _state: Requester, _status: Result<(), Need>) {
        // Dropping the collector handle should be enough.
    }
}
