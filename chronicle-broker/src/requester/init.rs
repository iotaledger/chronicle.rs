// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use rand::{
    prelude::SliceRandom,
    thread_rng,
};
use std::iter::FromIterator;

#[async_trait::async_trait]
impl Init<CollectorHandle> for Requester {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<CollectorHandle>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        self.shuffle();
        let event = CollectorEvent::Internal(Internal::Service(self.service.clone()));
        let _ = supervisor.as_mut().expect("Expected Collector handle").send(event);
        status
    }
}

impl Requester {
    /// shuffle the api_endpoints
    pub(crate) fn shuffle(&mut self) {
        let mut vec_api_endpoints = self.api_endpoints.iter().map(|e| e.clone()).collect::<Vec<_>>();
        vec_api_endpoints.shuffle(&mut thread_rng());
        self.api_endpoints = VecDeque::from_iter(vec_api_endpoints);
    }
}
