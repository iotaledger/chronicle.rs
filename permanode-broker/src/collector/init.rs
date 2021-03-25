// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> Init<BrokerHandle<H>> for Collector {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        // Get the first keyspace or default to "permanode"
        info!(
            "{} is Initializing, with permanode keyspace: {}",
            self.get_name(),
            self.default_keyspace.name()
        );
        self.spawn_requester();
        status
    }
}

impl Collector {
    fn spawn_requester(&mut self) {
        for id in 0..self.requester_count {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let handle = RequesterHandle {
                id,
                tx,
                processed_count: 0,
            };
            self.requester_handles.push(handle);
            let inbox = RequesterInbox { rx };
            let reqwest_client = self.reqwest_client.clone();
            let requester = RequesterBuilder::new()
                .inbox(inbox)
                .requester_id(id)
                .api_endpoints(self.api_endpoints.clone())
                .reqwest_client(reqwest_client)
                .build();
            tokio::spawn(requester.start(self.handle.clone()));
        }
    }
}
