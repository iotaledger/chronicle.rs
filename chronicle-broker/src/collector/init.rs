// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> Init<BrokerHandle<H>> for Collector {
    async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        // Get the first keyspace or default to "chronicle"
        info!(
            "{} is Initializing, with chronicle keyspace: {}",
            self.get_name(),
            self.default_keyspace.name()
        );
        self.service.update_status(ServiceStatus::Initializing);
        let event = BrokerEvent::Children(BrokerChild::Collector(self.service.clone()));
        let _ = _supervisor
            .as_mut()
            .expect("Collector expected BrokerHandle")
            .send(event);
        self.spawn_requester();
        status
    }
}

impl Collector {
    fn spawn_requester(&mut self) {
        for id in 0..self.requester_count {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let inbox = RequesterInbox { rx };
            let reqwest_client = self.reqwest_client.clone();
            let requester = RequesterBuilder::new()
                .inbox(inbox)
                .requester_id(id)
                .api_endpoints(self.api_endpoints.iter().cloned().collect())
                .retries_per_endpoint(5)
                .reqwest_client(reqwest_client)
                .build();
            let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();
            let handle = RequesterHandle {
                id,
                tx,
                abort_handle,
                processed_count: 0,
            };
            self.requester_handles.push(handle);
            tokio::spawn(requester.start_abortable(abort_registration, self.handle.clone()));
        }
    }
}
