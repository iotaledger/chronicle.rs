// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl Terminating<CollectorHandle> for Requester {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<CollectorHandle>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        let event = CollectorEvent::Internal(Internal::Service(self.service.clone()));
        let _ = supervisor.as_mut().expect("Expected Collector handle").send(event);
        _status
    }
}
