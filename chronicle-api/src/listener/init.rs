// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait]
impl<T: APIEngine, H: ChronicleAPIScope> Init<ChronicleAPISender<H>> for Listener<T> {
    async fn init(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<ChronicleAPISender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let Some(ref mut supervisor) = supervisor {
            supervisor
                .send(ChronicleAPIEvent::Children(ChronicleAPIChild::Listener(
                    self.service.clone(),
                )))
                .map_err(|_| Need::Abort)
        } else {
            Err(Need::Abort)
        }
    }
}
