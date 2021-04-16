// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait]
impl<H: ChronicleBrokerScope> Terminating<H> for ChronicleBroker<H> {
    async fn terminating(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // Invoke shutdown in case we got aborted
        if let Err(Need::Abort) = status {
            self.shutdown(_supervisor.as_mut().unwrap(), true).await;
        }
        status
    }
}
