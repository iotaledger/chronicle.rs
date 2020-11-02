// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This helper module is for unit test and not for production.
//! Production setup should manage and setup everything through dashboard. Therefore this mod will be removed eventually
//! once we have dashboard ready.

use crate::dashboard;
use chronicle_common::actor;
use std::time::Duration;
actor!(
    HelperBuilder {
        nodes: Vec<String>,
        dashboard_tx: dashboard::Sender
});

impl HelperBuilder {
    /// Build the helper.
    pub fn build(self) -> Helper {
        Helper {
            nodes: self.nodes.unwrap(),
            dashboard_tx: self.dashboard_tx.unwrap(),
        }
    }
}

/// The helper structure.
pub struct Helper {
    /// Nodes for testing.
    nodes: Vec<String>,
    /// The dashboard transmission channel.
    dashboard_tx: dashboard::Sender,
}
impl Helper {
    /// Start to run the helper.
    pub async fn run(self) {
        // create delay duration
        let five_seconds = Duration::new(5, 0);
        // spawn nodes with delay in-between for simplicty
        for node_address in self.nodes {
            let event = dashboard::Event::Toplogy(dashboard::Toplogy::AddNode(node_address));
            let _ = self.dashboard_tx.0.send(event);
            tokio::time::delay_for(five_seconds).await;
        }
        // send tryBuild (assuming the nodes have been added)
        let event = dashboard::Event::Toplogy(dashboard::Toplogy::TryBuild(1));
        let _ = self.dashboard_tx.0.send(event);
        tokio::time::delay_for(five_seconds).await;
        // now we make use of ring:send() with built Ring.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn create_helper_from_builder() {
        let (tx, _) = mpsc::unbounded_channel::<dashboard::Event>();
        let _ = HelperBuilder::new()
            .nodes(vec!["0.0.0.0:9042".to_string()])
            .dashboard_tx(dashboard::Sender(tx))
            .build();
    }
}
