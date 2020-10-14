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

//! This module defines the top-level methods (build/run) of the broker.

pub mod mqtt;
pub mod supervisor;

use chronicle_common::app;
app!(BrokerBuilder { trytes: Vec<String>, conf_trytes: Vec<String>, max_retries: usize, stream_capacity: usize });

impl BrokerBuilder {
    /// Build a broker to handle events from different topics.
    pub fn build(self) -> Broker {
        let supervisor_builder = supervisor::SupervisorBuilder::new()
            .trytes(self.trytes)
            .conf_trytes(self.conf_trytes)
            .max_retries(self.max_retries.unwrap())
            .stream_capacity(self.stream_capacity.unwrap())
            .launcher_tx(self.launcher_tx.unwrap());
        Broker { supervisor_builder }
    }
}

/// The `Broker` structure contains only the builder for broker supervisor.
pub struct Broker {
    supervisor_builder: supervisor::SupervisorBuilder,
}
impl Broker {
    /// Run the built broker supervisor (i.e., start to handle received events from different topics.)
    pub async fn run(self) {
        // build and spawn supervisor
        tokio::spawn(self.supervisor_builder.build().run());
    }
}
