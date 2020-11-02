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

//! The ScyllaDB stage-level module.
//! Stage in summary is an isolated connection state for a given scylla-shard with corresponding(reporter(s),
//! sender, receiver) while stage-supervisor is the one who handles restarting/shutingdown the whole stage or just sender
//! & receiver in case they lost connection with the socket now in order to start stage you should spawn a stage-supervisor
//! who is going to do so and make sure the stage always with active connection and correct state .
//! Node-supervisor is the one who spawns stage-supervisor per node-core(shard), and also can force it to shutdown if needed
//! Hierarchy: Cluster -> Node -> Stage

mod receiver;
pub mod reporter;
mod sender;
pub mod supervisor;
