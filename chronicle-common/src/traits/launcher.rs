// TODO compute token to enable shard_awareness.
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

//! This module defines the launcher trait.

use super::{dashboard::DashboardTx, shutdown::ShutdownTx};
/// The launcher trait definition.
pub trait LauncherTx: Send + LauncherTxClone {
    /// Start an application by the launcher.
    fn start_app(&mut self, app_name: String);
    /// Shutdown an applicaiton by the launcher.
    fn shutdown_app(&mut self, app_name: String);
    /// Aknowledge the application that it is going to be shutted down.
    fn aknowledge_shutdown(&mut self, app_name: String);
    /// Register a dashboard in the launcher.
    fn register_dashboard(&mut self, dashboard_name: String, dashboard_tx: Box<dyn DashboardTx>);
    /// Register an application in the launcher.
    fn register_app(&mut self, app_name: String, shutdown_tx: Box<dyn ShutdownTx>);
    /// Update the application status in the dashboard.
    fn apps_status(&mut self, dashboard_name: String);
    /// Exit the launcher program.
    fn exit_program(&mut self);
}

impl Clone for Box<dyn LauncherTx> {
    fn clone(&self) -> Box<dyn LauncherTx> {
        self.clone_box()
    }
}

/// The trait for cloning the launcher transmission channel.
pub trait LauncherTxClone {
    /// Clone the launcher transmission channel and return the box.
    fn clone_box(&self) -> Box<dyn LauncherTx>;
}

impl<T> LauncherTxClone for T
where
    T: 'static + LauncherTx + Clone,
{
    fn clone_box(&self) -> Box<dyn LauncherTx> {
        Box::new(self.clone())
    }
}
