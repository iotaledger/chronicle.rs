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

//! This module defineds a dashboard trait, which defines the methods of lifecycle of an application.
//! Also an applicaiton status enum is defined for indicating which stage the application is at.

use std::collections::HashMap;
/// The dashboard trait. The transmission suffix indicates that it can be sent by the caller.
pub trait DashboardTx: Send {
    /// The application is starting.
    fn starting_app(&mut self, app_name: String);
    /// The application was started.
    fn started_app(&mut self, app_name: String);
    /// The application was restarted.
    fn restarted_app(&mut self, app_name: String);
    /// The application is going to be shutted down.
    fn shutdown_app(&mut self, app_name: String);
    /// The application status.
    fn apps_status(&mut self, apps_status: HashMap<String, AppStatus>);
}

/// The hashmap of the application status.
pub type AppsStatus = HashMap<String, AppStatus>;

#[derive(Clone)]
/// The applicaiton status enum.
pub enum AppStatus {
    /// The application is starting.
    Starting(String),
    /// The application is running.
    Running(String),
    /// The application was shutted down.
    Shutdown(String),
    /// We are shutting down the application.
    ShuttingDown(String),
    /// We are restaring the application.
    Restarting(String),
}
impl PartialEq for AppStatus {
    fn eq(&self, other: &AppStatus) -> bool {
        match (self, other) {
            (&AppStatus::Starting(_), &AppStatus::Starting(_)) => true,
            (&AppStatus::Running(_), &AppStatus::Running(_)) => true,
            (&AppStatus::Shutdown(_), &AppStatus::Shutdown(_)) => true,
            (&AppStatus::ShuttingDown(_), &AppStatus::ShuttingDown(_)) => true,
            (&AppStatus::Restarting(_), &AppStatus::Restarting(_)) => true,
            _ => false,
        }
    }
}
