// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use rocket::Shutdown as RocketShutdown;
use serde::{
    Deserialize,
    Serialize,
};
use std::ops::Deref;
use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

mod event_loop;
mod init;
mod starter;
mod terminating;

/// Define the application scope trait
pub trait ChronicleAPIScope: LauncherSender<ChronicleAPIBuilder<Self>> {}
impl<H: LauncherSender<ChronicleAPIBuilder<H>>> ChronicleAPIScope for H {}

/// The Chronicle API. Defines endpoints which can be used to
/// retrieve data from the scylla database.
pub struct ChronicleAPI<H>
where
    H: ChronicleAPIScope,
{
    service: Service,
    inbox: UnboundedReceiver<ChronicleAPIEvent<H::AppsEvents>>,
    sender: Option<ChronicleAPISender<H>>,
    rocket_listener: Option<RocketShutdown>,
    // websocket: AbortHandle,
}

/// A wrapper type for the sender end of the Chronicle API event channel
pub struct ChronicleAPISender<H: ChronicleAPIScope> {
    tx: UnboundedSender<ChronicleAPIEvent<H::AppsEvents>>,
}

impl<H: ChronicleAPIScope> Deref for ChronicleAPISender<H> {
    type Target = UnboundedSender<ChronicleAPIEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: ChronicleAPIScope> Clone for ChronicleAPISender<H> {
    fn clone(&self) -> Self {
        ChronicleAPISender::<H> { tx: self.tx.clone() }
    }
}

impl<H: ChronicleAPIScope> Passthrough<ChronicleAPIThrough> for ChronicleAPISender<H> {
    fn passthrough(&mut self, _event: ChronicleAPIThrough, _from_app_name: String) {}

    fn app_status_change(&mut self, _service: &Service) {}

    fn launcher_status_change(&mut self, _service: &Service) {}

    fn service(&mut self, _service: &Service) {}
}

impl<H: ChronicleAPIScope> Shutdown for ChronicleAPISender<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.send(ChronicleAPIEvent::Passthrough(
            serde_json::from_str("{\"ChronicleAPI\": \"Shutdown\"}").unwrap(),
        ))
        .ok();
        None
    }
}

builder!(
    #[derive(Clone)]
    ChronicleAPIBuilder<H> {
        rocket_listener_handle: RocketShutdown
    }
);

impl<H: ChronicleAPIScope> ThroughType for ChronicleAPIBuilder<H> {
    type Through = ChronicleAPIThrough;
}

impl<H> Builder for ChronicleAPIBuilder<H>
where
    H: LauncherSender<Self>,
{
    type State = ChronicleAPI<H>;

    fn build(self) -> Self::State {
        let (tx, inbox) = tokio::sync::mpsc::unbounded_channel();
        let sender = Some(ChronicleAPISender { tx });
        if self.rocket_listener_handle.is_none() {
            panic!("No listener handle was provided!");
        }
        Self::State {
            service: Service::new(),
            inbox,
            sender,
            rocket_listener: self.rocket_listener_handle,
            // websocket: self.websocket_handle.expect("No websocket handle was provided!"),
        }
        .set_name()
    }
}

impl<H> Name for ChronicleAPI<H>
where
    H: ChronicleAPIScope,
{
    fn set_name(mut self) -> Self {
        self.service.update_name("ChronicleAPI".to_string());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

/// A Chronicle API Event
pub enum ChronicleAPIEvent<T> {
    /// Passthrough type
    Passthrough(T),
    /// Event which targets a specific child app
    Children(ChronicleAPIChild),
}

/// Chronicle API children apps
pub enum ChronicleAPIChild {
    /// The listener, which defines http endpoints
    Listener(Service),
    /// The websocket, which provides topics for the dashboard
    Websocket(Service),
}

/// Chronicle API throughtype
#[derive(Deserialize, Serialize, Clone)]
pub enum ChronicleAPIThrough {
    /// Shutdown the API
    Shutdown,
}
