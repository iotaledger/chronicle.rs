// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use async_trait::async_trait;
use backstage::*;
use chronicle_common::{
    get_config_async,
    get_history_mut,
    get_history_mut_async,
    Persist,
};
use scylla_rs::prelude::ScyllaThrough;
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

pub trait WebsocketScope: LauncherSender<WebsocketBuilder<Self>> {}
impl<H: LauncherSender<WebsocketBuilder<H>>> WebsocketScope for H {}

/// A websocket actor
pub struct Websocket<H>
where
    H: WebsocketScope,
{
    /// The websocket's service
    pub service: Service,
    pub inbox: UnboundedReceiver<WebsocketEvent<H::AppsEvents>>,
    pub sender: Option<WebsocketSender<H>>,
}

builder!(
    #[derive(Clone)]
    WebsocketBuilder<H> {}
);

impl<H: WebsocketScope> Builder for WebsocketBuilder<H> {
    type State = Websocket<H>;

    fn build(self) -> Self::State {
        let (tx, inbox) = tokio::sync::mpsc::unbounded_channel();
        let sender = Some(WebsocketSender { tx });
        Self::State {
            service: Service::new(),
            inbox,
            sender,
        }
        .set_name()
    }
}

impl<H: WebsocketScope> Name for Websocket<H> {
    fn set_name(mut self) -> Self {
        self.service.update_name("Websocket".into());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

impl<H: WebsocketScope> ThroughType for WebsocketBuilder<H> {
    type Through = WebsocketThrough;
}

pub struct WebsocketSender<H: WebsocketScope> {
    tx: UnboundedSender<WebsocketEvent<H::AppsEvents>>,
}

impl<H: WebsocketScope> Deref for WebsocketSender<H> {
    type Target = UnboundedSender<WebsocketEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: WebsocketScope> Clone for WebsocketSender<H> {
    fn clone(&self) -> Self {
        WebsocketSender::<H> { tx: self.tx.clone() }
    }
}

impl<H: WebsocketScope> Passthrough<WebsocketThrough> for WebsocketSender<H> {
    fn passthrough(&mut self, _event: WebsocketThrough, _from_app_name: String) {}

    fn app_status_change(&mut self, _service: &Service) {}

    fn launcher_status_change(&mut self, _service: &Service) {}

    fn service(&mut self, _service: &Service) {}
}

pub enum WebsocketEvent<T> {
    /// Passthrough type
    Passthrough(T),
}

impl<H: WebsocketScope> Shutdown for WebsocketSender<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        get_history_mut().persist().unwrap_or_else(|e| log::error!("{}", e));
        self.send(WebsocketEvent::Passthrough(
            serde_json::from_str("{\"Websocket\": \"Shutdown\"}").unwrap(),
        ))
        .ok();
        None
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub enum WebsocketThrough {
    /// Shutdown the websocket
    Shutdown,
}
