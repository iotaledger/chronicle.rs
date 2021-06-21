// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::*;
use chronicle_common::{
    get_history_mut,
    Persist,
};

use serde::{
    Deserialize,
    Serialize,
};
use std::ops::Deref;
use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

mod core;
mod event_loop;
mod init;
mod starter;
mod terminating;
mod websocket;
mod workers;

pub trait DashboardScope: LauncherSender<DashboardBuilder<Self>> {}
impl<H: LauncherSender<DashboardBuilder<H>>> DashboardScope for H {}

/// A dashboard actor
pub struct Dashboard<H>
where
    H: DashboardScope,
{
    /// The dashboard's service
    pub service: Service,
    pub inbox: UnboundedReceiver<DashboardEvent<H::AppsEvents>>,
    pub sender: Option<DashboardSender<H>>,
}

builder!(
    #[derive(Clone)]
    DashboardBuilder<H> {}
);

impl<H: DashboardScope> Builder for DashboardBuilder<H> {
    type State = Dashboard<H>;

    fn build(self) -> Self::State {
        let (tx, inbox) = tokio::sync::mpsc::unbounded_channel();
        let sender = Some(DashboardSender { tx });
        Self::State {
            service: Service::new(),
            inbox,
            sender,
        }
        .set_name()
    }
}

impl<H: DashboardScope> Name for Dashboard<H> {
    fn set_name(mut self) -> Self {
        self.service.update_name("Dashboard".into());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

impl<H: DashboardScope> ThroughType for DashboardBuilder<H> {
    type Through = DashboardThrough;
}

pub struct DashboardSender<H: DashboardScope> {
    tx: UnboundedSender<DashboardEvent<H::AppsEvents>>,
}

impl<H: DashboardScope> Deref for DashboardSender<H> {
    type Target = UnboundedSender<DashboardEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: DashboardScope> Clone for DashboardSender<H> {
    fn clone(&self) -> Self {
        DashboardSender::<H> { tx: self.tx.clone() }
    }
}

impl<H: DashboardScope> Passthrough<DashboardThrough> for DashboardSender<H> {
    fn passthrough(&mut self, _event: DashboardThrough, _from_app_name: String) {}

    fn app_status_change(&mut self, _service: &Service) {}

    fn launcher_status_change(&mut self, _service: &Service) {}

    fn service(&mut self, _service: &Service) {}
}

pub enum DashboardEvent<T> {
    /// Passthrough type
    Passthrough(T),
}

impl<H: DashboardScope> Shutdown for DashboardSender<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        get_history_mut().persist().unwrap_or_else(|e| log::error!("{}", e));
        self.send(DashboardEvent::Passthrough(
            serde_json::from_str("{\"Dashboard\": \"Shutdown\"}").unwrap(),
        ))
        .ok();
        None
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub enum DashboardThrough {
    /// Shutdown the dashboard
    Shutdown,
}
