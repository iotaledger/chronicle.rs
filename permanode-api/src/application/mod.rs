use std::ops::Deref;

use super::*;

use rocket::futures::future::AbortHandle;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

mod event_loop;
mod init;
mod starter;
mod terminating;
pub struct Permanode<H>
where
    H: LauncherSender<PermanodeBuilder<H>>,
{
    service: Service,
    inbox: UnboundedReceiver<PermanodeEvent<H::AppsEvents>>,
    sender: PermanodeSender<H>,
    listener: AbortHandle,
}

pub struct PermanodeSender<H: LauncherSender<PermanodeBuilder<H>>> {
    tx: UnboundedSender<PermanodeEvent<H::AppsEvents>>,
}

impl<H: LauncherSender<PermanodeBuilder<H>>> Deref for PermanodeSender<H> {
    type Target = UnboundedSender<PermanodeEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: LauncherSender<PermanodeBuilder<H>>> Clone for PermanodeSender<H> {
    fn clone(&self) -> Self {
        PermanodeSender::<H> { tx: self.tx.clone() }
    }
}

impl<H: LauncherSender<PermanodeBuilder<H>>> Passthrough<PermanodeThrough> for PermanodeSender<H> {
    fn passthrough(&mut self, event: PermanodeThrough, from_app_name: String) {}

    fn app_status_change(&mut self, service: &Service) {}

    fn launcher_status_change(&mut self, service: &Service) {}

    fn service(&mut self, service: &Service) {}
}

impl<H: LauncherSender<PermanodeBuilder<H>>> Shutdown for PermanodeSender<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.send(PermanodeEvent::Passthrough(
            serde_json::from_str("{\"Permanode\": \"Shutdown\"}").unwrap(),
        ))
        .ok();
        None
    }
}

builder!(
    #[derive(Clone)]
    PermanodeBuilder<H> {
        listener_handle: AbortHandle
    }
);

impl<H: LauncherSender<Self>> ThroughType for PermanodeBuilder<H> {
    type Through = PermanodeThrough;
}

impl<H> Builder for PermanodeBuilder<H>
where
    H: LauncherSender<Self>,
{
    type State = Permanode<H>;

    fn build(self) -> Self::State {
        let (tx, inbox) = tokio::sync::mpsc::unbounded_channel();
        let sender = PermanodeSender { tx };
        Self::State {
            service: Service::new(),
            inbox,
            sender,
            listener: self.listener_handle.expect("No listener handle was provided!"),
        }
        .set_name()
    }
}

impl<H> Name for Permanode<H>
where
    H: LauncherSender<PermanodeBuilder<H>>,
{
    fn set_name(mut self) -> Self {
        self.service.update_name("Permanode".to_string());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

pub enum PermanodeEvent<T> {
    Passthrough(T),
    Children(PermanodeChild),
}

pub enum PermanodeChild {
    Listener(Service),
}

#[derive(Deserialize, Serialize, Clone)]
pub enum PermanodeThrough {
    Shutdown,
    AddNode(String),
    RemoveNode(String),
    TryBuild(u8),
}
