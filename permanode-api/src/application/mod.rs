use std::ops::Deref;

use super::*;

use permanode_storage::config::Config;
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
pub struct PermanodeAPI<H>
where
    H: LauncherSender<PermanodeAPIBuilder<H>>,
{
    service: Service,
    config: Config,
    inbox: UnboundedReceiver<PermanodeAPIEvent<H::AppsEvents>>,
    sender: Option<PermanodeAPISender<H>>,
    listener: AbortHandle,
}

pub struct PermanodeAPISender<H: LauncherSender<PermanodeAPIBuilder<H>>> {
    tx: UnboundedSender<PermanodeAPIEvent<H::AppsEvents>>,
}

impl<H: LauncherSender<PermanodeAPIBuilder<H>>> Deref for PermanodeAPISender<H> {
    type Target = UnboundedSender<PermanodeAPIEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: LauncherSender<PermanodeAPIBuilder<H>>> Clone for PermanodeAPISender<H> {
    fn clone(&self) -> Self {
        PermanodeAPISender::<H> { tx: self.tx.clone() }
    }
}

impl<H: LauncherSender<PermanodeAPIBuilder<H>>> Passthrough<PermanodeAPIThrough> for PermanodeAPISender<H> {
    fn passthrough(&mut self, event: PermanodeAPIThrough, from_app_name: String) {}

    fn app_status_change(&mut self, service: &Service) {}

    fn launcher_status_change(&mut self, service: &Service) {}

    fn service(&mut self, service: &Service) {}
}

impl<H: LauncherSender<PermanodeAPIBuilder<H>>> Shutdown for PermanodeAPISender<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.send(PermanodeAPIEvent::Passthrough(
            serde_json::from_str("{\"Permanode\": \"Shutdown\"}").unwrap(),
        ))
        .ok();
        None
    }
}

builder!(
    #[derive(Clone)]
    PermanodeAPIBuilder<H> {
        config: Config,
        listener_handle: AbortHandle
    }
);

impl<H: LauncherSender<Self>> ThroughType for PermanodeAPIBuilder<H> {
    type Through = PermanodeAPIThrough;
}

impl<H> Builder for PermanodeAPIBuilder<H>
where
    H: LauncherSender<Self>,
{
    type State = PermanodeAPI<H>;

    fn build(self) -> Self::State {
        let (tx, inbox) = tokio::sync::mpsc::unbounded_channel();
        let sender = Some(PermanodeAPISender { tx });
        Self::State {
            service: Service::new(),
            config: self.config.expect("No config was provided!"),
            inbox,
            sender,
            listener: self.listener_handle.expect("No listener handle was provided!"),
        }
        .set_name()
    }
}

impl<H> Name for PermanodeAPI<H>
where
    H: LauncherSender<PermanodeAPIBuilder<H>>,
{
    fn set_name(mut self) -> Self {
        self.service.update_name("Permanode".to_string());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

pub enum PermanodeAPIEvent<T> {
    Passthrough(T),
    Children(PermanodeAPIChild),
}

pub enum PermanodeAPIChild {
    Listener(Service),
}

#[derive(Deserialize, Serialize, Clone)]
pub enum PermanodeAPIThrough {
    Shutdown,
    AddNode(String),
    RemoveNode(String),
    TryBuild(u8),
}
