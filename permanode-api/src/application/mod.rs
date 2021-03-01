use super::*;
use permanode_storage::StorageConfig;
use rocket::futures::future::AbortHandle;
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
pub trait PermanodeAPIScope: LauncherSender<PermanodeAPIBuilder<Self>> {}
impl<H: LauncherSender<PermanodeAPIBuilder<H>>> PermanodeAPIScope for H {}

pub struct PermanodeAPI<H>
where
    H: PermanodeAPIScope,
{
    service: Service,
    api_config: ApiConfig,
    storage_config: StorageConfig,
    inbox: UnboundedReceiver<PermanodeAPIEvent<H::AppsEvents>>,
    sender: Option<PermanodeAPISender<H>>,
    listener: AbortHandle,
    websocket: AbortHandle,
}

pub struct PermanodeAPISender<H: PermanodeAPIScope> {
    tx: UnboundedSender<PermanodeAPIEvent<H::AppsEvents>>,
}

impl<H: PermanodeAPIScope> Deref for PermanodeAPISender<H> {
    type Target = UnboundedSender<PermanodeAPIEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: PermanodeAPIScope> Clone for PermanodeAPISender<H> {
    fn clone(&self) -> Self {
        PermanodeAPISender::<H> { tx: self.tx.clone() }
    }
}

impl<H: PermanodeAPIScope> Passthrough<PermanodeAPIThrough> for PermanodeAPISender<H> {
    fn passthrough(&mut self, event: PermanodeAPIThrough, from_app_name: String) {}

    fn app_status_change(&mut self, service: &Service) {}

    fn launcher_status_change(&mut self, service: &Service) {}

    fn service(&mut self, service: &Service) {}
}

impl<H: PermanodeAPIScope> Shutdown for PermanodeAPISender<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.send(PermanodeAPIEvent::Passthrough(
            serde_json::from_str("{\"PermanodeAPI\": \"Shutdown\"}").unwrap(),
        ))
        .ok();
        None
    }
}

builder!(
    #[derive(Clone)]
    PermanodeAPIBuilder<H> {
        api_config: ApiConfig,
        storage_config: StorageConfig,
        listener_handle: AbortHandle,
        websocket_handle: AbortHandle
    }
);

impl<H: PermanodeAPIScope> ThroughType for PermanodeAPIBuilder<H> {
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
            api_config: self.api_config.expect("No API config was provided!"),
            storage_config: self.storage_config.expect("No Storage config was provided!"),
            inbox,
            sender,
            listener: self.listener_handle.expect("No listener handle was provided!"),
            websocket: self.websocket_handle.expect("No websocket handle was provided!"),
        }
        .set_name()
    }
}

impl<H> Name for PermanodeAPI<H>
where
    H: PermanodeAPIScope,
{
    fn set_name(mut self) -> Self {
        self.service.update_name("PermanodeAPI".to_string());
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
    Websocket(Service),
}

#[derive(Deserialize, Serialize, Clone)]
pub enum PermanodeAPIThrough {
    Shutdown,
    AddNode(String),
    RemoveNode(String),
    TryBuild(u8),
}
