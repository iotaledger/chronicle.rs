use super::*;
use futures::future::AbortHandle;
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
pub trait PermanodeAPIScope: LauncherSender<PermanodeAPIBuilder<Self>> {}
impl<H: LauncherSender<PermanodeAPIBuilder<H>>> PermanodeAPIScope for H {}

/// The Permanode API. Defines endpoints which can be used to
/// retrieve data from the scylla database.
pub struct PermanodeAPI<H>
where
    H: PermanodeAPIScope,
{
    service: Service,
    api_config: ApiConfig,
    inbox: UnboundedReceiver<PermanodeAPIEvent<H::AppsEvents>>,
    sender: Option<PermanodeAPISender<H>>,
    rocket_listener: Option<RocketShutdown>,
    warp_listener: Option<AbortHandle>,
    websocket: AbortHandle,
}

/// A wrapper type for the sender end of the Permanode API event channel
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
        rocket_listener_handle: RocketShutdown,
        warp_listener_handle: AbortHandle,
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
        if self.rocket_listener_handle.is_none() && self.warp_listener_handle.is_none() {
            panic!("No listener handle was provided!");
        }
        Self::State {
            service: Service::new(),
            api_config: self.api_config.expect("No API config was provided!"),
            inbox,
            sender,
            rocket_listener: self.rocket_listener_handle,
            warp_listener: self.warp_listener_handle,
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

/// A Permanode API Event
pub enum PermanodeAPIEvent<T> {
    /// Passthrough type
    Passthrough(T),
    /// Event which targets a specific child app
    Children(PermanodeAPIChild),
}

/// Permanode API children apps
pub enum PermanodeAPIChild {
    /// The listener, which defines http endpoints
    Listener(Service),
    /// The websocket, which provides topics for the dashboard
    Websocket(Service),
}

/// Permanode API throughtype
#[derive(Deserialize, Serialize, Clone)]
pub enum PermanodeAPIThrough {
    Shutdown,
    AddNode(String),
    RemoveNode(String),
    TryBuild(u8),
}
