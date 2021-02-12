pub use super::*;
pub use chronicle::*;
use scylla::application::{
    ScyllaBuilder,
    ScyllaHandle,
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
    pub service: Service,
    pub inbox: UnboundedReceiver<PermanodeEvent<H::AppsEvents>>,
    pub sender: PermanodeSender<H>,
    pub scylla_handle: Option<ScyllaHandle<Sender>>,
}

pub struct PermanodeSender<H: LauncherSender<PermanodeBuilder<H>>> {
    tx: UnboundedSender<PermanodeEvent<H::AppsEvents>>,
}

impl<H: LauncherSender<PermanodeBuilder<H>>> Clone for PermanodeSender<H> {
    fn clone(&self) -> Self {
        PermanodeSender::<H> { tx: self.tx.clone() }
    }
}

impl<H: LauncherSender<PermanodeBuilder<H>>> Passthrough<PermanodeThrough> for PermanodeSender<H> {
    fn passthrough(&mut self, event: PermanodeThrough, from_app_name: String) {
        todo!()
    }

    fn app_status_change(&mut self, service: &Service) {
        todo!()
    }

    fn launcher_status_change(&mut self, service: &Service) {
        todo!()
    }

    fn service(&mut self, service: &Service) {
        todo!()
    }
}

impl<H: LauncherSender<PermanodeBuilder<H>>> Shutdown for PermanodeSender<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}

builder!(
    #[derive(Clone)]
    PermanodeBuilder<H> {
        listen_address: String
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
        todo!()
    }
}

impl<H> Name for Permanode<H>
where
    H: LauncherSender<PermanodeBuilder<H>>,
{
    fn set_name(self) -> Self {
        todo!()
    }

    fn get_name(&self) -> String {
        todo!()
    }
}

pub enum PermanodeEvent<T> {
    Passthrough(T),
}

#[derive(Deserialize, Serialize, Clone)]
pub enum PermanodeThrough {
    Shutdown,
    AddNode(String),
    RemoveNode(String),
    TryBuild(u8),
}

impl Clone for AppsEvents {
    fn clone(&self) -> Self {
        match self {
            AppsEvents::Permanode(t) => AppsEvents::Permanode(t.clone()),
            AppsEvents::Scylla(t) => AppsEvents::Scylla(t.clone()),
        }
    }
}

launcher!(builder: AppsBuilder {[] -> Permanode: PermanodeBuilder<Sender>, [Permanode] -> Scylla: ScyllaBuilder<Sender>}, state: Apps {});

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        todo!()
    }
}
