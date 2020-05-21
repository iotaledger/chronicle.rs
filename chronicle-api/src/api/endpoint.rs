use super::router::handle;
use chronicle_common::{
    actor,
    traits::{
        launcher::LauncherTx,
        shutdown::ShutdownTx,
    },
};
use hyper::{
    server::Server,
    service::{
        make_service_fn,
        service_fn,
    },
};
use std::{
    convert::Infallible,
    net::SocketAddr,
};
pub struct Shutdown(tokio::sync::oneshot::Sender<()>);
actor!(EndpointBuilder { listen_address: String, launcher_tx: Box<dyn LauncherTx> });

impl EndpointBuilder {
    pub fn build(self) -> Endpoint {
        let addr: SocketAddr = self.listen_address.unwrap().parse().unwrap();
        let launcher_tx: Box<dyn LauncherTx> = self.launcher_tx.unwrap();
        Endpoint { addr, launcher_tx }
    }
}

pub struct Endpoint {
    addr: SocketAddr,
    launcher_tx: Box<dyn LauncherTx>,
}
impl ShutdownTx for Shutdown {
    fn shutdown(self: Box<Self>) {
        self.0.send(()).unwrap();
    }
}
impl Endpoint {
    pub async fn run(mut self) {
        let service = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle)) });
        let server = Server::bind(&self.addr).serve(service);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // register api app with launcher
        self.launcher_tx.register_app("api".to_string(), Box::new(Shutdown(tx)));
        let graceful = server.with_graceful_shutdown(async {
            rx.await.ok();
        });
        if let Err(e) = graceful.await {
            eprintln!("error: {}, endpoint: {}", e, self.addr);
        }
        // aknowledge_shutdown
        self.launcher_tx.aknowledge_shutdown("api".to_string());
    }
}
