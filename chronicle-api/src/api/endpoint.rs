use super::router::handle;
use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::service::{make_service_fn, service_fn};
use hyper::server::Server;

actor!(ServerBuilder {
    listen_address: String
});

impl ServerBuilder {
    pub fn build(self) -> Endpoint {
        let addr: SocketAddr = self.listen_address.unwrap().parse().unwrap();
        Endpoint {
            addr,
        }
    }
}

pub struct Endpoint {
    addr: SocketAddr,
}

impl Endpoint {
    pub async fn run(self) {
        let service = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(handle))
        });
        let server = Server::bind(&self.addr).serve(service);
        if let Err(e) = server.await {
            eprintln!("error: {}, endpoint: {}",e,self.addr);
        }
    }
}
