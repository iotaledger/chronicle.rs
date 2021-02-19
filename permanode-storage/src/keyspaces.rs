pub use scylla::access::keyspace::Keyspace;
use scylla::{
    ring::Ring,
    stage::ReporterEvent,
};

#[derive(Default)]
pub struct Mainnet;

impl Keyspace for Mainnet {
    const NAME: &'static str = "mainnet";

    fn send_local(&self, token: i64, payload: Vec<u8>, worker: Box<dyn scylla::Worker>) {
        let request = ReporterEvent::Request { worker, payload };

        Ring::send_local_random_replica(token, request);
    }

    fn send_global(&self, token: i64, payload: Vec<u8>, worker: Box<dyn scylla::Worker>) {
        let request = ReporterEvent::Request { worker, payload };

        Ring::send_global_random_replica(token, request);
    }
}
