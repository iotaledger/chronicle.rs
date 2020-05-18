use bee_ternary::{
    num_conversions,
    TryteBuf,
};
use cdrs::{
    frame::{
        Flag,
        Frame as CdrsFrame,
        IntoBytes,
        Opcode,
    },
    query,
    query::QueryFlags,
    query_values,
};
use chronicle_common::actor;
use chronicle_cql::{
    compression::UNCOMPRESSED,
    frame::decoder::{
        Decoder,
        Frame,
    },
    rows,
    statements::statements::SELECT_TX_QUERY,
};
use chronicle_storage::{
    ring::ring::Ring,
    stage::reporter,
    worker::{
        Error,
        Worker,
    },
};
use hyper::{
    Body,
    Response,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
pub struct FindTransactionsId(Sender);

actor!(FindTransactionsBuilder {
    addresses: Option<Vec<String>>,
    bundles: Option<Vec<String>>,
    approvees: Option<Vec<String>>,
    hints: Option<Vec<JsonValue>>
});

impl FindTransactionsBuilder {
    pub fn build(self) -> FindTransactions {
        FindTransactions {
            addresses: self.addresses.unwrap(),
            bundles: self.bundles.unwrap(),
            approvees: self.approvees.unwrap(),
            hints: self.hints.unwrap(),
        }
    }
}

pub struct FindTransactions {
    addresses: Option<Vec<String>>,
    bundles: Option<Vec<String>>,
    approvees: Option<Vec<String>>,
    hints: Option<Vec<JsonValue>>,
}

#[derive(Serialize)]
struct ResTransactions {
    hashes: Option<Vec<String>>,
    hints: Option<Vec<JsonValue>>,
}

impl FindTransactions {
    pub async fn run(mut self) -> Response<Body> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let mut worker = Box::new(FindTransactionsId(tx));

        // Use the worker to get the hases from each parameters
        // Use HashSet to perfrom intersection by BitAnd
        todo!()
    }

    async fn process(
        value: &mut JsonValue,
        worker: Box<FindTransactionsId>,
        rx: &mut Receiver,
    ) -> Box<FindTransactionsId> {
        todo!()
    }
    fn query_by_addresses(addresses: String) -> Vec<u8> {
        todo!()
    }
    fn query_by_bundles(bundles: String) -> Vec<u8> {
        todo!()
    }
    fn query_by_approvees(approvees: String) -> Vec<u8> {
        todo!()
    }
    fn query_by_hints(hints: HashMap<String, String>) -> Vec<u8> {
        todo!()
    }
}

pub enum Event {
    Response {
        giveload: Vec<u8>,
        pid: Box<FindTransactionsId>,
    },
    Error {
        kind: Error,
        pid: Box<FindTransactionsId>,
    },
}

// implementation!
impl Worker for FindTransactionsId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        // to enable reusable self(Sender), we will do unsafe trick
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Response { giveload, pid };
            // now we can use raw to send self through itself.
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: Error) {
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Error { kind, pid };
            // now we can use raw to send itself through itself.
            let _ = (*raw).0.send(event);
        }
    }
}
