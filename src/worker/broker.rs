// TODO delete this mod
use super::preparer::try_prepare;
use super::{Error, Worker};
use crate::stage::reporter::{self, Giveload};
use tokio::sync::mpsc;

type Sender = mpsc::UnboundedSender<BrokerEvent>;

#[derive(Clone, Copy, Debug)]
pub struct QueryRef {
    query_id: usize,
    prepare_payload: &'static [u8],
}

// QueryRef new
impl QueryRef {
    pub fn new(query_id: usize, prepare_payload: &'static [u8]) -> Self {
        QueryRef {
            query_id: query_id,
            prepare_payload: prepare_payload,
        }
    }
}

#[derive(Debug)]
pub enum BrokerEvent {
    Response {
        giveload: Giveload,
        id: *mut Broker,
    },
    StreamStatus {
        id: Box<Broker>,
    },
    Error {
        kind: Error,
        id: *mut Broker,
    },
}

#[derive(Debug)]
pub struct Broker {
    tx: Sender,
    query: QueryRef,
}

impl Broker {
    pub fn new(tx: Sender, query: QueryRef) -> Broker {
        Broker { tx, query }
    }
}
unsafe impl Send for Broker {}
impl Worker for Broker {
    fn send_response(self: Box<Self>, tx: &Option<reporter::Sender>, giveload: Giveload) {
        //try_prepare(self.query.prepare_payload, tx, &giveload);
        let raw = Box::into_raw(self);
        let event = BrokerEvent::Response {
                giveload,
                id: raw,
            };
        unsafe {
            (*raw).tx.send(event).unwrap();
        }
    }
    fn send_error(self: Box<Self>, error: Error) {
        let raw = Box::into_raw(self);
        let event = BrokerEvent::Error {
            kind: error,
            id: raw,
        };
        unsafe {
            (*raw).tx.send(event).unwrap();
        }
    }
}
