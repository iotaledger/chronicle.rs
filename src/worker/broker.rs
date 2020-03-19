use super::{
    Status,
    StreamStatus,
    Error,
    Worker,
};
use super::preparer::try_prepare;
use crate::stage::reporter::{self, Giveload};
use tokio::sync::mpsc;

type Sender = mpsc::UnboundedSender<BrokerEvent>;

#[derive(Clone, Copy, Debug)]
pub struct QueryRef {
    query_id: usize,
    prepare_payload: &'static [u8],
    status: Status,
}

// QueryRef new
impl QueryRef {
    pub fn new(query_id: usize, prepare_payload: &'static [u8]) -> Self {
        QueryRef {
            query_id: query_id,
            prepare_payload: prepare_payload,
            status: Status::New,
        }
    }
}

#[derive(Debug)]
pub enum BrokerEvent {
    Response {
        giveload: Giveload,
        query: QueryRef,
        tx: Option<Sender>,
    },
    StreamStatus {
        stream_status: StreamStatus,
        query: QueryRef,
        tx: Option<Sender>,
    },
    Error {
        kind: Error,
        query: QueryRef,
        tx: Option<Sender>,
    },
}

#[derive(Debug)]
pub struct Broker {
    tx: Sender,
    query: QueryRef,
}

impl Broker {
    pub fn new(tx: Sender, query: QueryRef) -> Broker {
        Broker {
            tx,
            query,
        }
    }
}


impl Worker for Broker {
    fn send_streamstatus(&mut self, stream_status: StreamStatus) -> Status {
        match stream_status {
            Ok(_) => {
                let event = match self.query.status {
                    Status::New => BrokerEvent::StreamStatus {
                        stream_status,
                        query: self.query,
                        tx: None,
                    },
                    _ => BrokerEvent::StreamStatus {
                        stream_status,
                        query: self.query,
                        tx: Some(self.tx.to_owned()),
                    },
                };
                self.tx.send(event).unwrap();
            },
            Err(_) => {
                let event = BrokerEvent::StreamStatus {
                    stream_status,
                    query: self.query,
                    tx: Some(self.tx.to_owned()),
                };
                self.tx.send(event).unwrap();
            },
        }
        self.query.status.return_streamstatus()
    }

    fn send_response(&mut self, tx: &Option<reporter::Sender>, giveload: Giveload) -> Status {
        try_prepare(self.query.prepare_payload, tx, &giveload);
        let event = match self.query.status {
            Status::New => BrokerEvent::Response {
                giveload,
                query: self.query,
                tx: None,
            },
            _ => BrokerEvent::Response {
                giveload,
                query: self.query,
                tx: Some(self.tx.to_owned()),
            },
        };
        self.tx.send(event).unwrap();
        self.query.status.return_response()
    }

    fn send_error(&mut self, error: Error) -> Status {
        let event = BrokerEvent::Error {
            kind: error,
            query: self.query,
            tx: Some(self.tx.to_owned()),
        };
        self.tx.send(event).unwrap();
        self.query.status.return_error()
    }
}
