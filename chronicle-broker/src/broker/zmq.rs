use super::supervisor::{
    Peer,
    Sender as SupervisorTx,
    Topic,
};
use crate::importer;
use async_zmq::{
    errors::RecvError,
    subscribe::Subscribe,
    Message,
    Result as ZmqResult,
    StreamExt,
};
use chronicle_common::actor;
use chronicle_cql::{
    compression::MyCompression,
    frame::{
        decoder::{
            Decoder,
            Frame,
        },
        encoder::{
            ColumnEncoder,
            UNSET_VALUE,
        },
    },
};
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker::{
        self,
        Error,
    },
};
use chrono::{
    Datelike,
    NaiveDateTime,
};
use std::result::Result;
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
actor!(ZmqBuilder {
    peer: Peer,
    supervisor_tx: SupervisorTx
});

impl ZmqBuilder {
    pub fn build(self) -> Zmq {
        // create channel
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let mut pids = Vec::new();
        for _ in 0..7 {
            pids.push(Box::new(ZmqId(tx.clone())));
        }
        Zmq {
            rx,
            pids,
            pending: 0,
            peer: self.peer.unwrap(),
            supervisor_tx: self.supervisor_tx.unwrap(),
        }
    }
}

#[allow(dead_code)]
pub struct Zmq {
    rx: Receiver,
    peer: Peer,
    supervisor_tx: SupervisorTx,
    pids: Vec<Box<ZmqId>>,
    pending: usize,
}

#[derive(Debug)]
pub struct ZmqId(Sender);

pub enum Event {
    Void { pid: Box<ZmqId> },
    Error { kind: Error, pid: Box<ZmqId> },
}

impl Zmq {
    pub async fn run(mut self) {
        if let Ok(mut zmq) = self.init() {
            // start recv msgs from zmq topic according the subscribed topic
            match self.peer.get_topic() {
                // this topic used to store newly seen transactions
                Topic::Trytes => {
                    while let Some(msgs) = zmq.next().await {
                        if let Ok(msgs) = msgs {
                            for msg in msgs {
                                // process trytes msg
                                self.handle_trytes(msg);
                                // aknoweledge
                                match self.aknoweledge_responses().await {
                                    Ok(()) => {}
                                    Err(_error) => {
                                        // TODO retry/log and report to dashboard, check warnings,
                                        // TOOD impl smart strategy for internal error: No Sender/Lost which happens
                                        // when stage lose connection with
                                        // scylla node, all what we can do is to retry
                                        // a few times and if it kept failing:
                                        // - alert admin for a possiblity of a dead scylla node
                                        // - skip the transaction but make sure to log it (this log is important)
                                        // eventually the admin should fix the data layer(scylladb), and everything
                                        // should back to normal,
                                        // NOTE: solidifier job will force the data consistency (but it's not
                                        // implemented yet), still
                                        // once everything back to normal the log we just collected from the skipped
                                        // transactions should be reinserted by
                                        // the admin (possibly using importer)
                                    }
                                }
                            }
                        } else if let Err(RecvError::Interrupted) = msgs {
                            // we assume is retryable
                            continue;
                        } else {
                            unreachable!("unexepcted error: bug {:?}", msgs);
                        }
                    }
                }
                // this topic used to store confirmed transactions only
                Topic::SnTrytes => {
                    while let Some(msgs) = zmq.next().await {
                        if let Ok(msgs) = msgs {
                            for msg in msgs {
                                // process sn_trytes msg
                                self.handle_sn_trytes(&msg);
                                // aknoweledge
                                match self.aknoweledge_responses().await {
                                    Ok(()) => {}
                                    Err(_error) => {
                                        // as trytes topic
                                    }
                                }
                            }
                        } else if let Err(RecvError::Interrupted) = msgs {
                            // we assume is retryable
                            continue;
                        } else {
                            unreachable!("unexepcted error: bug {:?}", msgs);
                        }
                    }
                }
                // this topic used to upsert milestone column in transaction table (confirmed status)
                Topic::Sn => {
                    while let Some(msgs) = zmq.next().await {
                        if let Ok(msgs) = msgs {
                            // ignore if msg is sn_trytes
                            for msg in msgs {
                                if msg.as_ref()[2] == b' ' {
                                    // process sn msg
                                    // self.handle_sn(&msg);
                                    todo!();
                                }
                            }
                        } else if let Err(RecvError::Interrupted) = msgs {
                            // we assume is retryable
                            continue;
                        } else {
                            unreachable!("unexepcted error: bug {:?}", msgs);
                        }
                    }
                }
            }
        } else {
            // tell supervisor by returning peer with connected = false.
            todo!()
        }
    }

    fn init(&mut self) -> ZmqResult<Subscribe> {
        let zmq = async_zmq::subscribe(self.peer.get_address())?.connect()?;
        zmq.set_subscribe(&self.peer.get_topic_as_string())?;
        self.peer.set_connected(true);
        // todo tell supervisor that zmq worker got connected and subscribed to topic.
        Ok(zmq)
    }

    fn handle_trytes(&mut self, msg: Message) {
        self.pending += 6;
        let msg = msg.as_str().unwrap();
        let trytes = &msg[7..2680];
        let hash = &msg[2681..2762];
        self.send_insert_tx_query(hash, trytes, UNSET_VALUE);
        // extract the transaction value
        let value = importer::trytes_to_i64(&trytes[2268..2295]);
        // extract the timestamp and year, month
        let timestamp = importer::trytes_to_i64(&trytes[2322..2331]);
        let naive = NaiveDateTime::from_timestamp(timestamp, 0);
        let year = naive.year() as u16;
        let month = naive.month() as u8;
        // create queries related to the transaction value
        match value {
            0 => {
                let vertex = &trytes[2187..2268];
                let extra = importer::YearMonth::new(year, month);
                self.send_insert_edge_query(vertex, "hint", 0, "0", value, extra);
                self.send_insert_data_query(vertex, year, month, "address", timestamp, hash);
                self.pending += 1;
            }
            v if v > 0 => {
                self.send_insert_edge_query(&trytes[2187..2268], "output", timestamp, hash, value, UNSET_VALUE);
            }
            _ => {
                self.send_insert_edge_query(&trytes[2187..2268], "input", timestamp, hash, value, UNSET_VALUE);
            }
        }
        // insert queries not related to the transaction value
        self.send_insert_edge_query(&trytes[2430..2511], "trunk", timestamp, hash, value, UNSET_VALUE);
        self.send_insert_edge_query(&trytes[2511..2592], "branch", timestamp, hash, value, UNSET_VALUE);
        self.send_insert_edge_query(&trytes[2349..2430], "bundle", timestamp, hash, value, UNSET_VALUE);
        self.send_insert_data_query(&trytes[2592..2619], year, month, "tag", timestamp, hash);
    }
    fn handle_sn_trytes(&mut self, msg: &Message) {
        self.pending = 6;
        let msg = msg.as_str().unwrap();
        let trytes = &msg[10..2683];
        let hash = &msg[2684..2765];
        let milestone = msg[2766..].parse::<u64>().unwrap();
        self.send_insert_tx_query(hash, trytes, milestone);
        // extract the transaction value
        let value = importer::trytes_to_i64(&trytes[2268..2295]);
        // extract the timestamp and year, month
        let timestamp = importer::trytes_to_i64(&trytes[2322..2331]);
        let naive = NaiveDateTime::from_timestamp(timestamp, 0);
        let year = naive.year() as u16;
        let month = naive.month() as u8;
        // create queries related to the transaction value
        match value {
            0 => {
                let vertex = &trytes[2187..2268];
                let extra = importer::YearMonth::new(year, month);
                self.send_insert_edge_query(vertex, "hint", 0, "0", value, extra);
                self.send_insert_data_query(vertex, year, month, "address", timestamp, hash);
                self.pending += 1;
            }
            v if v > 0 => {
                self.send_insert_edge_query(&trytes[2187..2268], "output", timestamp, hash, value, UNSET_VALUE);
            }
            _ => {
                self.send_insert_edge_query(&trytes[2187..2268], "input", timestamp, hash, value, UNSET_VALUE);
            }
        }
        // insert queries not related to the transaction value
        self.send_insert_edge_query(&trytes[2430..2511], "trunk", timestamp, hash, value, UNSET_VALUE);
        self.send_insert_edge_query(&trytes[2511..2592], "branch", timestamp, hash, value, UNSET_VALUE);
        self.send_insert_edge_query(&trytes[2349..2430], "bundle", timestamp, hash, value, UNSET_VALUE);
        self.send_insert_data_query(&trytes[2592..2619], year, month, "tag", timestamp, hash);
    }
    #[allow(dead_code)]
    fn handle_sn(&mut self, _msg: &Message) {
        // let msg = &msg.as_str().unwrap()[..msg.len() - 328];
        // todo!
        todo!();
    }

    async fn aknoweledge_responses(&mut self) -> Result<(), worker::Error> {
        let mut r = Ok(());
        for _ in 0..self.pending {
            let event = self.rx.recv().await.unwrap();
            match event {
                Event::Void { pid } => {
                    self.pending -= 1;
                    self.pids.push(pid);
                }
                Event::Error { kind: error, pid } => {
                    self.pids.push(pid);
                    r = Err(error);
                }
            };
        }
        r
    }

    fn send_insert_tx_query(&mut self, hash: &str, tx_trytes: &str, milestone: impl ColumnEncoder) {
        let tx_query = importer::insert_to_tx_table(hash, tx_trytes, milestone);
        let request = reporter::Event::Request {
            payload: tx_query,
            worker: self.pids.pop().unwrap(),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
    }
    fn send_insert_edge_query(
        &mut self,
        vertex: &str,
        kind: &str,
        timestamp: i64,
        tx: &str,
        value: i64,
        extra: impl ColumnEncoder,
    ) {
        let edge_query = importer::insert_to_edge_table(vertex, kind, timestamp, tx, value, extra);
        let request = reporter::Event::Request {
            payload: edge_query,
            worker: self.pids.pop().unwrap(),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
    }
    fn send_insert_data_query(&mut self, vertex: &str, year: u16, month: u8, kind: &str, timestamp: i64, tx: &str) {
        let data_query = importer::insert_to_data_table(vertex, year, month, kind, timestamp, tx);
        let request = reporter::Event::Request {
            payload: data_query,
            worker: self.pids.pop().unwrap(),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
    }
}

impl worker::Worker for ZmqId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        let decoder = Decoder::new(giveload, MyCompression::get());
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event;
            if decoder.is_error() {
                let error = decoder.get_error();
                event = Event::Error {
                    kind: worker::Error::Cql(error),
                    pid,
                }
            } else {
                event = Event::Void { pid };
            }
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: worker::Error) {
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event = Event::Error { kind, pid };
            let _ = (*raw).0.send(event);
        }
    }
}
