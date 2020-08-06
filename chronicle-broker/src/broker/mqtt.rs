use super::supervisor::{
    Event as SupervisorEvent,
    Peer,
    Sender as SupervisorTx,
    Topic,
};
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker::{
        Error,
        Worker,
    },
};
use log::*;
use std::time;
use tokio::{
    sync::mpsc,
    time::delay_for,
};

use crate::importer::{
    trytes::{
        Compatible,
        Trytes,
    },
    trytes_to_i64,
    valid_timestamp,
    *,
};
use chronicle_cql::{
    compression::MyCompression,
    frame::decoder::{
        Decoder,
        Frame,
    },
};
use chrono::{
    DateTime,
    Datelike,
    NaiveDateTime,
};
use paho_mqtt::{
    self,
    Message,
};
use std::{
    fmt::Write,
    time::Duration,
};
use tokio::stream::{
    Stream,
    StreamExt,
};
const TWO_DAYS_IN_MS: i64 = 172800000;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
pub struct MqttId(Sender, u8);
impl MqttId {
    fn query_id(mut self: Box<Self>, query_id: u8) -> Box<Self> {
        self.1 = query_id;
        self
    }
    fn get_query_id(&self) -> u8 {
        self.1
    }
}

pub enum Event {
    Void { pid: Box<MqttId> },
    Error { kind: Error, pid: Box<MqttId> },
}

use chronicle_common::actor;

actor!(MqttBuilder {
    id: String,
    peer: Peer,
    client: paho_mqtt::AsyncClient,
    max_retries: usize
});

impl MqttBuilder {
    pub fn build(self) -> Mqtt {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let mut pids = Vec::new();
        // create some pids in advance to enable us to send concurrent queries without the cost for heap-reallocation
        for _ in 0..11 {
            pids.push(Box::new(MqttId(tx.clone(), 0)));
        }
        let max_retries = self.max_retries.unwrap();
        Mqtt {
            rx,
            pending: 0,
            peer: self.peer.unwrap(),
            client: self.client,
            pids,
            max_retries,
            initial_max_retries: max_retries,
            delay: 0,
        }
    }
}

pub struct Mqtt {
    rx: Receiver,
    pub peer: Peer,
    pub client: Option<paho_mqtt::AsyncClient>,
    pids: Vec<Box<MqttId>>,
    pending: usize,
    initial_max_retries: usize,
    max_retries: usize,
    delay: usize,
}
impl Mqtt {
    pub async fn run(
        mut self,
        supervisor_tx: SupervisorTx,
        mut stream: impl Stream<Item = Option<Message>> + std::marker::Unpin,
    ) {
        // For now mqtt worker directly persist the Mqtt messages,
        // NOTE: later we will have collector/cache/persisting layer.
        match self.peer.get_topic() {
            Topic::Trytes => {
                while let Some(Some(msg)) = stream.next().await {
                    // parse timestamp
                    let timestamp_millis = DateTime::parse_from_rfc3339(&msg.payload_str()[(2792)..(2812)])
                        .unwrap()
                        .timestamp_millis();
                    // create MqttMsg
                    let mqtt_msg = MqttMsg::new(msg, None, timestamp_millis);
                    self.handle_trytes(mqtt_msg).await;
                }
            }
            Topic::ConfTrytes => {
                while let Some(Some(msg)) = stream.next().await {
                    let m = &msg.payload_str();
                    let l = m.len();
                    // parse timestamp
                    let timestamp_millis = DateTime::parse_from_rfc3339(&m[(l - 22)..(l - 2)])
                        .unwrap()
                        .timestamp_millis();
                    let milestone = Some(m[2789..(l - 36)].parse::<u64>().unwrap());
                    // create MqttMsg
                    let mqtt_msg = MqttMsg::new(msg, milestone, timestamp_millis);
                    self.handle_conf_trytes(mqtt_msg).await;
                }
            }
        }
        self.peer.set_connected(false);
        let _ = supervisor_tx.send(SupervisorEvent::Reconnect(self));
    }
    pub async fn init(&mut self) -> Result<impl Stream<Item = Option<Message>>, paho_mqtt::Error> {
        // create mqtt AsyncClient in advance
        let mut client_id = String::new();
        write!(&mut client_id, "chronicle_{}", self.peer.id).unwrap();
        let mut cli = paho_mqtt::AsyncClient::new((&self.peer.address[..], &client_id[..]))?;
        let stream = cli.get_stream(100);
        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .finalize();
        cli.connect(conn_opts).await?;
        cli.subscribe(self.peer.get_topic_as_string(), 1).await?;
        self.peer.set_connected(true);
        self.client.replace(cli);
        Ok(stream)
    }
    pub async fn handle_trytes(&mut self, msg: MqttMsg) {
        // we should ignore transactions with invalid timestamps
        let trytes = msg.trytes();
        let attachment_timestamp = trytes_to_i64(trytes.atch_timestamp());
        if attachment_timestamp != 0 {
            // try to use attachment_timestamp
            if valid_timestamp(attachment_timestamp, 13) {
                let time_window = (msg.timestamp_millis - attachment_timestamp).abs();
                // check if attachment_timestamp within TWO_DAYS_IN_MS time_window
                if time_window < TWO_DAYS_IN_MS {
                    // presist the transaction
                    if let Err(_) = self.persist_transaction(&msg, attachment_timestamp).await {
                        if self.pending != 11 {
                            error!(
                                "Unable to force Data consistency for the transaction: hash: {}, trytes: {}",
                                msg.hash(),
                                msg.trytes().trytes()
                            );
                        }
                    };
                }
            }
        } else {
            // use timestamp instead attachment_timestamp
            let timestamp = trytes_to_i64(trytes.timestamp());
            if valid_timestamp(timestamp, 10) {
                let timestamp_ms = timestamp * 1000;
                let time_window = (msg.timestamp_millis - timestamp_ms).abs();
                // check if attachment_timestamp within TWO_DAYS_IN_MS time_window
                if time_window < TWO_DAYS_IN_MS {
                    // presist the transaction
                    if let Err(_) = self.persist_transaction(&msg, timestamp_ms).await {
                        if self.pending != 11 {
                            error!(
                                "Unable to force Data consistency for the transaction: hash: {}, trytes: {}",
                                msg.hash(),
                                msg.trytes().trytes()
                            );
                        }
                    };
                }
            }
        }
    }
    pub async fn handle_conf_trytes(&mut self, msg: MqttMsg) {
        // we should ignore transactions with invalid timestamps
        let trytes = msg.trytes();
        let hash = msg.hash();
        let attachment_timestamp = trytes_to_i64(trytes.atch_timestamp());
        if attachment_timestamp != 0 {
            // try to use attachment_timestamp
            if valid_timestamp(attachment_timestamp, 13) {
                // no need to do time_window check, as it already exist in hornet coo chyrsalis pt-1 implementation
                // presist the transaction
                if let Err(_) = self.persist_transaction(&msg, attachment_timestamp).await {
                    if self.pending != 11 {
                        error!(
                            "Unable to force data consistency for the transaction: hash: {}, trytes: {}",
                            hash,
                            trytes.trytes()
                        );
                    }
                };
            } else {
                // this not supposed to happens in chyrsalis pt-1
                warn!(
                    "Unable to persist transaction: {} with invalid attachment_timestamp: {} confirmed by milestone {}",
                    hash,
                    attachment_timestamp,
                    msg.milestone.unwrap()
                );
            }
        } else {
            // use timestamp instead attachment_timestamp
            let timestamp = trytes_to_i64(trytes.timestamp());
            if valid_timestamp(timestamp, 10) {
                let timestamp_ms = timestamp * 1000;
                // no need to do time_window check, as it already exist in hornet coo chyrsalis pt-1 implementation
                // presist the transaction
                if let Err(_) = self.persist_transaction(&msg, timestamp_ms).await {
                    if self.pending != 11 {
                        error!(
                            "Unable to force data consistency for the transaction: hash: {}, trytes: {}",
                            hash,
                            trytes.trytes()
                        );
                    }
                };
            } else {
                // this not supposed to happens in chyrsalis pt-1
                warn!(
                    "Unable to persist transaction: {} with invalid timestamp: {} confirmed by milestone {}",
                    hash,
                    attachment_timestamp,
                    msg.milestone.unwrap()
                );
            }
        }
    }
    pub async fn persist_transaction(
        &mut self,
        msg: &MqttMsg,
        timestamp_ms: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let naive = NaiveDateTime::from_timestamp(timestamp_ms / 1000, 0);
        let year = naive.year() as u16;
        let month = naive.month() as u8;
        let trytes = msg.trytes();
        let hash = msg.hash();
        let milestone = msg.milestone;
        // ----------- transaction table query ---------------
        let payload = insert_to_tx_table(hash, &trytes, milestone);
        let request = reporter::Event::Request {
            payload,
            worker: self.pids.pop().unwrap().query_id(1),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        // extract the transaction value
        let value = trytes_to_i64(trytes.value());
        let address_kind;
        if value < 0 {
            address_kind = INPUT
        } else {
            address_kind = OUTPUT
        }
        let itr: [(&str, &str, &str, u8); 4] = [
            (trytes.trunk(), APPROVEE, TRUNK, 4),
            (trytes.branch(), APPROVEE, BRANCH, 6),
            (trytes.bundle(), BUNDLE, BUNDLE, 8),
            (trytes.tag(), TAG, TAG, 10),
        ];
        // presist by address
        let payload = insert_to_hint_table(trytes.address(), ADDRESS, year, month, milestone);
        let request = reporter::Event::Request {
            payload,
            worker: self.pids.pop().unwrap().query_id(2),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        let payload = insert_to_data_table(
            trytes.address(),
            year,
            month,
            address_kind,
            timestamp_ms,
            hash,
            value,
            milestone,
        );
        let request = reporter::Event::Request {
            payload,
            worker: self.pids.pop().unwrap().query_id(3),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        // presist remaining queries
        for (vertex, hint_kind, data_kind, query_id) in &itr {
            // create payload and then put it inside reporter request
            let payload = insert_to_hint_table(vertex, hint_kind, year, month, milestone);
            let request = reporter::Event::Request {
                payload,
                worker: self.pids.pop().unwrap().query_id(*query_id),
            };
            // send request
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            let payload = insert_to_data_table(vertex, year, month, data_kind, timestamp_ms, hash, value, milestone);
            // create another request
            let request = reporter::Event::Request {
                payload,
                worker: self.pids.pop().unwrap().query_id(query_id + 1),
            };
            // send it
            Ring::send_local_random_replica(rand::random::<i64>(), request);
        }
        self.pending += 11;
        // process the responses for the pending queries
        // process the responses for the pending queries
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Void { pid } => {
                    // decrement
                    self.pending -= 1;
                    // return pid
                    self.pids.push(pid);
                    // check if this was the last response for a given line.
                    if self.pending == 0 {
                        // reset max_retries to the initial state for the next line
                        self.max_retries = self.initial_max_retries;
                        // reset delay to 0 seconds for the next line
                        self.delay = 0;
                        break;
                    }
                }
                Event::Error { kind, pid } => {
                    // check if we consumed max_retries for given line.
                    if self.max_retries == 0 {
                        self.pids.push(pid);
                        error!(
                            "MQTT id: {}, consumed all max_retries and unable to persist transaction: {}, trytes: {}",
                            self.peer.id,
                            hash,
                            trytes.trytes()
                        );
                        return Err(Box::new(kind));
                    } else {
                        // decrement retry
                        self.max_retries -= 1;
                        // icrement the delay
                        self.delay += 1;
                        warn!(
                            "MQTT id: {}, will sleep {} seconds before retrying, because it received: {:?}",
                            self.peer.id, self.delay, kind
                        );
                        // create delay_seconds
                        let seconds = time::Duration::from_secs(self.delay as u64);
                        // sleep the importer to not push any further queries to scylla
                        delay_for(seconds).await;
                        // retry the specific query based on its query_id using send_global_random_replica strategy
                        match pid.get_query_id() {
                            1 => {
                                let payload = insert_to_tx_table(hash, &trytes, milestone);
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            2 => {
                                let payload = insert_to_hint_table(trytes.address(), ADDRESS, year, month, milestone);
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_local_random_replica(rand::random::<i64>(), request);
                            }
                            3 => {
                                let payload = insert_to_data_table(
                                    trytes.address(),
                                    year,
                                    month,
                                    address_kind,
                                    timestamp_ms,
                                    hash,
                                    value,
                                    milestone,
                                );
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            4 => {
                                let payload = insert_to_hint_table(trytes.trunk(), APPROVEE, year, month, milestone);
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            5 => {
                                let payload = insert_to_data_table(
                                    trytes.trunk(),
                                    year,
                                    month,
                                    TRUNK,
                                    timestamp_ms,
                                    hash,
                                    value,
                                    milestone,
                                );
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            6 => {
                                let payload = insert_to_hint_table(trytes.branch(), APPROVEE, year, month, milestone);
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            7 => {
                                let payload = insert_to_data_table(
                                    trytes.branch(),
                                    year,
                                    month,
                                    BRANCH,
                                    timestamp_ms,
                                    hash,
                                    value,
                                    milestone,
                                );
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            8 => {
                                let payload = insert_to_hint_table(trytes.bundle(), BUNDLE, year, month, milestone);
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            9 => {
                                let payload = insert_to_data_table(
                                    trytes.bundle(),
                                    year,
                                    month,
                                    BUNDLE,
                                    timestamp_ms,
                                    hash,
                                    value,
                                    milestone,
                                );
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            10 => {
                                let payload = insert_to_hint_table(trytes.tag(), TAG, year, month, milestone);
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            11 => {
                                let payload = insert_to_data_table(
                                    trytes.tag(),
                                    year,
                                    month,
                                    TAG,
                                    timestamp_ms,
                                    hash,
                                    value,
                                    milestone,
                                );
                                let request = reporter::Event::Request { payload, worker: pid };
                                Ring::send_global_random_replica(rand::random::<i64>(), request);
                            }
                            _ => unreachable!("invalid query_id"),
                        }
                    }
                }
            };
        }
        Ok(())
    }
}

pub struct MqttMsg {
    pub msg: Message,
    pub milestone: Option<u64>,
    pub timestamp_millis: i64,
}

impl MqttMsg {
    fn new(msg: Message, milestone: Option<u64>, timestamp_millis: i64) -> Self {
        MqttMsg {
            msg,
            milestone,
            timestamp_millis,
        }
    }
}

impl Compatible for MqttMsg {
    fn trytes(&self) -> Trytes {
        Trytes::from(self)
    }
    fn hash(&self) -> &str {
        unsafe { std::mem::transmute::<&[u8], &str>(&self.msg.payload()[11..92]) }
    }
}

impl Worker for MqttId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        let decoder = Decoder::new(giveload, MyCompression::get());
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event;
            if decoder.is_error() {
                let error = decoder.get_error();
                event = Event::Error {
                    kind: Error::Cql(error),
                    pid,
                }
            } else {
                event = Event::Void { pid };
            }
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: Error) {
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event = Event::Error { kind, pid };
            let _ = (*raw).0.send(event);
        }
    }
}
