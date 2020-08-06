// TODO compute token to enable shard_awareness.
pub mod trytes;
use bee_ternary::TryteBuf;
use chronicle_common::actor;
use chronicle_cql::frame::encoder::ColumnEncoder;
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker,
};
use chrono::{
    Datelike,
    NaiveDateTime,
};
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use std::{
    convert::TryFrom,
    error::Error,
    time,
};
use tokio::{
    fs::File,
    sync::mpsc,
    time::delay_for,
};
use trytes::Trytes;

use log::*;

use chronicle_cql::{
    compression::MyCompression,
    frame::{
        consistency::Consistency,
        decoder::{
            Decoder,
            Frame,
        },
        header::Header,
        query::Query,
        queryflags::{
            SKIP_METADATA,
            VALUES,
        },
    },
};

use tokio::io::{
    AsyncBufReadExt,
    BufReader,
};

type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
struct Milestone(u64);
impl Milestone {
    pub fn new(index: u64) -> Self {
        Milestone(index)
    }
}
impl chronicle_cql::frame::encoder::ColumnEncoder for Milestone {
    fn encode(&self, buffer: &mut Vec<u8>) {
        if self.0 != 0 {
            u64::encode(&self.0, buffer);
        } else {
            chronicle_cql::frame::encoder::UNSET_VALUE.encode(buffer);
        }
    }
}

#[derive(Debug)]
pub struct ImporterId(Sender, u8);
impl ImporterId {
    fn query_id(mut self: Box<Self>, query_id: u8) -> Box<Self> {
        self.1 = query_id;
        self
    }
    fn get_query_id(&self) -> u8 {
        self.1
    }
}
actor!(ImporterBuilder {
    filepath: String,
    milestone: u64,
    only_confirmed: bool,
    max_retries: usize
});

pub enum Event {
    Response { decoder: Decoder, pid: Box<ImporterId> },
    Error { kind: worker::Error, pid: Box<ImporterId> },
}

impl ImporterBuilder {
    pub fn build(self) -> Importer {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let mut pids = Vec::new();
        // create some pids in advance to enable us to send concurrent queries without the cost for heap-reallocation
        for _ in 0..11 {
            pids.push(Box::new(ImporterId(tx.clone(), 0)));
        }
        let max_retries = self.max_retries.unwrap();
        Importer {
            rx,
            filepath: self.filepath.unwrap(),
            processed_bytes: 0,
            milestone: self.milestone.unwrap(),
            only_confirmed: self.only_confirmed.unwrap(),
            pids,
            progress_bar: None,
            pending: 0,
            initial_max_retries: max_retries,
            max_retries,
            delay: 0,
        }
    }
}

pub struct Importer {
    rx: Receiver,
    filepath: String,
    processed_bytes: u64,
    milestone: u64,
    only_confirmed: bool,
    pids: Vec<Box<ImporterId>>,
    progress_bar: Option<ProgressBar>,
    pending: usize,
    initial_max_retries: usize,
    max_retries: usize,
    delay: usize,
}

impl Importer {
    pub async fn run(mut self) -> Result<(), Box<dyn Error>> {
        // open dmp file
        let mut file = File::open(&self.filepath).await?;
        // get the metadata
        let metadata = file.metadata().await?;
        // get the length of the dmp file
        let len = metadata.len();
        // create progress bar
        let pb = ProgressBar::new(len);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) \n {msg}",
                )
                .progress_chars("#>-"),
        );
        self.progress_bar.replace(pb);
        // create buffer reader to enable us to read line by line
        let reader = BufReader::new(&mut file);
        self.handle_dmp(reader).await
    }
    async fn handle_dmp(&mut self, mut reader: BufReader<&mut File>) -> Result<(), Box<dyn Error>> {
        let try_attachment_timestamp = if self.milestone > 337_541 { true } else { false };
        let mut line = String::new();
        // start processing the file line by line
        loop {
            // clear the line buffer.
            line.clear();
            // read the line
            let line_length = reader.read_line(&mut line).await?;
            // break if EOF
            if line_length == 0 {
                break;
            }
            let hash = &line[..81];
            let trytes = Trytes::new(&line[82..2755]);
            // check if milestone is in the line
            if line_length > 2756 {
                self.milestone = line[2756..(line_length - 1)].parse::<u64>().unwrap();
            }
            // check whether to skip the transaction(line) if only_confirmed or not.
            if self.only_confirmed && self.milestone == 0 {
                // update the progresss bar
                self.processed_bytes += line_length as u64;
                self.progress_bar.as_ref().unwrap().set_position(self.processed_bytes);
                continue;
            }
            // timestamp in ms
            let timestamp_ms;
            // select obsolete_tag if try_attachment_timestamp is_false;
            let tag;
            // confirm the timestamp in seconds and attachment_timestamp(if any) is in ms
            if try_attachment_timestamp {
                tag = trytes.tag();
                let attachment_timestamp = trytes_to_i64(trytes.atch_timestamp());
                if attachment_timestamp != 0 {
                    // try to use attachment_timestamp
                    if valid_timestamp(attachment_timestamp, 13) {
                        timestamp_ms = attachment_timestamp;
                    } else {
                        if self.milestone > 0 {
                            warn!("Unable to import transaction: {} with invalid attachment_timestamp: {} confirmed by milestone: {}",hash, attachment_timestamp,self.milestone);
                        }
                        // invalid attachment_timestamp therefore for unconfirmed transaction, so we skip it
                        // update the progresss bar
                        self.processed_bytes += line_length as u64;
                        self.progress_bar.as_ref().unwrap().set_position(self.processed_bytes);
                        continue;
                    }
                } else {
                    // use timestamp instead attachment_timestamp
                    let timestamp = trytes_to_i64(trytes.timestamp());
                    if valid_timestamp(timestamp, 10) {
                        timestamp_ms = timestamp * 1000;
                    } else {
                        if self.milestone > 0 {
                            warn!("Unable to import transaction: {} with invalid timestamp: {} confirmed by milestone: {}",hash, timestamp,self.milestone);
                        }
                        // invalid timestamp for unconfirmed transaction
                        // update the progresss bar
                        self.processed_bytes += line_length as u64;
                        self.progress_bar.as_ref().unwrap().set_position(self.processed_bytes);
                        continue;
                    }
                }
            } else {
                // use obsolete_tag
                tag = trytes.obsolete_tag();
                // use timestamp instead attachment_timestamp
                let timestamp = trytes_to_i64(trytes.timestamp());
                if valid_timestamp(timestamp, 10) {
                    timestamp_ms = timestamp * 1000;
                } else {
                    if self.milestone > 0 {
                        warn!(
                            "Unable to import transaction: {} with invalid timestamp: {} confirmed by milestone: {}",
                            hash, timestamp, self.milestone
                        );
                    }
                    // invalid timestamp
                    // update the progresss bar
                    self.processed_bytes += line_length as u64;
                    self.progress_bar.as_ref().unwrap().set_position(self.processed_bytes);
                    continue;
                }
            }
            let naive = NaiveDateTime::from_timestamp(timestamp_ms / 1000, 0);
            let year = naive.year() as u16;
            let month = naive.month() as u8;
            // ----------- transaction table query ---------------
            let payload = insert_to_tx_table(hash, &trytes, Milestone::new(self.milestone));
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
                (tag, TAG, TAG, 10),
            ];
            // presist by address
            let payload = insert_to_hint_table(trytes.address(), ADDRESS, year, month, Milestone::new(self.milestone));
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
                Milestone::new(self.milestone),
            );
            let request = reporter::Event::Request {
                payload,
                worker: self.pids.pop().unwrap().query_id(3),
            };
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            // presist remaining queries
            for (vertex, hint_kind, data_kind, query_id) in &itr {
                // create payload and then put it inside reporter request
                let payload = insert_to_hint_table(vertex, hint_kind, year, month, Milestone::new(self.milestone));
                let request = reporter::Event::Request {
                    payload,
                    worker: self.pids.pop().unwrap().query_id(*query_id),
                };
                // send request
                Ring::send_local_random_replica(rand::random::<i64>(), request);
                let payload = insert_to_data_table(
                    vertex,
                    year,
                    month,
                    data_kind,
                    timestamp_ms,
                    hash,
                    value,
                    Milestone::new(self.milestone),
                );
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
            while let Some(event) = self.rx.recv().await {
                match event {
                    Event::Response { decoder, pid } => {
                        // decrement
                        self.pending -= 1;
                        // return pid
                        self.pids.push(pid);
                        // safety check
                        assert!(decoder.is_void());
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
                            error!("Importer consumed all max_retries and unable to import the dump file");
                            return Err(Box::new(kind));
                        } else {
                            // decrement retry
                            self.max_retries -= 1;
                            // icrement the delay
                            self.delay += 1;
                            warn!(
                                "Importer will sleep {} seconds before retrying, because it received: {}",
                                self.delay, kind
                            );
                            // create delay_seconds
                            let seconds = time::Duration::from_secs(self.delay as u64);
                            // sleep the importer to not push any further queries to scylla
                            delay_for(seconds).await;
                            // retry the specific query based on its query_id using send_global_random_replica strategy
                            match pid.get_query_id() {
                                1 => {
                                    let payload = insert_to_tx_table(hash, &trytes, Milestone::new(self.milestone));
                                    let request = reporter::Event::Request { payload, worker: pid };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                2 => {
                                    let payload = insert_to_hint_table(
                                        trytes.address(),
                                        ADDRESS,
                                        year,
                                        month,
                                        Milestone::new(self.milestone),
                                    );
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
                                        Milestone::new(self.milestone),
                                    );
                                    let request = reporter::Event::Request { payload, worker: pid };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                4 => {
                                    let payload = insert_to_hint_table(
                                        trytes.trunk(),
                                        APPROVEE,
                                        year,
                                        month,
                                        Milestone::new(self.milestone),
                                    );
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
                                        Milestone::new(self.milestone),
                                    );
                                    let request = reporter::Event::Request { payload, worker: pid };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                6 => {
                                    let payload = insert_to_hint_table(
                                        trytes.branch(),
                                        APPROVEE,
                                        year,
                                        month,
                                        Milestone::new(self.milestone),
                                    );
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
                                        Milestone::new(self.milestone),
                                    );
                                    let request = reporter::Event::Request { payload, worker: pid };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                8 => {
                                    let payload = insert_to_hint_table(
                                        trytes.bundle(),
                                        BUNDLE,
                                        year,
                                        month,
                                        Milestone::new(self.milestone),
                                    );
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
                                        Milestone::new(self.milestone),
                                    );
                                    let request = reporter::Event::Request { payload, worker: pid };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                10 => {
                                    let payload =
                                        insert_to_hint_table(tag, TAG, year, month, Milestone::new(self.milestone));
                                    let request = reporter::Event::Request { payload, worker: pid };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                11 => {
                                    let payload = insert_to_data_table(
                                        tag,
                                        year,
                                        month,
                                        TAG,
                                        timestamp_ms,
                                        hash,
                                        value,
                                        Milestone::new(self.milestone),
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
            // update the progresss bar
            self.processed_bytes += line_length as u64;
            self.progress_bar.as_ref().unwrap().set_position(self.processed_bytes);
        }
        self.progress_bar
            .as_ref()
            .unwrap()
            .finish_with_message(&format!("{} is processed succesfully.", self.filepath));
        Ok(())
    }
}

impl worker::Worker for ImporterId {
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
                event = Event::Response { decoder, pid };
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

/// Create insert cql query in transaction table
pub fn insert_to_tx_table(hash: &str, trytes: &Trytes, milestone: impl ColumnEncoder) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(INSERT_TANGLE_TX_QUERY)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(17) // the total value count
        .value(hash)
        .value(trytes.payload()) // PAYLOAD
        .value(trytes.address()) // ADDRESS
        .value(trytes.value()) // VALUE
        .value(trytes.obsolete_tag()) // OBSOLETE_TAG
        .value(trytes.timestamp()) // TIMESTAMP
        .value(trytes.current_index()) // CURRENT_IDX
        .value(trytes.last_index()) // LAST_IDX
        .value(trytes.bundle()) // BUNDLE_HASH
        .value(trytes.trunk()) // TRUNK
        .value(trytes.branch()) // BRANCH
        .value(trytes.tag()) // TAG
        .value(trytes.atch_timestamp()) // ATCH_TIMESTAMP
        .value(trytes.atch_timestamp_lower()) // ATCH_TIMESTAMP_LOWER
        .value(trytes.atch_timestamp_upper()) // ATCH_TIMESTAMP_UPPER
        .value(trytes.nonce()) // Nonce
        .value(milestone) // milestone
        .build(MyCompression::get());
    payload
}

/// Create insert(index) cql query in hint table
pub fn insert_to_hint_table(vertex: &str, kind: &str, year: u16, month: u8, milestone: impl ColumnEncoder) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(INSERT_TANGLE_HINT_STATMENT)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(5) // the total value count
        .value(vertex) // vertex
        .value(kind) // kind
        .value(year) // year
        .value(month) // month
        .value(milestone) // milestone
        .build(MyCompression::get());
    payload
}

/// Create insert(index) cql query in data table
pub fn insert_to_data_table(
    vertex: &str,
    year: u16,
    month: u8,
    kind: &str,
    timestamp: i64,
    tx: &str,
    value: i64,
    milestone: impl ColumnEncoder,
) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(INSERT_TANGLE_DATA_STATMENT)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(8) // the total value count
        .value(vertex) // vertex
        .value(year)
        .value(month)
        .value(kind) // kind
        .value(timestamp) // timestamp
        .value(tx) // hash
        .value(value) // value
        .value(milestone) // milestone | unset
        .build(MyCompression::get());
    payload
}

/// Convert valid trytes to i64
pub fn trytes_to_i64(slice: &str) -> i64 {
    i64::try_from(TryteBuf::try_from_str(slice).unwrap().as_trits()).unwrap()
}

pub fn count_digit(mut timestamp: i64) -> usize {
    let mut count = 0;
    while timestamp != 0 {
        count += 1;
        timestamp /= 10;
    }
    count
}

pub fn valid_timestamp(timestamp: i64, digit_count: usize) -> bool {
    if timestamp > 0 && count_digit(timestamp) == digit_count {
        // valid
        return true;
    } else {
        return false;
    }
}

// kind consts
pub const ADDRESS: &str = "address";
pub const INPUT: &str = "input";
pub const OUTPUT: &str = "output";
pub const APPROVEE: &str = "approvee";
pub const TRUNK: &str = "trunk";
pub const BRANCH: &str = "branch";
pub const BUNDLE: &str = "bundle";
pub const TAG: &str = "tag";

// statements consts
pub const INSERT_TANGLE_TX_QUERY: &str = {
    #[cfg(feature = "mainnet")]
    let cql = r#"
  INSERT INTO mainnet.transaction (
    hash,
    payload,
    address,
    value,
    obsolete_tag,
    timestamp,
    current_index,
    last_index,
    bundle,
    trunk,
    branch,
    tag,
    attachment_timestamp,
    attachment_timestamp_lower,
    attachment_timestamp_upper,
    nonce,
    milestone
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
"#;
    #[cfg(feature = "devnet")]
    let cql = r#"
  INSERT INTO devnet.transaction (
    hash,
    payload,
    address,
    value,
    obsolete_tag,
    timestamp,
    current_index,
    last_index,
    bundle,
    trunk,
    branch,
    tag,
    attachment_timestamp,
    attachment_timestamp_lower,
    attachment_timestamp_upper,
    nonce,
    milestone
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
"#;
    #[cfg(feature = "comnet")]
    let cql = r#"
  INSERT INTO comnet.transaction (
    hash,
    payload,
    address,
    value,
    obsolete_tag,
    timestamp,
    current_index,
    last_index,
    bundle,
    trunk,
    branch,
    tag,
    attachment_timestamp,
    attachment_timestamp_lower,
    attachment_timestamp_upper,
    nonce,
    milestone
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
"#;
    cql
};
//-------------------------------------
pub const INSERT_TANGLE_HINT_STATMENT: &str = {
    #[cfg(feature = "mainnet")]
    let cql = r#"
  INSERT INTO mainnet.hint (
    vertex,
    kind,
    year,
    month,
    milestone
) VALUES (?,?,?,?,?);
"#;
    #[cfg(feature = "devnet")]
    let cql = r#"
INSERT INTO devnet.hint (
vertex,
kind,
year,
month,
milestone
) VALUES (?,?,?,?,?);
"#;
    #[cfg(feature = "comnet")]
    let cql = r#"
INSERT INTO comnet.hint (
vertex,
kind,
year,
month,
milestone
) VALUES (?,?,?,?,?);
"#;
    cql
};
//-------------------------------------
pub const INSERT_TANGLE_DATA_STATMENT: &str = {
    #[cfg(feature = "mainnet")]
    let cql = r#"
  INSERT INTO mainnet.data (
    vertex,
    year,
    month,
    kind,
    timestamp,
    tx,
    value,
    milestone
) VALUES (?,?,?,?,?,?,?,?);
"#;
    #[cfg(feature = "devnet")]
    let cql = r#"
INSERT INTO devnet.data (
vertex,
year,
month,
kind,
timestamp,
tx,
value,
milestone
) VALUES (?,?,?,?,?,?,?,?);
"#;
    #[cfg(feature = "comnet")]
    let cql = r#"
INSERT INTO comnet.data (
vertex,
year,
month,
kind,
timestamp,
tx,
value,
milestone
) VALUES (?,?,?,?,?,?,?,?);
"#;
    cql
};
