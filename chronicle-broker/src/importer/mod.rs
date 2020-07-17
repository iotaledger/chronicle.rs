// TODO compute token to enable shard_awareness.
use bee_ternary::{
    t1b1::T1B1Buf,
    TritBuf,
    TryteBuf,
};
use chronicle_common::actor;
use chronicle_cql::frame::encoder::{
    ColumnEncoder,
    UNSET_VALUE,
};
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
};

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
const BE_3_BYTES_LENGTH: [u8; 4] = [0, 0, 0, 3];
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
pub struct YearMonth(u16, u8);
impl YearMonth {
    pub fn new(year: u16, month: u8) -> Self {
        YearMonth(year, month)
    }
}
impl chronicle_cql::frame::encoder::ColumnEncoder for YearMonth {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_3_BYTES_LENGTH);
        buffer.extend(&u16::to_be_bytes(self.0));
        buffer.push(self.1);
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
        // create 7 pids in advance to enable us to send 6 concurrent queries without the cost for heap-reallocation
        for _ in 0..7 {
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
        let mut try_attachment_timestamp = false;
        if self.milestone > 337541 {
            try_attachment_timestamp = true;
        }
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
            let txtrytes = &line[82..2755];
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
            self.pending += 6; // 1 tx_query + 5 edge_table queries
            let tx_query = insert_to_tx_table(hash, txtrytes, self.milestone);
            let request = reporter::Event::Request {
                payload: tx_query,
                worker: self.pids.pop().unwrap().query_id(1),
            };
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            // extract the transaction value
            let value = trytes_to_i64(&txtrytes[2268..2295]);
            // extract the timestamp and year, month
            let timestamp = trytes_to_i64(&txtrytes[2322..2331]);
            let mut naive = NaiveDateTime::from_timestamp(timestamp, 0);
            let mut year = naive.year() as u16;
            let mut month = naive.month() as u8;
            if try_attachment_timestamp {
                let attachment_timestamp = trytes_to_i64(&txtrytes[2619..2628]);
                if attachment_timestamp != 0 {
                    naive = NaiveDateTime::from_timestamp(attachment_timestamp, 0);
                    year = naive.year() as u16;
                    month = naive.month() as u8
                }
            }
            // create queries related to the transaction value
            match value {
                0 => {
                    // create insert hint queries
                    let hint_query =
                        insert_to_edge_table(&txtrytes[2187..2268], "hint", 0, "0", value, YearMonth(year, month));
                    let request = reporter::Event::Request {
                        payload: hint_query,
                        worker: self.pids.pop().unwrap().query_id(2),
                    };
                    Ring::send_local_random_replica(rand::random::<i64>(), request);
                    let address_query =
                        insert_to_data_table(&txtrytes[2187..2268], year, month, "address", timestamp, hash);
                    let request = reporter::Event::Request {
                        payload: address_query,
                        worker: self.pids.pop().unwrap().query_id(3),
                    };
                    Ring::send_local_random_replica(rand::random::<i64>(), request);
                    // because it is a hint
                    self.pending += 1;
                }
                v if v > 0 => {
                    // create insert output query
                    let output_query =
                        insert_to_edge_table(&txtrytes[2187..2268], "output", timestamp, hash, value, UNSET_VALUE);
                    let request = reporter::Event::Request {
                        payload: output_query,
                        worker: self.pids.pop().unwrap().query_id(4),
                    };
                    Ring::send_local_random_replica(rand::random::<i64>(), request);
                }
                _ => {
                    // create insert input query
                    let input_query =
                        insert_to_edge_table(&txtrytes[2187..2268], "input", timestamp, hash, value, UNSET_VALUE);
                    let request = reporter::Event::Request {
                        payload: input_query,
                        worker: self.pids.pop().unwrap().query_id(5),
                    };
                    Ring::send_local_random_replica(rand::random::<i64>(), request);
                }
            }
            // insert queries not related to the transaction value
            let trunk_query = insert_to_edge_table(&txtrytes[2430..2511], "trunk", timestamp, hash, value, UNSET_VALUE);
            let request = reporter::Event::Request {
                payload: trunk_query,
                worker: self.pids.pop().unwrap().query_id(6),
            };
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            let branch_query =
                insert_to_edge_table(&txtrytes[2511..2592], "branch", timestamp, hash, value, UNSET_VALUE);
            let request = reporter::Event::Request {
                payload: branch_query,
                worker: self.pids.pop().unwrap().query_id(7),
            };
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            let bundle_query =
                insert_to_edge_table(&txtrytes[2349..2430], "bundle", timestamp, hash, value, UNSET_VALUE);
            let request = reporter::Event::Request {
                payload: bundle_query,
                worker: self.pids.pop().unwrap().query_id(8),
            };
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            let tag_query = insert_to_data_table(&txtrytes[2592..2619], year, month, "tag", timestamp, hash);
            let request = reporter::Event::Request {
                payload: tag_query,
                worker: self.pids.pop().unwrap().query_id(9),
            };
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            // process the responses for the pending queries
            while let Some(event) = self.rx.recv().await {
                match event {
                    Event::Response { decoder, pid } => {
                        self.pending -= 1;
                        self.pids.push(pid);
                        assert!(decoder.is_void());
                        if self.pending == 0 {
                            // reset max_retries to the initial state for the next line
                            self.max_retries = self.initial_max_retries;
                            // reset delay to 0 seconds for the next line
                            self.delay = 0;
                            break;
                        }
                    }
                    Event::Error { kind, pid } => {
                        if self.max_retries == 0 {
                            self.pids.push(pid);
                            error!("Importer consumed all max_retries and unable to import the dump file");
                            return Err(Box::new(kind));
                        } else {
                            self.max_retries -= 1;
                            // icrement the delay
                            self.delay += 1;
                            warn!(
                                "Importer will sleep {} seconds before retrying, because it received: {}",
                                self.delay, kind
                            );
                            let seconds = time::Duration::from_secs(self.delay as u64);
                            // sleep the importer main thread to not push any further queries to scylla
                            tokio::time::delay_for(seconds).await;
                            // retry the specific query based on its query_id using send_global_random_replica strategy
                            match pid.get_query_id() {
                                1 => {
                                    let tx_query = insert_to_tx_table(hash, txtrytes, self.milestone);
                                    let request = reporter::Event::Request {
                                        payload: tx_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                2 => {
                                    let hint_query = insert_to_edge_table(
                                        &txtrytes[2187..2268],
                                        "hint",
                                        0,
                                        "0",
                                        value,
                                        YearMonth(year, month),
                                    );
                                    let request = reporter::Event::Request {
                                        payload: hint_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                3 => {
                                    let address_query = insert_to_data_table(
                                        &txtrytes[2187..2268],
                                        year,
                                        month,
                                        "address",
                                        timestamp,
                                        hash,
                                    );
                                    let request = reporter::Event::Request {
                                        payload: address_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                4 => {
                                    let output_query = insert_to_edge_table(
                                        &txtrytes[2187..2268],
                                        "output",
                                        timestamp,
                                        hash,
                                        value,
                                        UNSET_VALUE,
                                    );
                                    let request = reporter::Event::Request {
                                        payload: output_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                5 => {
                                    let input_query = insert_to_edge_table(
                                        &txtrytes[2187..2268],
                                        "input",
                                        timestamp,
                                        hash,
                                        value,
                                        UNSET_VALUE,
                                    );
                                    let request = reporter::Event::Request {
                                        payload: input_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                6 => {
                                    let trunk_query = insert_to_edge_table(
                                        &txtrytes[2430..2511],
                                        "trunk",
                                        timestamp,
                                        hash,
                                        value,
                                        UNSET_VALUE,
                                    );
                                    let request = reporter::Event::Request {
                                        payload: trunk_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                7 => {
                                    let branch_query = insert_to_edge_table(
                                        &txtrytes[2511..2592],
                                        "branch",
                                        timestamp,
                                        hash,
                                        value,
                                        UNSET_VALUE,
                                    );
                                    let request = reporter::Event::Request {
                                        payload: branch_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                8 => {
                                    let bundle_query = insert_to_edge_table(
                                        &txtrytes[2349..2430],
                                        "bundle",
                                        timestamp,
                                        hash,
                                        value,
                                        UNSET_VALUE,
                                    );
                                    let request = reporter::Event::Request {
                                        payload: bundle_query,
                                        worker: pid,
                                    };
                                    Ring::send_global_random_replica(rand::random::<i64>(), request);
                                }
                                9 => {
                                    let tag_query = insert_to_data_table(
                                        &txtrytes[2592..2619],
                                        year,
                                        month,
                                        "tag",
                                        timestamp,
                                        hash,
                                    );
                                    let request = reporter::Event::Request {
                                        payload: tag_query,
                                        worker: pid,
                                    };
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
pub fn insert_to_tx_table(hash: &str, txtrytes: &str, milestone: impl ColumnEncoder) -> Vec<u8> {
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
        .value(&txtrytes[..2187]) // PAYLOAD
        .value(&txtrytes[2187..2268]) // ADDRESS
        .value(&txtrytes[2268..2295]) // VALUE
        .value(&txtrytes[2295..2322]) // OBSOLETE_TAG
        .value(&txtrytes[2322..2331]) // TIMESTAMP
        .value(&txtrytes[2331..2340]) // CURRENT_IDX
        .value(&txtrytes[2340..2349]) // LAST_IDX
        .value(&txtrytes[2349..2430]) // BUNDLE_HASH
        .value(&txtrytes[2430..2511]) // TRUNK
        .value(&txtrytes[2511..2592]) // BRANCH
        .value(&txtrytes[2592..2619]) // TAG
        .value(&txtrytes[2619..2628]) // ATCH_TIMESTAMP
        .value(&txtrytes[2628..2637]) // ATCH_TIMESTAMP_LOWER
        .value(&txtrytes[2637..2646]) // ATCH_TIMESTAMP_UPPER
        .value(&txtrytes[2646..2673]) // Nonce
        .value(milestone) // milestone
        .build(MyCompression::get());
    payload
}

/// Create insert(index) cql query in edge table
pub fn insert_to_edge_table(
    vertex: &str,
    kind: &str,
    timestamp: i64,
    tx: &str,
    value: i64,
    extra: impl ColumnEncoder,
) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(INSERT_TANGLE_EDGE_STATMENT)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(6) // the total value count
        .value(vertex) // vertex
        .value(kind) // kind
        .value(timestamp) // timestamp
        .value(tx) // tx-hash
        .value(value) // value
        .value(extra) // extra
        .build(MyCompression::get());
    payload
}
/// Create insert(index) cql query in data table
pub fn insert_to_data_table(vertex: &str, year: u16, month: u8, kind: &str, timestamp: i64, tx: &str) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(INSERT_TANGLE_DATA_STATMENT)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(6) // the total value count
        .value(vertex) // vertex
        .value(year)
        .value(month)
        .value(kind) // kind
        .value(timestamp) // timestamp
        .value(tx) // tx-hash
        .build(MyCompression::get());
    payload
}

/// Convert valid trytes to i64
pub fn trytes_to_i64(slice: &str) -> i64 {
    let trytes = TryteBuf::try_from_str(slice);
    let trit_buf: TritBuf<T1B1Buf> = trytes.unwrap().as_trits().encode();
    i64::try_from(trit_buf).unwrap()
}
pub const INSERT_TANGLE_TX_QUERY: &str = r#"
  INSERT INTO tangle.transaction (
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

pub const INSERT_TANGLE_EDGE_STATMENT: &str = r#"
  INSERT INTO tangle.edge (
    vertex,
    kind,
    timestamp,
    tx,
    value,
    extra
) VALUES (?,?,?,?,?,?);
"#;

pub const INSERT_TANGLE_DATA_STATMENT: &str = r#"
  INSERT INTO tangle.data (
    vertex,
    year,
    month,
    kind,
    timestamp,
    tx
) VALUES (?,?,?,?,?,?);
"#;
