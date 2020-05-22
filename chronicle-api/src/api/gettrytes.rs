use chronicle_common::actor;
use chronicle_cql::{
    compression::UNCOMPRESSED,
    frame::decoder::{
        Decoder,
        Frame,
    },
    frame::header::Header,
    frame::header,
    frame::queryflags,
    frame::query::Query,
    frame::error,
    frame::consistency::Consistency,
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
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
pub struct GetTrytesId(Sender);

actor!(GetTrytesBuilder {
    hashes: Vec<JsonValue>
});

impl GetTrytesBuilder {
    pub fn build(self) -> GetTrytes {
        GetTrytes {
            hashes: self.hashes.unwrap(),
        }
    }
}

pub struct GetTrytes {
    hashes: Vec<JsonValue>,
}

#[derive(Serialize)]
struct ResTrytes {
    trytes: Vec<JsonValue>,
}

impl GetTrytes {
    pub async fn run(mut self) -> Response<Body> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let mut worker = Box::new(GetTrytesId(tx));
        let mut hashes = self.hashes.iter_mut();
        while let Some(value) = hashes.next() {
            worker = Self::process(value, worker, &mut rx).await;
        }
        let res_trytes = ResTrytes { trytes: self.hashes };
        response!(body: serde_json::to_string(&res_trytes).unwrap())
    }

    async fn process(value: &mut JsonValue, worker: Box<GetTrytesId>, rx: &mut Receiver) -> Box<GetTrytesId> {
        // by taking the value we are leaving behind null.
        // now we try to query and get the result
        if let JsonValue::String(hash) = value.take() {
            let request = reporter::Event::Request {
                payload: Self::query(hash),
                worker,
            };
            // use random token till murmur3 hash function algo impl is ready
            // send_local_random_replica will select random replica for token.
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            match rx.recv().await.unwrap() {
                Event::Response { giveload, pid } => {
                    // create decoder
                    let decoder = Decoder::new(giveload, UNCOMPRESSED);
                    if decoder.is_rows() {
                        if let Some(trytes) = Trytes::new(decoder).decode().finalize() {
                            *value = serde_json::value::Value::String(trytes);
                        };
                    } else {
                        let error = error::CqlError::from(decoder.body());
                        println!("GetTrytes: {:?}", error);
                    }
                    return pid;
                }
                Event::Error { kind: _, pid } => {
                    // do nothing as the value is already null,
                    // still we can apply other retry strategies
                    return pid;
                }
            }
        } else {
            unreachable!()
        }
    }
    // unfortunately for now a lot of un necessarily heap allocation in cdrs.
    // this is a cql query frame, later we will impl execute frame.
    fn query(hash: String) -> Vec<u8> {
        let Query(payload) = Query::new()
            .version()
            .flags(header::IGNORE)
            .stream(0)
            .opcode()
            .length()
            .statement(SELECT_TX_QUERY)
            .consistency(Consistency::One)
            .query_flags(queryflags::SKIP_METADATA | queryflags::VALUES | queryflags::PAGE_SIZE)
            .value_count(1) // the total value count
            .value(hash)
            .page_size(1)
            .build(UNCOMPRESSED);
        payload
    }
}

pub enum Event {
    Response { giveload: Vec<u8>, pid: Box<GetTrytesId> },
    Error { kind: Error, pid: Box<GetTrytesId> },
}

// implementation!
impl Worker for GetTrytesId {
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

rows!(
    rows: Trytes {},
    row: Row(
        Payload,
        Address,
        Value,
        ObsoleteTag,
        Timestamp,
        CurrentIndex,
        LastIndex,
        Bundle,
        Trunk,
        Branch,
        Tag,
        AttachmentTimestamp,
        AttachmentTimestampLower,
        AttachmentTimestampUpper,
        Nonce
    ),
    column_decoder: TrytesDecoder
);

// define decoder trait as you wish
trait Rows {
    // to decode the rows
    fn decode(self) -> Self;
    // to finalize it as the expected result (trytes or none)
    fn finalize(self) -> Option<String>;
}

impl Rows for Trytes {
    fn decode(mut self) -> Self {
        // each next() call will decode one row
        self.next();
        // return
        self
    }
    fn finalize(mut self) -> Option<String> {
        // check if result was not empty
        if self.rows_count == 1 {
            // the buffer is ready to be converted to string trytes
            self.buffer().truncate(2673);
            Some(String::from_utf8(self.decoder.into_buffer()).unwrap())
        } else {
            // we didn't have any transaction row for the provided hash.
            None
        }
    }
}
// implementation to decoder the columns in order to form the trytes eventually
impl TrytesDecoder for Payload {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Payload trytes offest is 0..2187, note: assuming length != -1(indicate empty column).
        // copy_within so a buffer[0..2187] will = buffer[start..length]
        acc.buffer().copy_within(start..(start + length as usize), 0)
    }
}
impl TrytesDecoder for Address {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Address trytes offest is 2187..2268, note: we assume the length value is also correct
        acc.buffer().copy_within(start..(start + length as usize), 2187)
    }
}
impl TrytesDecoder for Value {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Value tryte offset is 2268..2295
        acc.buffer().copy_within(start..(start + length as usize), 2268);
    }
}
impl TrytesDecoder for ObsoleteTag {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // ObsoleteTag tryte offset is 2295..2322
        acc.buffer().copy_within(start..(start + length as usize), 2295)
    }
}
impl TrytesDecoder for Timestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Timestamp tryte offset is 2322..2331
        acc.buffer().copy_within(start..(start + length as usize), 2322);
    }
}
impl TrytesDecoder for CurrentIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // CurrentIndex tryte offset is 2331..2340
        acc.buffer().copy_within(start..(start + length as usize), 2331);
    }
}
impl TrytesDecoder for LastIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // LastIndex tryte offset is 2340..2349
        acc.buffer().copy_within(start..(start + length as usize), 2340);
    }
}
impl TrytesDecoder for Bundle {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Bundle tryte offset is 2349..2430
        acc.buffer().copy_within(start..(start + length as usize), 2349)
    }
}
impl TrytesDecoder for Trunk {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Trunk tryte offset is 2430..2511
        acc.buffer().copy_within(start..(start + length as usize), 2430)
    }
}
impl TrytesDecoder for Branch {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Branch tryte offset is 2511..2592
        acc.buffer().copy_within(start..(start + length as usize), 2511)
    }
}
impl TrytesDecoder for Tag {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Tag tryte offset is 2592..2619
        acc.buffer().copy_within(start..(start + length as usize), 2592)
    }
}
impl TrytesDecoder for AttachmentTimestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // AttachmentTimestamp tryte offset is 2619..2628
        acc.buffer().copy_within(start..(start + length as usize), 2619);
    }
}
impl TrytesDecoder for AttachmentTimestampLower {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // AttachmentTimestampLower tryte offset is 2628..2637
        acc.buffer().copy_within(start..(start + length as usize), 2628);
    }
}
impl TrytesDecoder for AttachmentTimestampUpper {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // AttachmentTimestampUpper tryte offset is 2637..2646
        acc.buffer().copy_within(start..(start + length as usize), 2637);
    }
}
impl TrytesDecoder for Nonce {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Tag tryte offset is 2646..2673
        acc.buffer().copy_within(start..(start + length as usize), 2646)
    }
}
