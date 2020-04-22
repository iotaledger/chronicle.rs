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
    frame::{
        frame::Frame,
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
                worker: worker,
            };
            // use random token till murmur3 hash function algo impl is ready
            // send_local_random_replica will select random replica for token.
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            match rx.recv().await.unwrap() {
                Event::Response { giveload, tx } => {
                    if Opcode::from(giveload.opcode()) == Opcode::Result {
                        if let Some(trytes) = Trytes::new(giveload).decode().finalize() {
                            *value = serde_json::value::Value::String(trytes);
                        };
                    }
                    // return box<sender>
                    return tx;
                }
                Event::Error { kind: _, tx } => {
                    // do nothing as the value is already null,
                    // still we can apply other retry strategies
                    return tx;
                }
            }
        } else {
            unreachable!()
        }
    }
    // unfortunately for now a lot of un necessarily heap allocation in cdrs.
    // this is a cql query frame, later we will impl execute frame.
    fn query(hash: String) -> Vec<u8> {
        let params = query::QueryParamsBuilder::new()
            .values(query_values!(hash))
            .page_size(1)
            .flags(vec![QueryFlags::SkipMetadata])
            .finalize();
        let query = query::Query {
            query: SELECT_TX_QUERY.to_string(),
            params,
        };
        CdrsFrame::new_query(query, vec![Flag::Ignore]).into_cbytes()
    }
}

pub enum Event {
    Response { giveload: Vec<u8>, tx: Box<GetTrytesId> },
    Error { kind: Error, tx: Box<GetTrytesId> },
}

// implementation!
impl Worker for GetTrytesId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        // to enable reusable self(Sender), we will do unsafe trick
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let tx = Box::from_raw(raw);
            let event = Event::Response { giveload, tx };
            // now we can use raw to send self through itself.
            (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: Error) {
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let samebox = Box::from_raw(raw);
            let event = Event::Error { kind, tx: samebox };
            // now we can use raw to send itself through itself.
            (*raw).0.send(event);
        }
    }
}

rows!(
    rows: Trytes {},
    row: Row(
        Hash,
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
        Nonce,
        Milestone
    ),
    column_decoder: TrytesDecoder
);

// define decoder trait as you wish
trait Rows {
    // to decode the rows
    fn decode(self) -> Self;
    // to finalize it as the expected result (trytes or none)
    fn finalize(self) -> Option<String> ;
}

impl Rows for Trytes {
    fn decode(mut self) -> Self {
        while let Some(_remaining_rows_count) = self.next() {
            // one transaction can only have one row ever
        }
        // it should be fine now to finalize the buffer as stringtryte,
        // return
        self
    }
    fn finalize(mut self) -> Option<String> {
        // check if result is not empty
        if self.rows_count == 1 {
            // the buffer is ready to be converted to string trytes
            self.buffer.truncate(2763);
            Some(String::from_utf8(self.buffer).unwrap())
        } else {
            // we didn't have any transaction row for the provided hash.
            None
        }
    }
}
// implementation to decoder the columns in order to form the trytes eventually
impl TrytesDecoder for Hash {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // we don't need the hash to build the trytes, so nothing should be done.
    }
}
impl TrytesDecoder for Payload {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Address {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Value {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for ObsoleteTag {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Timestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for CurrentIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for LastIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Bundle {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Trunk {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Branch {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Tag {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for AttachmentTimestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for AttachmentTimestampLower {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for AttachmentTimestampUpper {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Nonce {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
impl TrytesDecoder for Milestone {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        todo!()
    }
}
