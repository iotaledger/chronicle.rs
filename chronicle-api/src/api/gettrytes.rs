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
use chronicle_common::{
    actor,
    app,
};
use chronicle_cql::{
    frame::{
        decoder::Decoder,
        frame::Frame,
    },
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
use serde_json::Value;
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
pub struct GetTrytesId(Sender);

actor!(GetTrytesBuilder {
    hashes: Vec<Value>
});

impl GetTrytesBuilder {
    pub fn build(self) -> GetTrytes {
        GetTrytes {
            hashes: self.hashes.unwrap(),
        }
    }
}

pub struct GetTrytes {
    hashes: Vec<Value>,
}

#[derive(Serialize)]
struct ResTrytes {
    trytes: Vec<Value>,
}

enum Trytes {
    GiveLoad(Vec<u8>),
    Trytes(String),
}

impl Decoder for Trytes {
    fn decode(self) -> Self {
        if let Trytes::GiveLoad(_giveload) = self {
            // decode giveload as string/ trytes
            unimplemented!()
        // return Trytes::Trytes(String)
        } else {
            unreachable!()
        }
    }
}

struct T {}

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

    async fn process(value: &mut Value, worker: Box<GetTrytesId>, rx: &mut Receiver) -> Box<GetTrytesId> {
        // by taking the value we are leaving behind null.
        // now we try to query and get the result
        if let Value::String(hash) = value.take() {
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
                        if let Trytes::Trytes(trytes) = Trytes::GiveLoad(giveload).decode() {
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
