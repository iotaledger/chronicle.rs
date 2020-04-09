// work in progress
use crate::statements::SELECT_TX_QUERY;
use crate::ring::ring::Ring;
use crate::worker::{
    Worker,
    Error
};
use crate::stage::reporter;
use hyper::{
    Response,
    Body
};
use serde_json::Value;
use serde::Serialize;
use tokio::sync::mpsc;
use cdrs::frame::traits::{FromCursor, TryFromRow};
use cdrs::frame::{frame_result, Flag, Frame, IntoBytes};
use cdrs::types::{blob::Blob, from_cdrs::FromCDRSByName, rows::Row};
use cdrs::{query, query_values};

type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;


actor!(GetTrytesBuilder {
    hashes: Vec<Value>,
    data_center: &'static String
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
    trytes: Vec<Value>
}

impl GetTrytes {
    pub async fn run(mut self) -> Response<Body> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let mut worker = Box::new(tx);
        let len = self.hashes.len();
        let mut hashes = self.hashes.iter_mut();
        for _ in 0..len {
            worker = Self::process(hashes.next().unwrap(), worker, &mut rx).await;
        }
        let res_trytes = ResTrytes{trytes: self.hashes};
        response!(body: serde_json::to_string(&res_trytes).unwrap())
    }

    async fn process(value: &mut Value, worker: Box<Sender>, rx: &mut Receiver) -> Box<Sender> {
        // by taking the value we are leaving behind null.
        // now we try to query and get the result
        if let Value::String(hash) = value.take() {
            let request = reporter::Event::Request{
                payload: Self::query(hash),
                worker: worker
            };
            // use random token till murmur3 hash function algo impl is ready
            // send_local_random_replica will select random replica for token.
            Ring::send_local_random_replica(rand::random::<i64>(), request);
                match rx.recv().await.unwrap() {
                    Event::Response{giveload, tx} => {
                        // TODO decode the giveload and mutate the value to trytes

                        // return box<sender>
                        return tx
                    }
                    Event::Error{kind, tx} => {
                        // do nothing as the value is already null,
                        // still we can apply other retry strategies
                        return tx
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
            .finalize();
        let query = query::Query{
            query: SELECT_TX_QUERY.to_string(),
            params
         };
        Frame::new_query(query, vec![Flag::Ignore]).into_cbytes()
    }

}

pub enum Event {
    Response {
        giveload: Vec<u8>,
        tx: Box<Sender>,
    },
    Error {
        kind: Error,
        tx: Box<Sender>,
    },
}

// implementation!
impl Worker for Sender {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        // to enable reusable self(Sender), we will do unsafe trick
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let tx = Box::from_raw(raw);
            let event = Event::Response{giveload, tx};
            // now we can use raw to send self through itself.
            (*raw).send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: Error) {
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let samebox = Box::from_raw(raw);
            let event = Event::Error{kind, tx: samebox};
            // now we can use raw to send itself through itself.
            (*raw).send(event);
        }
    }

}
