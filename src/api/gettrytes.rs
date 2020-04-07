// work in progress
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

type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;

#[derive(Serialize)]
struct Trytes(Vec<Value>);

actor!(GetTrytesBuilder {
    hashes: Vec<String>
});

impl GetTrytesBuilder {
    pub fn build(self) -> GetTrytes {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        GetTrytes {
            hashes: self.hashes.unwrap(),
            trytes: Trytes(Vec::new()),
            tx: Box::new(tx),
            rx
        }
    }
}

pub struct GetTrytes {
    hashes: Vec<String>,
    trytes: Trytes,
    tx: Box<Sender>,
    rx: Receiver,
}

impl GetTrytes {
    pub async fn run(mut self) -> Response<Body> {
        for hash_index in 0..self.hashes.len() {
            let value = self.process(hash_index).await;
            self.trytes.0.push(value);
        }
        response!(body: serde_json::to_string(&self.trytes).unwrap())
    }

    async fn process(&mut self, hash_index: usize) -> Value {
        unimplemented!()
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
