use chronicle_common::actor;
use crate::{
    stage::reporter,
    ring::ring::Ring,
};
use tokio::sync::mpsc;
use super::{
    Error,
    Worker,
};
use chronicle_cql::frame::decoder::{
    Frame,
    Decoder,
};
use chronicle_cql::frame::query::Query;
use chronicle_cql::frame::header::Header;
use chronicle_cql::compression::MyCompression;
use chronicle_cql::frame::consistency::Consistency;

#[derive(Debug)]
pub struct SchemaCqlId(mpsc::UnboundedSender<Event>);

pub enum Event {
    Response { decoder: Decoder },
    Error { kind: Error },
}

actor!(SchemaCqlBuilder { statement: String });

impl SchemaCqlBuilder {
    pub fn build(self) -> SchemaCql {
        SchemaCql {
            statement: self.statement.unwrap(),
        }
    }
}

pub struct SchemaCql { statement: String }

impl SchemaCql {
    pub async fn run(self) -> Result<(), Error> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let Query(payload) = Query::new()
            .version()
            .flags(MyCompression::flag())
            .stream(0)
            .opcode()
            .length()
            .statement(&self.statement)
            .consistency(Consistency::Quorum)
            .build(MyCompression::get());
        // send query to the ring
        Ring::send_local_random_replica(0,
            reporter::Event::Request {
                worker: Box::new(SchemaCqlId(tx)),
                payload
            }
        );
        if let Some(event) = rx.recv().await {
            match event {
                Event::Response{decoder: _} => {
                    // TODO add extra check if is_schema.
                    return Ok(())
                }
                Event::Error{kind} => {
                    return Err(kind)
                }
            }
        } else {
            unreachable!()
        }

    }
}

impl Worker for SchemaCqlId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        // create decoder for the giveload
        let decoder = Decoder::new(giveload, MyCompression::get());
        let event;
        // check if is_error
        if decoder.is_error() {
            event = Event::Error{kind: Error::Cql(decoder.get_error())};
        } else {
            event = Event::Response { decoder};
        }
        let _ = self.0.send(event);
    }
    fn send_error(self: Box<Self>, kind: Error) {
        let event = Event::Error { kind };
        let _ = self.0.send(event);
    }
}
