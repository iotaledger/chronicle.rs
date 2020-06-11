use super::{
    Error,
    Worker,
};
use crate::{
    ring::Ring,
    stage::reporter,
};
use chronicle_common::actor;
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
        queryflags::SKIP_METADATA,
    },
};
use tokio::sync::mpsc;
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

pub struct SchemaCql {
    statement: String,
}

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
            .query_flags(SKIP_METADATA)
            .build(MyCompression::get());
        // send query to the ring
        Ring::send_local_random_replica(
            0,
            reporter::Event::Request {
                worker: Box::new(SchemaCqlId(tx)),
                payload,
            },
        );
        if let Some(event) = rx.recv().await {
            match event {
                Event::Response { .. } => {
                    // TODO add extra check if is_schema.
                    Ok(())
                }
                Event::Error { kind } => Err(kind),
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
        let event = if decoder.is_error() {
            Event::Error {
                kind: Error::Cql(decoder.get_error()),
            }
        } else {
            Event::Response { decoder }
        };
        let _ = self.0.send(event);
    }
    fn send_error(self: Box<Self>, kind: Error) {
        let event = Event::Error { kind };
        let _ = self.0.send(event);
    }
}
