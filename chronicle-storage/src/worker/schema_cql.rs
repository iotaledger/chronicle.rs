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
use log::*;
use tokio::sync::mpsc;
#[derive(Debug)]
pub struct SchemaCqlId(mpsc::UnboundedSender<Event>);

pub enum Event {
    Response { decoder: Decoder },
    Error { kind: Error },
}

actor!(SchemaCqlBuilder {
    statement: String,
    max_retries: usize
});

impl SchemaCqlBuilder {
    pub fn build(self) -> SchemaCql {
        SchemaCql {
            statement: self.statement.unwrap(),
            max_retries: self.max_retries,
        }
    }
}

pub struct SchemaCql {
    statement: String,
    max_retries: Option<usize>,
}

impl SchemaCql {
    pub async fn run(mut self) -> Result<(), Error> {
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
            rand::random::<i64>(),
            reporter::Event::Request {
                worker: Box::new(SchemaCqlId(tx.clone())),
                payload,
            },
        );
        let mut result = Ok(());
        while let Some(event) = rx.recv().await {
            match event {
                Event::Response { .. } => {
                    // TODO add extra check if is_schema.
                    break;
                }
                Event::Error { kind } => {
                    if let Some(max_retries) = self.max_retries.as_mut() {
                        if *max_retries > 0 {
                            *max_retries -= 1;
                            warn!(
                                "Retrying schema_cql query, remaning max_retries: {}",
                                self.max_retries.as_ref().unwrap()
                            );
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
                            Ring::send_global_random_replica(
                                rand::random::<i64>(),
                                reporter::Event::Request {
                                    worker: Box::new(SchemaCqlId(tx.clone())),
                                    payload,
                                },
                            );
                        } else {
                            result = Err(kind);
                            break;
                        }
                    } else {
                        result = Err(kind);
                        break;
                    }
                }
            }
        }
        result
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
