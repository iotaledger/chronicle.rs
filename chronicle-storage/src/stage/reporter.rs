use super::{
    sender::{
        self,
        Payload,
    },
    supervisor,
};
use crate::worker::{
    Error,
    Worker,
};
use chronicle_common::actor;
use log::*;
use std::{
    collections::HashMap,
    io::{
        Error as IoError,
        ErrorKind,
    },
};
use tokio::sync::mpsc;
// types
pub type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
// Giveload type is vector<unsigned-integer-8bit>
pub type Giveload = Vec<u8>;
// stream id type
pub type Stream = i16;
// Streams type is array/list which should hold u8 from 1 to 32768
pub type Streams = Vec<Stream>;
// Worker is how will be presented in the workers_map
type Workers = HashMap<Stream, Box<dyn Worker>>;
#[derive(Debug)]
pub enum Event {
    Request { worker: Box<dyn Worker>, payload: Payload },
    Response { stream_id: Stream },
    Err(std::io::Error, Stream),
    Session(Session),
}

#[derive(Debug)]
pub enum Session {
    New(usize, sender::Sender),
    CheckPoint(usize),
    Shutdown,
}

actor!(ReporterBuilder {
    session_id: usize,
    reporter_id: u8,
    streams: Streams,
    address: String,
    shard_id: u8,
    tx: Sender,
    rx: Receiver,
    stage_tx: supervisor::Sender,
    payloads: supervisor::Payloads
});

impl ReporterBuilder {
    pub fn build(self) -> Reporter {
        Reporter {
            session_id: self.session_id.unwrap(),
            reporter_id: self.reporter_id.unwrap(),
            streams: self.streams.unwrap(),
            address: self.address.unwrap(),
            shard_id: self.shard_id.unwrap(),
            workers: HashMap::new(),
            checkpoints: 0,
            tx: self.tx,
            rx: self.rx.unwrap(),
            stage_tx: self.stage_tx.unwrap(),
            sender_tx: None,
            payloads: self.payloads.unwrap(),
        }
    }
}

pub struct Reporter {
    session_id: usize,
    reporter_id: u8,
    streams: Streams,
    address: String,
    shard_id: u8,
    workers: Workers,
    checkpoints: u8,
    tx: Option<Sender>,
    rx: Receiver,
    stage_tx: supervisor::Sender,
    sender_tx: Option<sender::Sender>,
    payloads: supervisor::Payloads,
}

impl Reporter {
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Request { worker, mut payload } => {
                    if let Some(stream) = self.streams.pop() {
                        // Send the event
                        match &self.sender_tx {
                            Some(sender) => {
                                // Assign stream_id to the payload
                                assign_stream_to_payload(stream, &mut payload);
                                // store payload as reusable at payloads[stream]
                                self.payloads[stream as usize].as_mut().replace(payload);
                                sender.send(stream).unwrap();
                                self.workers.insert(stream, worker);
                            }
                            None => {
                                // return the stream_id
                                self.streams.push(stream);
                                // This means the sender_tx had been droped as a result of checkpoint from receiver
                                worker.send_error(Error::Io(IoError::new(ErrorKind::Other, "No Sender!")));
                            }
                        }
                    } else {
                        // Send overload to the worker in-case we don't have anymore streams
                        worker.send_error(Error::Overload);
                    }
                }
                Event::Response { stream_id } => {
                    self.handle_response(stream_id);
                }
                Event::Err(io_error, stream_id) => {
                    self.handle_error(stream_id, Error::Io(io_error));
                }
                Event::Session(session) => {
                    // drop the sender_tx to prevent any further payloads, and also force sender to gracefully shutdown
                    self.sender_tx = None;
                    match session {
                        Session::New(new_session, new_sender_tx) => {
                            self.session_id = new_session;
                            self.sender_tx = Some(new_sender_tx);
                            info!(
                                "address: {}, shard_id: {}, reporter_id: {}, received session: {:?}",
                                &self.address, self.shard_id, self.reporter_id, self.session_id
                            );
                        }
                        Session::CheckPoint(old_session) => {
                            // check how many checkpoints we have.
                            if self.checkpoints == 1 {
                                // first we drain workers map from stucked requests, to force_consistency of the
                                // old_session requests
                                force_consistency(&mut self.streams, &mut self.workers);
                                // reset checkpoints to 0
                                self.checkpoints = 0;
                                warn!(
                                    "address: {}, shard_id: {}, reporter_id: {}, closing session: {:?}",
                                    &self.address, self.shard_id, self.reporter_id, old_session
                                );
                                // tell stage_tx to reconnect
                                let event = supervisor::Event::Reconnect(old_session);
                                self.stage_tx.send(event).unwrap();
                            } else {
                                self.checkpoints = 1;
                            }
                        }
                        Session::Shutdown => {
                            // set self.tx to None, otherwise reporter never shutdown.
                            self.tx = None;
                            // as we already dropped the sender_tx
                            // dropping the sender_tx will drop the sender and eventaully drop receiver , this means our
                            // reporter_tx in both sender&reciever will be dropped.. finally the only reporter_tx left
                            // is in Rings which will eventaully be dropped. techincally
                            // reporters are active till the last Ring::send(..) call.
                            // this make sure we don't leave any requests behind and enabling the workers to async send
                            // requests with guarantee to be processed back
                        }
                    }
                }
            }
        } // reporter will reach this line only when it recvs shutdown event and eventually drains its rx.
          // therefore it must drains workers map from stucked requests(if any) to force_consistency.
        force_consistency(&mut self.streams, &mut self.workers);
        warn!(
            "reporter_id: {} of shard_id: {} in node: {}, gracefully shutting down.",
            self.reporter_id, self.shard_id, &self.address
        );
    }
    fn handle_response(&mut self, stream: Stream) {
        // remove the worker from workers and send response.
        let worker = self.workers.remove(&stream).unwrap();
        worker.send_response(&self.tx, self.payloads[stream as usize].as_mut().take().unwrap());
        // push the stream_id back to streams vector.
        self.streams.push(stream);
    }
    fn handle_error(&mut self, stream: Stream, error: Error) {
        // remove the worker from workers and send error.
        let worker = self.workers.remove(&stream).unwrap();
        // drop payload.
        self.payloads[stream as usize].as_mut().take().unwrap();
        worker.send_error(error);
        // push the stream_id back to streams vector.
        self.streams.push(stream);
    }
}

// private functions
fn assign_stream_to_payload(stream: Stream, payload: &mut Payload) {
    payload[2] = (stream >> 8) as u8; // payload[2] is where the first byte of the stream_id should be,
    payload[3] = stream as u8; // payload[3] is the second byte of the stream_id. please refer to cql specs
}

fn force_consistency(streams: &mut Streams, workers: &mut Workers) {
    for (stream_id, worker_id) in workers.drain() {
        // push the stream_id back into the streams vector
        streams.push(stream_id);
        // tell worker_id that we lost the response for his request, because we lost scylla connection in middle of
        // request cycle, still this is a rare case.
        worker_id.send_error(Error::Lost);
    }
}
