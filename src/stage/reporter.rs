use super::sender::{self, Payload};
use super::supervisor;
use crate::worker::{Error, Status, StreamStatus, Worker};
use std::collections::HashMap;
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
    Request {
        worker: Box<dyn Worker>,
        payload: Payload,
    },
    Response {
        giveload: Giveload,
        stream_id: Stream,
    },
    StreamStatus(StreamStatus),
    Session(Session),
}

#[derive(Debug)]
pub enum Session {
    New(usize, sender::Sender),
    CheckPoint(usize),
    Shutdown,
}

pub struct ReporterBuilder {
    session_id: Option<usize>,
    reporter_id: Option<u8>,
    streams: Option<Streams>,
    address: Option<String>,
    shard_id: Option<u8>,
    tx: Option<Sender>,
    rx: Option<Receiver>,
    stage_tx: Option<supervisor::Sender>,
    sender_tx: Option<sender::Sender>,
}

impl ReporterBuilder {
    pub fn new() -> Self {
        ReporterBuilder {
            tx: None,
            rx: None,
            stage_tx: None,
            sender_tx: None,
            session_id: None,
            reporter_id: None,
            streams: None,
            address: None,
            shard_id: None,
        }
    }

    set_builder_option_field!(tx, Sender);
    set_builder_option_field!(rx, Receiver);
    set_builder_option_field!(sender_tx, sender::Sender);
    set_builder_option_field!(session_id, usize);
    set_builder_option_field!(stage_tx, supervisor::Sender);
    set_builder_option_field!(reporter_id, u8);
    set_builder_option_field!(streams, Streams);
    set_builder_option_field!(address, String);
    set_builder_option_field!(shard_id, u8);

    pub fn build(self) -> Reporter {
        Reporter {
            session_id: self.session_id.unwrap(),
            reporter_id: self.reporter_id.unwrap(),
            streams: self.streams.unwrap(),
            address: self.address.unwrap(),
            shard_id: self.shard_id.unwrap(),
            workers: HashMap::new(),
            checkpoints: 0,
            tx: self.tx.unwrap(),
            rx: self.rx.unwrap(),
            stage_tx: self.stage_tx.unwrap(),
            sender_tx: self.sender_tx,
        }
    }
}

// reporter state struct holds Streams and Workers and the reporter's Sender
pub struct Reporter {
    session_id: usize,
    reporter_id: u8,
    streams: Streams,
    address: String,
    shard_id: u8,
    workers: Workers,
    checkpoints: u8,
    tx: Sender,
    rx: Receiver,
    stage_tx: supervisor::Sender,
    sender_tx: Option<sender::Sender>,
}

impl Reporter {
    pub async fn run(mut self) -> () {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Request {
                    mut worker,
                    mut payload,
                } => {
                    if let Some(stream) = self.streams.pop() {
                        // Assign stream_id to the payload
                        assign_stream_to_payload(stream, &mut payload);
                        // Put the payload inside an event of a socket_sender(the sender_tx inside reporter's state)
                        let event = sender::Event::Payload {
                            stream: stream,
                            payload: payload,
                            reporter_id: self.reporter_id,
                        };
                        // Send the event
                        match &self.sender_tx {
                            Some(sender) => {
                                sender.send(event).unwrap();
                                // Insert worker into workers map using stream_id as key
                                self.workers.insert(stream, worker);
                            }
                            None => {
                                // This means the sender_tx had been droped as a result of checkpoint
                                worker.send_streamstatus(StreamStatus::Err(0));
                            }
                        }
                    } else {
                        // Send overload to the worker in-case we don't have anymore streams
                        worker.send_error(Error::Overload);
                    }
                }
                Event::Response {
                    giveload,
                    stream_id,
                } => {
                    let worker = self.workers.get_mut(&stream_id).unwrap();
                    if let Status::Done = worker.send_response(&self.tx, giveload) {
                        // Remove the worker from workers.
                        self.workers.remove(&stream_id).unwrap();
                        // Push the stream_id back to streams vector.
                        self.streams.push(stream_id);
                    };
                }
                Event::StreamStatus(send_status) => {
                    match send_status {
                        StreamStatus::Ok(stream_id) => {
                            // get_mut worker from workers map.
                            dbg!("stream_id {:?}", stream_id);
                            let worker = self.workers.get_mut(&stream_id).unwrap();
                            // tell the worker and mutate its status,
                            if let Status::Done = worker.send_streamstatus(send_status) {
                                // remove the worker from workers.
                                self.workers.remove(&stream_id);
                                // push the stream_id back to streams vector.
                                self.streams.push(stream_id);
                            };
                        }
                        StreamStatus::Err(stream_id) => {
                            // get_mut worker from workers map.
                            let worker = self.workers.get_mut(&stream_id).unwrap();
                            // tell the worker and mutate worker status,
                            let _status_done = worker.send_streamstatus(send_status);
                            // remove the worker from workers.
                            self.workers.remove(&stream_id);
                            // push the stream_id back to streams vector.
                            self.streams.push(stream_id);
                        }
                    }
                }
                Event::Session(session) => {
                    // drop the sender_tx to prevent any further payloads, and also force sender to gracefully shutdown
                    self.sender_tx = None;
                    match session {
                        Session::New(new_session, new_sender_tx) => {
                            self.session_id = new_session;
                            self.sender_tx = Some(new_sender_tx);
                            dbg!(
                                "address: {}, shard_id: {}, reporter_id: {}, received new session: {:?}",
                                &self.address,
                                self.shard_id,
                                self.reporter_id,
                                self.session_id
                            );
                        }
                        Session::CheckPoint(old_session) => {
                            // check how many checkpoints we have.
                            if self.checkpoints == 1 {
                                // first we drain workers map from stucked requests, to force_consistency of the old_session requests
                                force_consistency(&mut self.streams, &mut self.workers);
                                // reset checkpoints to 0
                                self.checkpoints = 0;
                                dbg!("address: {}, shard_id: {}, reporter_id: {}, received new session: {:?}", &self.address, self.shard_id, self.reporter_id, old_session);
                                // tell stage_tx to reconnect
                                let event = supervisor::Event::Reconnect(old_session);
                                self.stage_tx.send(event).unwrap();
                            } else {
                                self.checkpoints = 1;
                            }
                        }
                        Session::Shutdown => {
                            // closing rx is enough to shutdown the reporter
                            self.rx.close();
                        }
                    }
                }
            }
        } // reporter will reach this line only when it recvs shutdown event and eventually drains its rx.
          // therefore it must drains workers map from stucked requests(if any) to force_consistency.
        force_consistency(&mut self.streams, &mut self.workers);
        dbg!(
            "reporter_id: {} of shard_id: {} in node: {}, gracefully shutting down.",
            self.reporter_id,
            self.shard_id,
            &self.address
        );
    }
}

// private functions
fn assign_stream_to_payload(stream: Stream, payload: &mut Payload) {
    payload[2] = (stream >> 8) as u8; // payload[2] is where the first byte of the stream_id should be,
    payload[3] = stream as u8; // payload[3] is the second byte of the stream_id. please refer to cql specs
}

fn force_consistency(streams: &mut Streams, workers: &mut Workers) {
    for (stream_id, mut worker_id) in workers.drain() {
        // push the stream_id back into the streams vector
        streams.push(stream_id);
        // tell worker_id that we lost the response for his request, because we lost scylla connection in middle of request cycle,
        // still this is a rare case.
        worker_id.send_error(Error::Lost);
    }
}
