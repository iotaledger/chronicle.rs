// uses
use super::sender;
use super::supervisor;
use tokio::sync::mpsc;
use std::collections::HashMap;

// types
pub type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
// Payload type is vector<unsigned-integer-8bit>
type Payload = Vec<u8>; 
// Giveload type is vector<unsigned-integer-8bit>
pub type Giveload = Vec<u8>;
// stream id type
type StreamId = u16;
// Worker is how will be presented in the workers_map
type Worker = Box<dyn WorkerId>;
// StreamIds type is array/list which should hold u8 from 1 to 32768
type StreamIds = Vec<StreamId>;
// Workers type is array/list where the element is the worker_id which is a worker_tx
type Workers = HashMap<u16,Worker>;

#[derive(Debug)]
pub enum Event {
    Query {
        worker: Box<dyn WorkerId>,
        payload: Payload,
    },
    Response{giveload: Giveload, stream_id: StreamId},
    SendStatus(SendStatus),
    Session(Session),
}

#[derive(Clone, Copy, Debug)]
pub enum SendStatus {
    Ok(StreamId),
    Err(StreamId),
}

pub enum Error {
    Overload,
    Lost,
}

#[derive(Debug)]
pub enum Session {
    New(usize,sender::Sender),
    CheckPoint(usize),
    Shutdown,
}

#[derive(Clone, Copy)]
pub enum Status {
    New, // the query cycle is New
    Sent, // the query had been sent to the socket.
    Respond, // the query got a response.
    Done, // the query cycle is done.
}

// query status
impl Status {
    pub fn return_sendstatus_ok(self: &mut Status) -> Status {
        match self {
            Status::New => {
                *self = Status::Sent
            }
            _ => {
                *self = Status::Done
            }
        };
        return *self
    }
    pub fn return_error(self: &mut Status) -> Status {
        *self = Status::Done;
        return *self
    }
    pub fn return_response(self: &mut Status) -> Status {
        match self {
            Status::New => {
                *self = Status::Respond
            }
            _ => {
                *self = Status::Done
            }
        };
        return *self
    }
}

// WorkerId trait type which will be implemented by worker in order to send their channeL_tx.
pub trait WorkerId: Send + std::fmt::Debug {
    fn send_sendstatus_ok(&mut self, send_status: SendStatus) -> Status ;
    fn send_sendstatus_err(&mut self, send_status: SendStatus) -> Status ;
    fn send_response(&mut self,tx: &Sender, giveload: Giveload) -> Status ;
    fn send_error(&mut self, error: Error) -> Status ;
}

pub struct ReporterBuilder {
    tx: Option<Sender>,
    rx: Option<Receiver>,
    sender_tx: Option<sender::Sender>,
    session_id: Option<usize>,
    supervisor_tx: Option<supervisor::Sender>,
    reporter_num: Option<u8>,
    stream_ids: Option<StreamIds>,
    address: Option<String>,
    shard: Option<u8>,
}

impl ReporterBuilder {
    pub fn new() -> Self {
        ReporterBuilder{
            tx: None,
            rx: None,
            sender_tx: None,
            session_id: None,
            supervisor_tx: None,
            reporter_num: None,
            stream_ids: None,
            address: None,
            shard: None,
        }
    }

    pub fn tx(mut self, tx: Sender) -> Self {
        self.tx.replace(tx);
        self
    }
    
    pub fn rx(mut self, rx: Receiver) -> Self {
        self.rx.replace(rx);
        self
    }

    pub fn sender_tx(mut self, sender_tx: sender::Sender) -> Self {
        self.sender_tx.replace(sender_tx);
        self
    }

    pub fn session_id(mut self, session_id: usize) -> Self {
        self.session_id.replace(session_id);
        self
    }

    pub fn supervisor_tx(mut self, supervisor_tx: supervisor::Sender) -> Self {
        self.supervisor_tx.replace(supervisor_tx);
        self
    }

    pub fn reporter_num(mut self, reporter_num: u8) -> Self {
        self.reporter_num.replace(reporter_num);
        self
    }

    pub fn stream_ids(mut self, stream_ids: StreamIds) -> Self {
        self.stream_ids.replace(stream_ids);
        self
    }

    pub fn address(mut self, address: String) -> Self {
        self.address.replace(address);
        self
    }

    pub fn shard(mut self, shard: u8) -> Self {
        self.shard.replace(shard);
        self
    }

    pub fn build(self) -> Reporter {
        Reporter {
            stream_ids: self.stream_ids.unwrap(),
            workers: HashMap::new(),
            tx: self.tx.unwrap(),
            rx: self.rx.unwrap(),
            sender_tx: self.sender_tx.unwrap(),
            session_id: self.session_id.unwrap(),
            checkpoints: 0,
            supervisor_tx: self.supervisor_tx.unwrap(),
            reporter_num: self.reporter_num.unwrap(),
            address: self.address.unwrap(),
            shard: self.shard.unwrap(),
        }
    }
}

// reporter state struct holds StreamIds and Workers and the reporter's Sender
pub struct Reporter {
    stream_ids: StreamIds,
    workers: Workers,
    tx: Sender,
    rx: Receiver,
    sender_tx: sender::Sender,
    session_id: usize,
    checkpoints: u8,
    supervisor_tx: supervisor::Sender,
    reporter_num: u8,
    address: String,
    shard: u8,
}

impl Reporter {
    pub async fn run(mut self) -> () {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Query{mut worker, payload} => {
                    if let Some(stream_id) = self.stream_ids.pop()   {
                        // insert worker into workers map using stream_id as key.
                        self.workers.insert(stream_id, worker);
                        // assign stream_id to the payload.
                        let payload = assign_stream_id_to_payload(stream_id, payload);
                        // put the payload inside an event of a socket_sender(the sender_tx inside reporter's state)
                        let event = sender::Event::Payload{stream_id: stream_id, payload: payload, reporter: self.tx.clone()};
                        // send the event
                        self.sender_tx.send(event).unwrap(); // make sure the sender_tx is not closed, if error
                        // reply with
                    } else {
                        // send_overload to the worker in-case we don't have anymore stream_ids.
                        worker.send_error(Error::Overload);
                    }
                },
                Event::Response{giveload, stream_id} => {

                    let worker = self.workers.get_mut(&stream_id).unwrap();
                    if let Status::Done = worker.send_response(&self.tx, giveload) {
                        // remove the worker from workers.
                        self.workers.remove(&stream_id);
                        // push the stream_id back to stream_ids vector.
                        self.stream_ids.push(stream_id);
                    };
                }
                Event::SendStatus(send_status) => {
                    match send_status {
                        SendStatus::Ok(stream_id) => {
                            // get_mut worker from workers map.
                            println!("stream_id {:?}", stream_id);
                            let worker = self.workers.get_mut(&stream_id).unwrap();
                            // tell the worker and mutate its status,
                            if let Status::Done = worker.send_sendstatus_ok(send_status){
                                // remove the worker from workers.
                                self.workers.remove(&stream_id);
                                // push the stream_id back to stream_ids vector.
                                self.stream_ids.push(stream_id);
                            };
                        },
                        SendStatus::Err(stream_id) => {
                            // get_mut worker from workers map.
                            let worker = self.workers.get_mut(&stream_id).unwrap();
                            // tell the worker and mutate worker status,
                            let _status_done = worker.send_sendstatus_err(send_status);
                            // remove the worker from workers.
                            self.workers.remove(&stream_id);
                            // push the stream_id back to stream_ids vector.
                            self.stream_ids.push(stream_id);
                        },
                    }
                }
                Event::Session(checkpoint) => {
                    match checkpoint {
                        Session::New(new_session_id, new_sender_tx) => {
                            self.session_id = new_session_id;
                            self.sender_tx = new_sender_tx;
                            println!("new session_id: {:?}", self.session_id);
                        },
                        Session::CheckPoint(old_session_id) => {
                            // check how many checkpoints we have.
                            if self.checkpoints == 1 {
                                // first we drain workers map from stucked requests
                                for (stream_id, mut worker_id) in self.workers.drain() {
                                    // push the stream_id back into the stream_ids vector
                                    self.stream_ids.push(stream_id);
                                    // tell worker_id
                                    worker_id.send_error(Error::Lost);
                                }
                                println!("session_id: {:?} has been closed", old_session_id);
                                // reset checkpoints to 0
                                self.checkpoints = 0;
                                // tell supervisor_tx to reconnect
                                let event = supervisor::Event::Reconnect(old_session_id);
                                self.supervisor_tx.send(event).unwrap();
                            } else {
                                self.checkpoints = 1;
                            }
                        }
                        Session::Shutdown => {
                            // closing rx is enough to shutdown the reporter
                            self.rx.close();
                        }

                    }
                },
            }
        };
        println!("reporter: {} of shard: {} in node: {}, gracefully shutting down.", self.reporter_num, self.shard, self.address);
    }
}

// private functions
fn assign_stream_id_to_payload(stream_id: StreamId,mut payload: Payload) -> Payload {
    payload[2] = (stream_id >> 8) as u8; // payload[2] is where the first byte of the stream_id should be,
    payload[3] = stream_id as u8; // payload[3] is the second byte of the stream_id. please refer to cql specs
    return payload
}
