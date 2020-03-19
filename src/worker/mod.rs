pub mod broker;
mod preparer;

use crate::stage::reporter::{
    Giveload,
    Sender,
    Stream
};

pub type StreamStatus = Result<Stream, Stream>;

// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + std::fmt::Debug {
    fn send_streamstatus(&mut self, stream_status: StreamStatus) -> Status;
    fn send_response(&mut self, tx: &Option<Sender>, giveload: Giveload) -> Status;
    fn send_error(&mut self, error: Error) -> Status;
}

#[derive(Debug)]
pub enum Error {
    Overload,
    Lost,
    NoRing,
}

#[derive(Clone, Copy, Debug)]
pub enum Status {
    New,     // the request cycle is New
    Sent,    // the request had been sent to the socket.
    Respond, // the request got a response.
    Done,    // the request cycle is done.
}

// query status
impl Status {
    pub fn return_streamstatus(self: &mut Status) -> Status {
        match self {
            Status::New => *self = Status::Sent,
            _ => *self = Status::Done,
        };
        return *self;
    }
    pub fn return_error(self: &mut Status) -> Status {
        *self = Status::Done;
        return *self;
    }
    pub fn return_response(self: &mut Status) -> Status {
        match self {
            Status::New => *self = Status::Respond,
            _ => *self = Status::Done,
        };
        return *self;
    }
}
