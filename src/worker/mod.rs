mod preparer;
use crate::stage::reporter::{Giveload, Sender};

// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + std::fmt::Debug {
    fn send_response(self: Box<Self>, tx: &Option<Sender>, giveload: Giveload);
    fn send_error(self: Box<Self>, error: Error);
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Overload,
    Lost,
    NoRing,
}
