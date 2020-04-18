// please note: preparer is not a real actor instead is a resumption actor (without its own mailbox/channel).
use super::{
    Error,
    Worker,
};
use crate::stage::reporter::{
    Event,
    Giveload,
    Sender,
};

#[derive(Debug)]
pub struct QueryRef {}

impl QueryRef {
    fn new() -> Self {
        QueryRef {}
    }
}

#[derive(Debug)]
pub struct Preparer {
    query: QueryRef,
}

impl Worker for Preparer {
    fn send_response(self: Box<Self>, _tx: &Option<Sender>, _giveload: Vec<u8>) {}
    fn send_error(self: Box<Self>, _error: Error) {}
}

pub fn try_prepare(prepare_payload: &[u8], tx: &Option<Sender>, giveload: &Giveload) {
    // check if the giveload is unprepared_error.
    if check_unprepared(giveload) {
        // create preparer
        let preparer = Preparer { query: QueryRef::new() };
        // create event query
        let event = Event::Request {
            payload: prepare_payload.to_vec(),
            worker: Box::new(preparer),
        };
        // send to reporter(self as this function is invoked inside reporter)
        if let Some(tx) = tx {
            tx.send(event).unwrap();
        };
    }
}

fn check_unprepared(giveload: &Giveload) -> bool {
    giveload[4] == 0 && giveload[9..13] == [0, 0, 37, 0] // cql specs
}
