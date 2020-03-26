// please note: preparer is not a real actor instead is a resumption actor (without its own mailbox/channel).
use super::{Error, StreamStatus, Status, Worker};
use crate::stage::reporter::{Event, Giveload, Sender};

#[derive(Debug)]
pub struct QueryRef {
    status: Status,
}

impl QueryRef {
    fn new() -> Self {
        QueryRef {
            status: Status::New,
        }
    }
}

#[derive(Debug)]
pub struct Preparer {
    query: QueryRef,
}

impl Worker for Preparer {
    fn send_streamstatus(&mut self, stream_status: StreamStatus) -> Status {
        match stream_status {
            Ok(_) => self.query.status.return_streamstatus(),
            Err(_) => self.query.status.return_error()
        }
    }

    fn send_response(&mut self, _tx: &Option<Sender>, _giveload: Vec<u8>) -> Status {
        self.query.status.return_response()
    }
    fn send_error(&mut self, _error: Error) -> Status {
        self.query.status.return_error()
    }
}

pub fn try_prepare(prepare_payload: &[u8], tx: &Option<Sender>, giveload: &Giveload) {
    // check if the giveload is unprepared_error.
    if check_unprepared(giveload) {

        // create preparer
        let preparer = Preparer {
            query: QueryRef::new(),
        };
        // create event query
        let event = Event::Request {
            payload: prepare_payload.to_vec(),
            worker: smallbox!(preparer),
        };
        // send to reporter(self as this function is invoked inside reporter)
        if let Some(tx) = tx {
            tx
            .send(event)
            .unwrap();
        };
    }
}

fn check_unprepared(giveload: &Giveload) -> bool {
    giveload[3] == 0 && giveload[7..11] == [0, 0, 37, 0] // cql specs
}
