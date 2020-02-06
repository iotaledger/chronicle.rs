// please note: preparer is not a real actor instead is a resumption actor (without its own mailbox/channel).
use super::reporter::{WorkerId, Status,SendStatus, Error, Sender,Giveload, Event};

pub struct QueryRef {status: Status}

// QueryRef new
impl QueryRef {
    fn new() -> Self {
        QueryRef {status: Status::New}
    }
}

// worker's WorkerId struct
pub struct PreparerId {query_reference: QueryRef}

impl WorkerId for PreparerId {
    fn send_sendstatus_ok(&mut self, _send_status: SendStatus) -> Status {
        self.query_reference.status.return_sendstatus_ok()
     }
     fn send_sendstatus_err(&mut self, _send_status: SendStatus) -> Status {
         self.query_reference.status.return_error()
      }
     fn send_response(&mut self, _tx: &Sender, _giveload: Vec<u8>) -> Status {
         self.query_reference.status.return_response()
      }
      fn send_error(&mut self, _error: Error) -> Status {
          self.query_reference.status.return_error()
      }
}


pub fn try_prepare(prepare_payload:  &[u8],tx: &Sender, giveload: &Giveload)  {
    // check if the giveload is unprepared_error.
    if check_unprepared(giveload) {
        // create preparer
        let preparer = PreparerId {query_reference: QueryRef::new()};
        // create event query
        let event = Event::Query{payload: prepare_payload.to_vec(), worker: Box::new(preparer)};
        // send to reporter(self as this function is invoked inside reporter)
        let _ = tx.send(event);
    }
}

fn check_unprepared(giveload: &Giveload) -> bool {
    giveload[3] == 0 && giveload[7..11] == [0,0,37,0]
}
