// uses
use super::reporter;
use super::supervisor;
use tokio::net::TcpStream;
use tokio::io::ReadHalf;
use tokio::prelude::*;


// consts
const HEADER_LENGTH: usize = 9;
const BUFFER_LENGTH: usize = 1024000;

pub struct ReceiverBuidler {
    supervisor_tx: Option<supervisor::Sender>,
    reporters: Option<supervisor::Reporters>,
    socket_rx: Option<ReadHalf<TcpStream>>,
    session_id: Option<usize>,
}

impl ReceiverBuidler {
    pub fn new() -> Self {
        ReceiverBuidler {
            supervisor_tx: None,
            reporters: None,
            socket_rx: None,
            session_id: None
        }
    }

    set_builder_option_field!(socket_rx, ReadHalf<TcpStream>);
    set_builder_option_field!(session_id, usize);
    set_builder_option_field!(supervisor_tx, supervisor::Sender);
    set_builder_option_field!(reporters, supervisor::Reporters);

    pub fn build(self) -> Receiver {
        Receiver {
            supervisor_tx: self.supervisor_tx.unwrap(),
            reporters: self.reporters.unwrap(),
            socket: self.socket_rx.unwrap(),
            stream_id: 0,
            total_length: 0,
            header: false,
            buffer: Vec::new(),
            i: 0,
            session_id: self.session_id.unwrap(),
            events: Vec::with_capacity(1000),
        }
    }
}

// suerpvisor state struct
pub struct Receiver {
    supervisor_tx: supervisor::Sender,
    reporters: supervisor::Reporters,
    socket: ReadHalf<TcpStream>,
    stream_id: u16,
    total_length: usize,
    header: bool,
    buffer: Vec<u8>,
    i: usize,
    session_id: usize,
    events: Vec<(u16,reporter::Event)>
}

macro_rules! create_ring_mod {
    ($module:ident, $reporters:expr) => (
        pub mod $module {
            fn get_reporter_by_stream_id() {
                unimplemented!()
            }
        }
    );
}

impl Receiver {
    pub async fn run(mut self) {
        create_ring_mod!(ring, reporters);

        while let Ok(n) = self.socket.read(&mut self.buffer[self.i..]).await {
            let mut current_length = self.i + n;
            if current_length < HEADER_LENGTH {
                self.i = current_length; // not enough bytes to decode the frame-header
            } else {
                if !self.header {
                    self.total_length = get_total_length_usize(&self.buffer);
                    // resize buffer if total_length is larger than our buffer
                    self.stream_id = get_stream_id_u16(&self.buffer);
                    if self.total_length > BUFFER_LENGTH {
                        // resize the len of the buffer
                        self.buffer.resize(self.total_length, 0);
                    }
                }
                if current_length >= self.total_length {
                    let remaining_buffer = self.buffer.split_off(self.total_length);
                    // send event(response) to reporter
                    let event = reporter::Event::Response{giveload: self.buffer, stream_id: self.stream_id};
                    let reporter_tx = self.reporters.get(&0).unwrap();
                    let _ = reporter_tx.send(event);
                    // decrease total_length from current_length
                    current_length -= self.total_length;
                    // reset total_length to zero
                    self.total_length = 0;
                    // reset i to new current_length
                    self.i = current_length;
                    // process any events in the remaining_buffer.
                    self.buffer = process_remaining(remaining_buffer, &mut self.stream_id,&mut self.total_length, &mut self.header, &mut self.i, &mut self.events);
                    // drain acc events
                    for (s_id, event) in self.events.drain(..) {
                        let reporter_tx = self.reporters.get(&0).unwrap();
                        let _ = reporter_tx.send(event);
                    }
                } else {
                    self.i = current_length; // update i to n.
                    self.header = true; // as now we got the frame-header including knowing its body-length
                }
            }
        };
        // clean shutdown
        for (_,reporter_tx) in &self.reporters {
            let _ = reporter_tx.send(reporter::Event::Session(reporter::Session::CheckPoint(self.session_id)));
        }
    }
}

// private functions
fn process_remaining(mut buffer:  Vec<u8>, stream_id: &mut u16, total_length: &mut usize, header: &mut bool, current_length:&mut usize, acc: &mut Vec<(u16,reporter::Event)>) -> Vec<u8> {
    // first check if current_length hold header at least
    if *current_length >= HEADER_LENGTH {
        // decode and update total_length
        *total_length = get_total_length_usize(&buffer);
        // decode and update stream_id
        *stream_id = get_stream_id_u16(&buffer);
        // check if current_length
        if *current_length >= *total_length {
            let remaining_buffer = buffer.split_off(*total_length);
            let event = reporter::Event::Response{giveload: buffer, stream_id: *stream_id};
            acc.push((*stream_id, event));
            // reset before loop
            *current_length -= *total_length;
            process_remaining(remaining_buffer, stream_id, total_length, header, current_length, acc)
        } else {
            if *total_length > BUFFER_LENGTH {
                buffer.resize(*total_length, 0);
            } else {
                buffer.resize(BUFFER_LENGTH, 0);
            }
            *header = true;
            buffer
        }
    } else {
        // not enough to decode the buffer, make sure to resize(extend) the buffer to BUFFER_LENGTH
        buffer.resize(BUFFER_LENGTH, 0);
        *header = false;
        *total_length = 0;
        buffer
    }
}

fn get_total_length_usize(buffer: &[u8]) -> usize {
    HEADER_LENGTH +
    // plus body length
    ((buffer[5] as usize) << 24) +
    ((buffer[6] as usize) << 16) +
    ((buffer[7] as usize) <<  8) +
    ((buffer[8] as usize) <<  0)
}

fn get_stream_id_u16(buffer: &[u8]) -> u16 {
    ((buffer[2] as u16) << 8) | buffer[3] as u16
}
