// uses
use super::reporter;
use super::reporter::Stream;
use super::supervisor;
use super::supervisor::Reporters;
use tokio::io::ReadHalf;
use tokio::net::TcpStream;
use tokio::prelude::*;

// consts
const HEADER_LENGTH: usize = 9;
const BUFFER_LENGTH: usize = 1024000;

actor!(
    ReceiverBuidler {
        reporters: supervisor::Reporters,
        socket_rx: ReadHalf<TcpStream>,
        session_id: usize
});

impl ReceiverBuidler {

    pub fn build(self) -> Receiver {
        let reporters = self.reporters.unwrap();
        let reporters_len = reporters.len();
        Receiver {
            reporters: reporters,
            socket: self.socket_rx.unwrap(),
            stream_id: 0,
            total_length: 0,
            header: false,
            buffer: vec![0; BUFFER_LENGTH],
            i: 0,
            session_id: self.session_id.unwrap(),
            appends_num: 32767 / reporters_len as i16,
        }
    }
}

// suerpvisor state struct
pub struct Receiver {
    reporters: supervisor::Reporters,
    socket: ReadHalf<TcpStream>,
    stream_id: reporter::Stream,
    total_length: usize,
    header: bool,
    buffer: Vec<u8>,
    i: usize,
    session_id: usize,
    appends_num: i16,
}

impl Receiver {
    pub async fn run(mut self) {
        // receiver event loop
        while let Ok(n) = self.socket.read(&mut self.buffer[self.i..]).await {
            // if n != 0 then the socket is not closed
            if n != 0 {
                let mut current_length = self.i + n; // cuurrent buffer length is i(recent index) + received n-bytes
                if current_length < HEADER_LENGTH {
                    self.i = current_length; // not enough bytes to decode the frame-header
                } else {
                    // if no-header decode the header and resize the buffer(if needed).
                    if !self.header {
                        // decode total_length(HEADER_LENGTH + frame_body_length)
                        self.total_length = get_total_length_usize(&self.buffer);
                        // decode stream_id
                        self.stream_id = get_stream_id(&self.buffer);
                        // resize buffer only if total_length is larger than our buffer
                        if self.total_length > BUFFER_LENGTH {
                            // resize the len of the buffer.
                            self.buffer.resize(self.total_length, 0);
                        }
                    }
                    if current_length >= self.total_length {
                        let remaining_buffer = self.buffer.split_off(self.total_length);
                        // send event(response) to reporter
                        let event = reporter::Event::Response {
                            giveload: self.buffer,
                            stream_id: self.stream_id,
                        };
                        let reporter_tx = self
                            .reporters
                            .get(&compute_reporter_num(self.stream_id, self.appends_num))
                            .unwrap();
                        let _ = reporter_tx.send(event);
                        // decrease total_length from current_length
                        current_length -= self.total_length;
                        // reset total_length to zero
                        self.total_length = 0;
                        // reset i to new current_length
                        self.i = current_length;
                        // process any events in the remaining_buffer.
                        self.buffer = process_remaining(
                            remaining_buffer,
                            &mut self.stream_id,
                            &mut self.total_length,
                            &mut self.header,
                            &mut self.i,
                            &self.appends_num,
                            &self.reporters,
                        );
                    } else {
                        self.i = current_length; // update i to n.
                        self.header = true; // as now we got the frame-header including knowing its body-length
                    }
                }
            } else {
                // breaking the while loop as the received n == 0.
                break;
            }
        }
        // clean shutdown
        for (_, reporter_tx) in &self.reporters {
            let _ = reporter_tx.send(reporter::Event::Session(reporter::Session::CheckPoint(
                self.session_id,
            )));
        }
    }
}

// private functions
fn process_remaining(
    mut buffer: Vec<u8>,
    stream_id: &mut reporter::Stream,
    total_length: &mut usize,
    header: &mut bool,
    current_length: &mut usize,
    appends_num: &i16,
    reporters: &Reporters,
) -> Vec<u8> {
    // first check if current_length hold header at least
    if *current_length >= HEADER_LENGTH {
        // decode and update total_length
        *total_length = get_total_length_usize(&buffer);
        // decode and update stream_id
        *stream_id = get_stream_id(&buffer);
        // check if current_length
        if *current_length >= *total_length {
            let remaining_buffer = buffer.split_off(*total_length);
            let event = reporter::Event::Response {
                giveload: buffer,
                stream_id: *stream_id,
            };
            let reporter_tx = reporters
                .get(&compute_reporter_num(*stream_id, *appends_num))
                .unwrap();
            let _ = reporter_tx.send(event);
            // reset before loop
            *current_length -= *total_length;
            process_remaining(
                remaining_buffer,
                stream_id,
                total_length,
                header,
                current_length,
                appends_num,
                reporters,
            )
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

fn get_stream_id(buffer: &[u8]) -> reporter::Stream {
    ((buffer[2] as reporter::Stream) << 8) | buffer[3] as reporter::Stream
}

fn compute_reporter_num(stream_id: Stream, appends_num: i16) -> u8 {
    (stream_id / appends_num) as u8
}
