// uses
use super::{
    reporter,
    reporter::Stream,
    supervisor,
};
use chronicle_common::actor;
use tokio::{
    io::ReadHalf,
    net::TcpStream,
    prelude::*,
};
// consts
const HEADER_LENGTH: usize = 9;

actor!(
    ReceiverBuilder {
        reporters: supervisor::Reporters,
        socket_rx: ReadHalf<TcpStream>,
        session_id: usize,
        payloads: supervisor::Payloads,
        buffer_size: usize
});

impl ReceiverBuilder {
    pub fn build(self) -> Receiver {
        let reporters = self.reporters.unwrap();
        let reporters_len = reporters.len();
        let buffer_size = self.buffer_size.unwrap();
        Receiver {
            reporters,
            socket: self.socket_rx.unwrap(),
            stream_id: 0,
            total_length: 0,
            current_length: 0,
            header: false,
            buffer: vec![0; buffer_size],
            i: 0,
            session_id: self.session_id.unwrap(),
            appends_num: 32767 / reporters_len as i16,
            payloads: self.payloads.unwrap(),
        }
    }
}

// suerpvisor state struct
pub struct Receiver {
    reporters: supervisor::Reporters,
    socket: ReadHalf<TcpStream>,
    stream_id: reporter::Stream,
    total_length: usize,
    current_length: usize,
    header: bool,
    buffer: Vec<u8>,
    i: usize,
    session_id: usize,
    appends_num: i16,
    payloads: supervisor::Payloads,
}

impl Receiver {
    pub async fn run(mut self) {
        while let Ok(n) = self.socket.read(&mut self.buffer[self.i..]).await {
            if n != 0 {
                self.current_length += n;
                if self.current_length < HEADER_LENGTH {
                    self.i = self.current_length;
                } else {
                    self.handle_frame_header(0);
                    self.handle_frame(n, 0);
                }
            } else {
                break;
            }
        }
        // clean shutdown
        for (_, reporter_tx) in &self.reporters {
            let _ = reporter_tx.send(reporter::Event::Session(reporter::Session::CheckPoint(self.session_id)));
        }
    }
    fn handle_remaining_buffer(&mut self, i: usize, end: usize) {
        if self.current_length < HEADER_LENGTH {
            for index in i..end {
                self.buffer[self.i] = self.buffer[index];
                self.i += 1;
            }
        } else {
            self.handle_frame_header(i);
            self.handle_frame(self.current_length, i);
        }
    }
    fn handle_frame_header(&mut self, padding: usize) {
        // if no-header decode the header and resize the payload(if needed).
        if !self.header {
            // decode total_length(HEADER_LENGTH + frame_body_length)
            self.total_length = get_total_length_usize(&self.buffer[padding..]);
            // decode stream_id
            self.stream_id = get_stream_id(&self.buffer[padding..]);
            // get mut ref to payload for stream_id
            let payload = self.payloads[self.stream_id as usize].as_mut_payload().unwrap();
            // resize payload only if total_length is larger than the payload length
            if self.total_length > payload.len() {
                // resize the len of the payload.
                payload.resize(self.total_length, 0);
            }
            // set self.i to zero
            self.i = 0;
            // set header to true
            self.header = true;
        }
    }
    #[allow(unused_must_use)]
    fn handle_frame(&mut self, n: usize, mut padding: usize) {
        if self.current_length >= self.total_length {
            // get mut ref to payload for stream_id as giveload
            let giveload = self.payloads[self.stream_id as usize].as_mut_payload().unwrap();
            // memcpy the current bytes from self.buffer into payload
            let start = self.current_length - n;
            giveload[start..self.total_length]
                .copy_from_slice(&self.buffer[padding..(padding + self.total_length - start)]);
            // tell reporter that giveload is ready.
            self.reporters
                .get(&compute_reporter_num(self.stream_id, self.appends_num))
                .unwrap()
                .send(reporter::Event::Response {
                    stream_id: self.stream_id,
                });
            // set header to false
            self.header = false;
            // update current_length
            self.current_length -= self.total_length;
            // update padding
            padding = padding + self.total_length;
            self.handle_remaining_buffer(padding, padding + self.current_length);
        } else {
            // get mut ref to payload for stream_id
            let payload = self.payloads[self.stream_id as usize].as_mut_payload().unwrap();
            // memcpy the current bytes from self.buffer into payload
            payload[(self.current_length - n)..self.current_length]
                .copy_from_slice(&self.buffer[padding..(padding + n)]);
        }
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

pub fn compute_reporter_num(stream_id: Stream, appends_num: i16) -> u8 {
    (stream_id / appends_num) as u8
}
