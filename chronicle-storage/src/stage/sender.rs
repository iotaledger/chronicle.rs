// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! The ScyllaDB stage-level sender.

// uses
use super::{receiver::compute_reporter_num, reporter, supervisor};
use crate::stage::reporter::Stream;
use chronicle_common::actor;
use tokio::{io::WriteHalf, net::TcpStream, prelude::*, sync::mpsc};
// types
pub type Sender = mpsc::UnboundedSender<Stream>;
pub type Receiver = mpsc::UnboundedReceiver<Stream>;
// Payload type is vector<unsigned-integer-8bit>.
pub type Payload = Vec<u8>;

actor!(
    SenderBuilder {
        tx: Sender,
        rx: Receiver,
        socket_tx: WriteHalf<TcpStream>,
        reporters: supervisor::Reporters,
        session_id: usize,
        payloads: supervisor::Payloads
});

impl SenderBuilder {
    pub fn build(self) -> SenderState {
        // pass sender_tx to reporters
        for reporter_tx in self.reporters.as_ref().unwrap().values() {
            reporter_tx
                .send(reporter::Event::Session(reporter::Session::New(
                    *self.session_id.as_ref().unwrap(),
                    self.tx.as_ref().unwrap().clone(),
                )))
                .unwrap();
        }
        let reporters = self.reporters.unwrap();
        let reporters_len = reporters.len();
        SenderState {
            reporters,
            session_id: self.session_id.unwrap(),
            socket: self.socket_tx.unwrap(),
            rx: self.rx.unwrap(),
            payloads: self.payloads.unwrap(),
            appends_num: 32767 / reporters_len as i16,
        }
    }
}

// sender's state struct.
pub struct SenderState {
    reporters: supervisor::Reporters,
    session_id: usize,
    socket: WriteHalf<TcpStream>,
    rx: Receiver,
    payloads: supervisor::Payloads,
    appends_num: i16,
}

impl SenderState {
    pub async fn run(mut self) {
        // loop to process event by event.
        while let Some(stream) = self.rx.recv().await {
            // write the payload to the socket, make sure the result is valid
            if let Err(io_error) = self
                .socket
                .write_all(self.payloads[stream as usize].as_ref_payload().unwrap())
                .await
            {
                // send to reporter send_status::Err(stream_id)
                self.reporters
                    .get(&compute_reporter_num(stream, self.appends_num))
                    .unwrap()
                    .send(reporter::Event::Err(io_error, stream))
                    .unwrap();
            }
        } // if sender reached this line, then either write_all returned IO Err(err) or reporter(s) droped sender_tx(s)
          // probably not needed
        let _ = self.socket.shutdown().await;
        // send checkpoint to all reporters because the socket is mostly closed
        for reporter_tx in self.reporters.values() {
            reporter_tx
                .send(reporter::Event::Session(reporter::Session::CheckPoint(self.session_id)))
                .unwrap();
        }
    }
}
