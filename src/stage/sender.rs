// uses
use super::reporter;
use super::supervisor;
use crate::worker::StreamStatus;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::mpsc;

// types
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
// Payload type is vector<unsigned-integer-8bit>.
pub type Payload = Vec<u8>;

#[derive(Debug)]
pub enum Event {
    Payload {
        stream: reporter::Stream,
        payload: Payload,
        reporter_id: u8,
    },
}

actor!(
    SenderBuilder {
        tx: Sender,
        rx: Receiver,
        socket_tx: WriteHalf<TcpStream>,
        reporters: supervisor::Reporters,
        session_id: usize
});

impl SenderBuilder {

    pub fn build(self) -> SenderState {
        // pass sender_tx to reporters
        for (_, reporter_tx) in self.reporters.as_ref().unwrap() {
            reporter_tx
                .send(reporter::Event::Session(reporter::Session::New(
                    self.session_id.as_ref().unwrap().clone(),
                    self.tx.as_ref().unwrap().clone(),
                )))
                .unwrap();
        }
        SenderState {
            reporters: self.reporters.unwrap(),
            session_id: self.session_id.unwrap(),
            socket: self.socket_tx.unwrap(),
            rx: self.rx.unwrap(),
        }
    }
}

// sender's state struct.
pub struct SenderState {
    reporters: supervisor::Reporters,
    session_id: usize,
    socket: WriteHalf<TcpStream>,
    rx: Receiver,
}

impl SenderState {
    pub async fn run(mut self) {
        // loop to process event by event.
        while let Some(Event::Payload {
            stream,
            payload,
            reporter_id,
        }) = self.rx.recv().await
        {
            // write the payload to the socket, make sure the result is valid
            match self.socket.write_all(&payload).await {
                Ok(()) => {
                    // send to reporter send_status::Ok(stream_id)
                    self.reporters
                        .get(&reporter_id)
                        .unwrap()
                        .send(reporter::Event::StreamStatus(StreamStatus::Ok(stream)))
                        .unwrap();
                }
                Err(_) => {
                    // send to reporter send_status::Err(stream_id)
                    self.reporters
                        .get(&reporter_id)
                        .unwrap()
                        .send(reporter::Event::StreamStatus(StreamStatus::Err(stream)))
                        .unwrap();
                    // close channel to prevent any further Payloads to be sent from reporters
                    self.rx.close();
                }
            }
        } // if sender reached this line, then either write_all returned IO Err(err) or reporter(s) droped sender_tx(s)

        // probably not needed
        self.socket.shutdown().await.unwrap();

        // send checkpoint to all reporters because the socket is mostly closed
        for (_, reporter_tx) in &self.reporters {
            reporter_tx
                .send(reporter::Event::Session(reporter::Session::CheckPoint(
                    self.session_id,
                )))
                .unwrap();
        }
    }
}
