// uses
use super::reporter;
use super::supervisor;
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
        stream_id: reporter::StreamId,
        payload: Payload,
        reporter: mpsc::UnboundedSender<reporter::Event>,
    },
}

// args struct, each actor must have public Arguments struct,
// to pass options when starting the actor.
pub struct SenderBuilder {
    // sender's tx only to pass it to reporters (if recoonect == true).
    tx: Option<Sender>,
    // sender's rx to recv events
    rx: Option<Receiver>,
    supervisor_tx: Option<supervisor::Sender>,
    socket_tx: Option<WriteHalf<TcpStream>>,
    reporters: Option<supervisor::Reporters>,
    session_id: Option<usize>,
    reconnect: bool,
}

impl SenderBuilder {
    pub fn new() -> Self {
        SenderBuilder {
            tx: None,
            rx: None,
            supervisor_tx: None,
            socket_tx: None,
            reporters: None,
            session_id: None,
            reconnect: false,
        }
    }

    set_builder_option_field!(tx, Sender);
    set_builder_option_field!(rx, Receiver);
    set_builder_option_field!(supervisor_tx, supervisor::Sender);
    set_builder_option_field!(socket_tx, WriteHalf<TcpStream>);
    set_builder_option_field!(reporters, supervisor::Reporters);
    set_builder_option_field!(session_id, usize);
    set_builder_field!(reconnect, bool);

    pub fn build(self) -> SenderState {
        let state = SenderState {
            supervisor_tx: self.supervisor_tx.unwrap(),
            reporters: self.reporters.unwrap(),
            session_id: self.session_id.unwrap(),
            socket: self.socket_tx.unwrap(),
            tx: self.tx.unwrap(),
            rx: self.rx.unwrap(),
        };

        if self.reconnect {
            for (_, reporter_tx) in &state.reporters {
                reporter_tx
                    .send(reporter::Event::Session(reporter::Session::New(
                        state.session_id,
                        state.tx.clone(),
                    )))
                    .unwrap();
            }
        }

        state
    }
}

// sender's state struct.
pub struct SenderState {
    supervisor_tx: supervisor::Sender,
    reporters: supervisor::Reporters,
    session_id: usize,
    // the socket_writehalf side to that given shard
    socket: WriteHalf<TcpStream>,
    tx: Sender,
    rx: Receiver,
}

impl SenderState {
    pub async fn run(mut self) {
        // loop to process event by event.
        while let Some(Event::Payload {
            stream_id,
            payload,
            reporter,
        }) = self.rx.recv().await
        {
            // write the payload to the socket, make sure the result is valid
            match self.socket.write_all(&payload).await {
                Ok(()) => {
                    // send to reporter send_status::Ok(stream_id)
                    reporter
                        .send(reporter::Event::SendStatus(reporter::SendStatus::Ok(
                            stream_id,
                        )))
                        .unwrap();
                }
                Err(_) => {
                    // send to reporter send_status::Err(stream_id)
                    reporter
                        .send(reporter::Event::SendStatus(reporter::SendStatus::Err(
                            stream_id,
                        )))
                        .unwrap();
                    // close channel to prevent any further Payloads to be sent from reporters
                    self.rx.close();
                }
            }
        } // if sender reached this line, then either write_all returned IO Err(err) or reporter(s) droped sender_tx(s)

        // probably not needed
        self.socket.shutdown().await;

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
