// uses
use super::reporter;
use super::supervisor;
use tokio::sync::mpsc;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::prelude::*;

// types
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;

#[derive(Debug)]
pub enum Event {
    Payload {
            stream_id: u16,
            payload: Vec<u8>,
            reporter: mpsc::UnboundedSender<reporter::Event>
        },
}

// args struct, each actor must have public Arguments struct,
// to pass options when starting the actor.
pub struct SenderBuilder {
    // sender's tx to send self events if needed
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

    pub fn tx(mut self, tx: Sender) -> Self {
        self.tx.replace(tx);
        self
    }

    pub fn rx(mut self, rx: Receiver) -> Self {
        self.rx.replace(rx);
        self
    }

    pub fn supervisor_tx(mut self, supervisor_tx: supervisor::Sender) -> Self {
        self.supervisor_tx.replace(supervisor_tx);
        self
    }

    pub fn socket_tx(mut self, socket_tx: WriteHalf<TcpStream>) -> Self {
        self.socket_tx.replace(socket_tx);
        self
    }

    pub fn reporters(mut self, reporters: supervisor::Reporters) -> Self {
        self.reporters.replace(reporters);
        self
    }

    pub fn session_id(mut self, session_id: usize) -> Self {
        self.session_id.replace(session_id);
        self
    }

    pub fn reconnect(mut self, reconnect: bool) -> Self {
        self.reconnect = reconnect;
        self
    }

    pub fn build(self) -> SenderState {
        let state = SenderState{
            supervisor_tx: self.supervisor_tx.unwrap(),
            reporters: self.reporters.unwrap(),
            session_id: self.session_id.unwrap(),
            socket: self.socket_tx.unwrap(),
            tx: self.tx.unwrap(),
            rx: self.rx.unwrap(),
        };

        if self.reconnect {
            for (_,reporter_tx) in &state.reporters {
                reporter_tx.send(reporter::Event::Session(reporter::Session::New(state.session_id, state.tx.clone()))).unwrap();
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
        while let Some(Event::Payload{stream_id, payload, reporter}) = self.rx.recv().await {
            // write the payload to the socket, make sure the result is valid
            match self.socket.write_all(&payload).await {
                Ok(_) => {
                    // send to reporter send_status::Ok(stream_id)
                    reporter.send(reporter::Event::SendStatus(reporter::SendStatus::Ok(stream_id))).unwrap();
                },
                Err(_) => {
                    // send to reporter send_status::Err(stream_id)
                    reporter.send(reporter::Event::SendStatus(reporter::SendStatus::Err(stream_id))).unwrap();
                    // close channel to prevent any further Payloads to be sent from reporters
                    self.rx.close();
                    // break while loop
                    break;
                },
            }
        }
        // clean shutdown, we drain the channel first TODO (Not needed, but prefered)

        // send checkpoint to all reporters because the socket is mostly closed (todo confirm)
        for (_,reporter_tx) in &self.reporters {
            reporter_tx.send(reporter::Event::Session(reporter::Session::CheckPoint(self.session_id))).unwrap();
        }
    }
}
