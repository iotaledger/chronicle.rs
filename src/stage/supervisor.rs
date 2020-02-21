use super::receiver;
use super::reporter;
use super::sender;
use crate::node;
use crate::stage::reporter::{Stream, Streams};
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::delay_for;

pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type Reporters = HashMap<u8, mpsc::UnboundedSender<reporter::Event>>;

#[derive(Debug)]
pub enum Event {
    // Sender, Receiver, Reconnect
    Connect(sender::Sender, sender::Receiver, bool),
    Reconnect(usize),
    Shutdown,
}

pub struct SupervisorBuilder {
    address: Option<String>,
    reporters: u8,
    shard: u8,
    tx: Option<Sender>,
    rx: Option<Receiver>,
    supervisor_tx: Option<node::supervisor::Sender>,
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            address: None,
            reporters: 1,
            shard: 1,
            tx: None,
            rx: None,
            supervisor_tx: None,
        }
    }

    set_builder_option_field!(address, String);
    set_builder_field!(reporters, u8);
    set_builder_field!(shard, u8);
    set_builder_option_field!(tx, Sender);
    set_builder_option_field!(rx, Receiver);
    set_builder_option_field!(supervisor_tx, node::supervisor::Sender);

    pub fn build(self) -> Supervisor {
        Supervisor {
            session: 0,
            // Generate vector with capcity of reporters number
            reporters: HashMap::with_capacity(self.reporters as usize),
            reconnect_requests: 0,
            connected: false,
            shutting_down: false,
            address: self.address.unwrap(),
            shards: self.shard,
            tx: self.tx.unwrap(),
            rx: self.rx.unwrap(),
            supervisor_tx: self.supervisor_tx.unwrap(),
        }
    }
}

pub struct Supervisor {
    session: usize,
    reconnect_requests: u8,
    connected: bool,
    shutting_down: bool,
    address: String,
    shards: u8,
    tx: Sender,
    rx: Receiver,
    supervisor_tx: node::supervisor::Sender,
    reporters: Reporters,
}

impl Supervisor {
    pub async fn run(mut self) {
        let reporters_num = self.reporters.capacity() as u8;
        // Create sender's channel
        let (sender_tx, sender_rx) = mpsc::unbounded_channel::<sender::Event>();
        // Prepare range to later create stream_ids vector per reporter
        let (mut start_range, appends_num): (Stream, Stream) =
            (0, 32767 / (reporters_num as i16));
        // Start reporters
        for reporter_num in 0..reporters_num {
            // Create reporter's channel
            let (reporter_tx, reporter_rx) = mpsc::unbounded_channel::<reporter::Event>();
            // Add reporter to reporters map
            self.reporters.insert(reporter_num, reporter_tx.clone());
            // Start reporter
            let last_range = start_range + appends_num;
            let streams: Streams = ((if reporter_num == 0 {
                1 // we force first reporter_num to start range from 1, as we reversing stream_id=0 for future uses.
            } else {
                start_range // otherwise we keep the start_range as it's
            })..last_range)
                .collect();
            start_range = last_range;
            let reporter_builder = reporter::ReporterBuilder::new()
                .reporter(reporter_num)
                .session(self.session)
                .streams(streams)
                .shard(self.shards.clone())
                .address(self.address.clone())
                .tx(reporter_tx)
                .rx(reporter_rx)
                .supervisor_tx(self.tx.clone())
                .sender_tx(sender_tx.clone());
            let reporter = reporter_builder.build();
            tokio::spawn(reporter.run());
        }

        // Send self event::connect
        // false because they already have the sender_tx and no need to reconnect
        self.tx
            .send(Event::Connect(sender_tx, sender_rx, false))
            .unwrap();
        // Supervisor event loop
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Connect(sender_tx, sender_rx, reconnect) => {
                    // Only try to connect if the stage not shutting_down
                    if !self.shutting_down {
                        match TcpStream::connect(self.address.clone()).await {
                            Ok(stream) => {
                                // Change the connected status to true
                                self.connected = true;
                                // TODO convert the session_id to a meaningful (timestamp + count)
                                self.session += 1;
                                // Split the stream
                                let (socket_rx, socket_tx) = tokio::io::split(stream);
                                // Spawn/restart sender
                                let sender_state = sender::SenderBuilder::new()
                                    .reconnect(reconnect)
                                    .tx(sender_tx)
                                    .rx(sender_rx)
                                    .session_id(self.session)
                                    .socket_tx(socket_tx)
                                    .reporters(self.reporters.clone())
                                    .supervisor_tx(self.tx.clone())
                                    .build();
                                tokio::spawn(sender_state.run());
                                // Spawn/restart receiver
                                let receiver = receiver::ReceiverBuidler::new()
                                    .socket_rx(socket_rx)
                                    .reporters(self.reporters.clone())
                                    .supervisor_tx(self.tx.clone())
                                    .session_id(self.session)
                                    .build();
                                tokio::spawn(receiver.run());
                                if !reconnect {
                                    // TODO now reporters are ready to be exposed to workers.. (ex evmap ring.)
                                    // create key which could be address:shard (ex "127.0.0.1:9042:5")
                                    let event = node::supervisor::Event::Expose(
                                        self.shards,
                                        self.reporters.clone(),
                                    );
                                    self.supervisor_tx.send(event).unwrap();
                                    dbg!("just exposed stage reporters of shard: {}, to node supervisor", self.shards);
                                }
                            }
                            Err(err) => {
                                // TODO erro handling
                                dbg!("trying to connect every 5 seconds: err {}", err);
                                delay_for(Duration::from_millis(5000)).await;
                                // Try again to connect
                                self.tx
                                    .send(Event::Connect(sender_tx, sender_rx, reconnect))
                                    .unwrap();
                            }
                        }
                    }
                }
                Event::Reconnect(_) if self.reconnect_requests != reporters_num - 1 => {
                    // supervisor requires reconnect_requests from all its reporters in order to reconnect.
                    // so in this scope we only count the reconnect_requests up to reporters_num-1, which means only one is left behind.
                    self.reconnect_requests += 1;
                }
                Event::Reconnect(_) => {
                    // the last reconnect_request from last reporter,
                    // reset reconnect_requests to zero
                    self.reconnect_requests = 0;
                    // change the connected status
                    self.connected = false;
                    // Create sender's channel
                    let (sender_tx, sender_rx) = mpsc::unbounded_channel::<sender::Event>();
                    self.tx
                        .send(Event::Connect(sender_tx, sender_rx, true))
                        .unwrap();
                }
                Event::Shutdown => {
                    self.shutting_down = true;
                    // this will make sure both sender and receiver of the stage are dead.
                    if !self.connected {
                        // therefore now we tell reporters to gracefully shutdown
                        for (_, reporter_tx) in self.reporters.drain() {
                            reporter_tx
                                .send(reporter::Event::Session(reporter::Session::Shutdown))
                                .unwrap();
                        }
                        // finally close rx channel
                        self.rx.close();
                    } else {
                        // wait for 5 second
                        delay_for(Duration::from_secs(5)).await;
                        // trap self with shutdown event.
                        self.tx.send(Event::Shutdown).unwrap();
                    }
                }
            }
        }
    }
}
