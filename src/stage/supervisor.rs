use super::receiver;
use super::reporter;
use super::sender;
use crate::cluster::supervisor::Address;
use crate::node;
use crate::stage::reporter::{StreamId, StreamIds};
use crate::node::supervisor::StageNum;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::delay_for;

pub type ReporterNum = u8;
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type Reporters = HashMap<ReporterNum, mpsc::UnboundedSender<reporter::Event>>;

#[derive(Debug)]
pub enum Event {
    // Sender, Receiver, Reconnect
    Connect(sender::Sender, sender::Receiver, bool),
    Reconnect(usize),
    Shutdown,
}

pub struct SupervisorBuilder {
    address: Option<Address>,
    reporters_num: Option<ReporterNum>,
    shard: Option<u8>,
    tx: Option<Sender>,
    rx: Option<Receiver>,
    supervisor_tx: Option<node::supervisor::Sender>,
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            address: None,
            reporters_num: None,
            shard: None,
            tx: None,
            rx: None,
            supervisor_tx: None,
        }
    }

    set_builder_option_field!(address, Address);
    set_builder_option_field!(reporters_num, ReporterNum);
    set_builder_option_field!(shard, u8);
    set_builder_option_field!(tx, Sender);
    set_builder_option_field!(rx, Receiver);
    set_builder_option_field!(supervisor_tx, node::supervisor::Sender);

    pub fn build(self) -> Supervisor {
        Supervisor {
            session_id: 0,
            // generate vector with capcity of reporters_num
            reporters: HashMap::with_capacity(self.reporters_num.unwrap() as usize),
            reconnect_requests: 0,
            connected: false,
            shutting_down: false,
            address: self.address.unwrap(),
            reporters_num: self.reporters_num.unwrap(),
            shard: self.shard.unwrap(),
            tx: self.tx.unwrap(),
            rx: self.rx.unwrap(),
            supervisor_tx: self.supervisor_tx.unwrap(),
        }
    }
}

pub struct Supervisor {
    session_id: usize,
    reporters: Reporters,
    reconnect_requests: u8,
    tx: Sender,
    rx: Receiver,
    connected: bool,
    shutting_down: bool,
    address: Address,
    shard: StageNum,
    reporters_num: u8,
    supervisor_tx: node::supervisor::Sender,
}

impl Supervisor {
    pub async fn run(&mut self) {
        // Create sender's channel
        let (sender_tx, sender_rx) = mpsc::unbounded_channel::<sender::Event>();
        // Prepare range to later create stream_ids vector per reporter
        let (mut start_range, appends_num): (StreamId, StreamId) = (0,32767/(self.reporters_num as i16));
        // Start reporters
        for reporter_num in 0..self.reporters_num {
            // Create reporter's channel
            let (reporter_tx, reporter_rx) = mpsc::unbounded_channel::<reporter::Event>();
            // Add reporter to reporters map
            self.reporters.insert(reporter_num, reporter_tx.clone());
            // Start reporter
            let reporter_builder = if reporter_num != self.reporters_num - 1 {
                let last_range = start_range + appends_num;
                let stream_ids: StreamIds = (start_range..last_range).collect();
                start_range = last_range;
                reporter::ReporterBuilder::new()
                    .reporter_num(reporter_num)
                    .session_id(self.session_id)
                    .sender_tx(sender_tx.clone())
                    .supervisor_tx(self.tx.clone())
                    .stream_ids(stream_ids)
                    .tx(reporter_tx)
                    .rx(reporter_rx)
                    .shard(self.shard.clone())
                    .address(self.address.clone())
            } else {
                let stream_ids: StreamIds = (start_range..32767).collect();
                reporter::ReporterBuilder::new()
                    .reporter_num(reporter_num)
                    .session_id(self.session_id)
                    .sender_tx(sender_tx.clone())
                    .supervisor_tx(self.tx.clone())
                    .stream_ids(stream_ids)
                    .tx(reporter_tx)
                    .rx(reporter_rx)
                    .shard(self.shard.clone())
                    .address(self.address.clone())
            };
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
                                self.session_id += 1;
                                // Split the stream
                                let (socket_rx, socket_tx) = tokio::io::split(stream);
                                // Spawn/restart sender
                                let sender_state = sender::SenderBuilder::new()
                                    .reconnect(reconnect)
                                    .tx(sender_tx)
                                    .rx(sender_rx)
                                    .session_id(self.session_id)
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
                                    .session_id(self.session_id)
                                    .build();
                                tokio::spawn(receiver.run());
                                if !reconnect {
                                    // TODO now reporters are ready to be exposed to workers.. (ex evmap ring.)
                                    // create key which could be address:shard (ex "127.0.0.1:9042:5")
                                    let event = node::supervisor::Event::Expose(
                                        self.shard,
                                        self.reporters.clone(),
                                    );
                                    self.supervisor_tx.send(event).unwrap();
                                    println!("just exposed stage reporters of shard: {}, to node supervisor", self.shard);
                                }
                            }
                            Err(err) => {
                                // TODO erro handling
                                println!("trying to connect every 5 seconds: err {}", err);
                                delay_for(Duration::from_millis(5000)).await;
                                // Try again to connect
                                self.tx
                                    .send(Event::Connect(sender_tx, sender_rx, reconnect))
                                    .unwrap();
                            }
                        }
                    }
                }
                Event::Reconnect(_) if self.reconnect_requests != self.reporters_num - 1 => {
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
