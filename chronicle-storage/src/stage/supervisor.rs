use super::{
    receiver,
    reporter,
    sender,
};
use crate::{
    connection::cql::connect_to_shard_id,
    node,
    stage::reporter::{
        Stream,
        Streams,
    },
};
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::delay_for,
};

pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type Reporters = HashMap<u8, mpsc::UnboundedSender<reporter::Event>>;
pub type Payloads = Arc<Vec<Reusable>>;

#[derive(Debug)]
pub enum Event {
    Connect(sender::Sender, sender::Receiver),
    Reconnect(usize),
    Shutdown,
}

actor!(SupervisorBuilder {
    address: String,
    reporter_count: u8,
    shard_id: u8,
    tx: Sender,
    rx: Receiver,
    node_tx: node::supervisor::Sender,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>
});

impl SupervisorBuilder {
    pub fn build(self) -> Supervisor {
        // create reusable payloads as giveload
        let vector: Vec<Reusable> = Vec::new();
        let payloads: Payloads = Arc::new(vector);
        Supervisor {
            session_id: 0,
            reporters: HashMap::with_capacity(self.reporter_count.unwrap() as usize),
            reconnect_requests: 0,
            connected: false,
            address: self.address.unwrap(),
            shard_id: self.shard_id.unwrap(),
            tx: self.tx,
            rx: self.rx.unwrap(),
            node_tx: self.node_tx.unwrap(),
            payloads: payloads,
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size.unwrap(),
            send_buffer_size: self.send_buffer_size.unwrap(),
        }
    }
}
#[derive(Default)]
pub struct Reusable {
    value: UnsafeCell<Option<sender::Payload>>,
}
impl Reusable {
    pub fn as_mut(&self) -> &mut Option<sender::Payload> {
        unsafe { self.value.get().as_mut().unwrap() }
    }
    pub fn as_ref_payload(&self) -> Option<&sender::Payload> {
        unsafe { self.value.get().as_ref().unwrap().as_ref() }
    }
    pub fn as_mut_payload(&self) -> Option<&mut sender::Payload> {
        unsafe { self.value.get().as_mut().unwrap().as_mut() }
    }
}
unsafe impl Sync for Reusable {}

pub struct Supervisor {
    session_id: usize,
    reconnect_requests: u8,
    connected: bool,
    address: String,
    shard_id: u8,
    tx: Option<Sender>,
    rx: Receiver,
    node_tx: node::supervisor::Sender,
    reporters: Reporters,
    payloads: Payloads,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
}

impl Supervisor {
    pub async fn run(mut self) {
        let reporters_num = self.reporters.capacity() as u8;
        // Create sender's channel
        let (sender_tx, sender_rx) = mpsc::unbounded_channel::<Stream>();
        // Prepare range to later create stream_ids vector per reporter
        let (mut start_range, appends_num): (Stream, Stream) = (0, 32767 / (reporters_num as i16));
        // init Reusable payloads holder to enable reporter/sender/receiver
        // to reuse the payload whenever is possible.
        {
            let last_range = appends_num * (reporters_num as i16);
            let payloads = Arc::get_mut(&mut self.payloads).unwrap();
            for _ in 0..last_range {
                payloads.push(Reusable::default())
            }
        }
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
                .reporter_id(reporter_num)
                .session_id(self.session_id)
                .streams(streams)
                .shard_id(self.shard_id.clone())
                .address(self.address.clone())
                .tx(reporter_tx)
                .rx(reporter_rx)
                .stage_tx(self.tx.as_ref().unwrap().clone());
            let reporter = reporter_builder.build();
            tokio::spawn(reporter.run());
        }
        // expose stage reporters in advance
        let event = node::supervisor::Event::RegisterReporters(self.shard_id, self.reporters.clone());
        self.node_tx.send(event).unwrap();
        // todo improve dbg! msgs
        dbg!(
            "just exposed stage reporters of shard: {}, to node supervisor",
            self.shard_id
        );
        // Send self event::connect
        self.tx
            .as_ref()
            .unwrap()
            .send(Event::Connect(sender_tx, sender_rx))
            .unwrap();
        // Supervisor event loop
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Connect(sender_tx, sender_rx) => {
                    // Only try to connect if the stage not shutting_down
                    if self.tx.is_some() {
                        match connect_to_shard_id(
                            &self.address,
                            self.shard_id,
                            self.recv_buffer_size,
                            self.send_buffer_size,
                        )
                        .await
                        {
                            Ok(mut cqlconn) => {
                                // Change the connected status to true
                                self.connected = true;
                                // TODO convert the session_id to a meaningful (timestamp + count)
                                self.session_id += 1;
                                // Split the stream
                                let (socket_rx, socket_tx) = tokio::io::split(cqlconn.take_stream());
                                // Spawn/restart sender
                                let sender_state = sender::SenderBuilder::new()
                                    .tx(sender_tx)
                                    .rx(sender_rx)
                                    .session_id(self.session_id)
                                    .socket_tx(socket_tx)
                                    .reporters(self.reporters.clone())
                                    .payloads(self.payloads.clone())
                                    .build();
                                tokio::spawn(sender_state.run());
                                // Spawn/restart receiver
                                let receiver = receiver::ReceiverBuilder::new()
                                    .socket_rx(socket_rx)
                                    .reporters(self.reporters.clone())
                                    .session_id(self.session_id)
                                    .payloads(self.payloads.clone())
                                    .buffer_size(self.buffer_size)
                                    .build();
                                tokio::spawn(receiver.run());
                            }
                            Err(err) => {
                                // TODO erro handling
                                dbg!("trying to connect every 5 seconds: err {}", err);
                                delay_for(Duration::from_millis(5000)).await;
                                // Try again to connect
                                if let Some(tx) = &self.tx {
                                    tx.send(Event::Connect(sender_tx, sender_rx)).unwrap();
                                };
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
                    let (sender_tx, sender_rx) = mpsc::unbounded_channel::<Stream>();
                    if let Some(tx) = &self.tx {
                        tx.send(Event::Connect(sender_tx, sender_rx)).unwrap();
                    };
                }
                Event::Shutdown => {
                    // drop self.tx by setting it to None.
                    self.tx = None;
                    // therefore now we tell reporters to gracefully shutdown by droping
                    // sender_tx and eventaully stage_tx, draining reporters is important
                    // otherwise we have to close the rx channel.
                    for (_, reporter_tx) in self.reporters.drain() {
                        reporter_tx
                            .send(reporter::Event::Session(reporter::Session::Shutdown))
                            .unwrap();
                    }
                }
            }
        }
    }
}
