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

//! The ScyllaDB stage-level supervisor, which is the top-level module in stage that initializes the reporter,
//! sender, and receiver.

use super::{receiver, reporter, sender};
use crate::{
    connection::cql::connect_to_shard_id,
    node,
    stage::reporter::{Stream, Streams},
};
use chronicle_cql::frame::auth_response::PasswordAuth;

use chronicle_common::actor;
use log::*;
use std::{cell::UnsafeCell, collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::delay_for};
/// The sender of stage supervisor event.
pub type Sender = mpsc::UnboundedSender<Event>;
/// The receiver of stage supervisor event.
pub type Receiver = mpsc::UnboundedReceiver<Event>;
/// The reporters of shard id to its corresponding sender of stage reporter events.
pub type Reporters = HashMap<u8, mpsc::UnboundedSender<reporter::Event>>;
/// The thread-safe reusable sender payloads.
pub type Payloads = Arc<Vec<Reusable>>;

#[derive(Debug)]
/// Stage supervisor event.
pub enum Event {
    /// Connect to a shard.
    Connect(sender::Sender, sender::Receiver),
    /// Reconnection request.
    Reconnect(usize),
    /// Shutdwon a stage.
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
    send_buffer_size: Option<usize>,
    authenticator: Option<PasswordAuth>
});

impl SupervisorBuilder {
    /// Build the stage supervisor
    pub fn build(self) -> Supervisor {
        // create reusable payloads as giveload
        let vector: Vec<Reusable> = Vec::new();
        let payloads: Payloads = Arc::new(vector);
        Supervisor {
            session_id: 0,
            reporters: HashMap::with_capacity(self.reporter_count.unwrap() as usize),
            reporters_count: self.reporter_count.unwrap(),
            reconnect_requests: 0,
            connected: false,
            address: self.address.unwrap(),
            shard_id: self.shard_id.unwrap(),
            tx: self.tx,
            rx: self.rx.unwrap(),
            node_tx: self.node_tx.unwrap(),
            payloads,
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size.unwrap(),
            send_buffer_size: self.send_buffer_size.unwrap(),
            authenticator: self.authenticator.unwrap(),
        }
    }
}
#[derive(Default)]
/// The reusable sender payload.
pub struct Reusable {
    value: UnsafeCell<Option<sender::Payload>>,
}
impl Reusable {
    #[allow(clippy::mut_from_ref)]
    /// Return as mutable sender payload value.
    pub fn as_mut(&self) -> &mut Option<sender::Payload> {
        unsafe { self.value.get().as_mut().unwrap() }
    }
    /// Return as reference sender payload.
    pub fn as_ref_payload(&self) -> Option<&sender::Payload> {
        unsafe { self.value.get().as_ref().unwrap().as_ref() }
    }
    /// Return as mutable sender payload.
    pub fn as_mut_payload(&self) -> Option<&mut sender::Payload> {
        self.as_mut().as_mut()
    }
}
unsafe impl Sync for Reusable {}

/// The stage supervisor structure.
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
    reporters_count: u8,
    payloads: Payloads,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    authenticator: Option<PasswordAuth>,
}

impl Supervisor {
    /// Start to run the stage supervisor event loop.
    pub async fn run(mut self) {
        let reporters_num = self.reporters_count;
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
                .payloads(self.payloads.clone())
                .tx(reporter_tx)
                .rx(reporter_rx)
                .stage_tx(self.tx.as_ref().unwrap().clone());
            let reporter = reporter_builder.build();
            tokio::spawn(reporter.run());
        }
        // expose stage reporters in advance
        let event = node::supervisor::Event::RegisterReporters(self.shard_id, self.reporters.clone());
        self.node_tx.send(event).unwrap();
        info!(
            "Exposed stage reporters of shard: {}, to node: {} supervisor",
            self.shard_id, self.address
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
                            self.authenticator.as_ref(),
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
                                warn!("trying to connect every 5 seconds: err {}", err);
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
                    // so in this scope we only count the reconnect_requests up to reporters_num-1, which means only one
                    // is left behind.
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
