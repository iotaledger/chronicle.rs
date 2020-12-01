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

//! This module provides the top-level functionalities for handling events from different topics (like mqtt).

use super::mqtt;
use chronicle_common::{
    actor,
    traits::{launcher::LauncherTx, shutdown::ShutdownTx},
};
use log::*;
use std::{collections::HashMap, iter::Iterator, string::ToString};
use tokio::{
    sync::mpsc,
    time::{delay_for, Duration},
};
actor!(SupervisorBuilder {
    trytes: Option<Vec<String>>,
    conf_trytes: Option<Vec<String>>,
    max_retries: usize,
    stream_capacity: usize,
    launcher_tx: Box<dyn LauncherTx>
});

/// The broker supervisor event.
pub enum Event {
    /// Add a mqtt peer to receive events.
    AddMqtt(Peer),
    /// Reconnect the MQTT.
    Reconnect(super::mqtt::Mqtt),
    /// Shutdown the broker.
    Shutdown(Option<usize>),
}

/// Sender for broker events.
pub type Sender = mpsc::UnboundedSender<Event>;
/// Receiver for broker events.
pub type Receiver = mpsc::UnboundedReceiver<Event>;
/// Shutdown for the broker.
pub struct Shutdown(Sender);

#[allow(unused_must_use)]
impl ShutdownTx for Shutdown {
    fn shutdown(self: Box<Self>) {
        self.0.send(Event::Shutdown(None));
    }
}

#[derive(Clone)]
/// MQTT peer structure.
pub struct Peer {
    /// The `Peer` ID.
    pub id: usize,
    /// The single MQTT topic to subscribe from the Peer.
    topic: Topic,
    /// The peer address.
    pub address: String,
    /// To indicate the `Peer` is connected or not.
    pub connected: bool,
}
impl Peer {
    /// Get the `Peer` ID.
    pub fn get_id(&self) -> usize {
        self.id
    }
    /// Get the `Peer` address.
    pub fn get_address<'a>(&'a self) -> &'a str {
        &self.address
    }
    /// Get the subscribed MQTT topic.
    pub fn get_topic(&self) -> Topic {
        self.topic
    }
    /// Get the subscribed MQTT topic as `String`.
    pub fn get_topic_as_string(&self) -> String {
        self.topic.to_string()
    }
    /// Set the peer connection flag.
    pub fn set_connected(&mut self, connected: bool) {
        self.connected = connected;
    }
}

#[derive(Clone, Copy)]
/// MQTT topic to sbuscribe.
pub enum Topic {
    /// MQTT `Trytes` topic.
    Trytes,
    /// MQTT `ConfTrytes` topic.
    ConfTrytes,
}
impl ToString for Topic {
    fn to_string(&self) -> String {
        match self {
            Topic::Trytes => "trytes".to_owned(),
            Topic::ConfTrytes => "conf_trytes".to_owned(),
        }
    }
}

impl SupervisorBuilder {
    /// Build the broker supervisor.
    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let peers = HashMap::new();
        let clients = HashMap::new();
        // create peers from trytes nodes (if any)
        if let Some(mut addresses) = self.trytes.unwrap().take() {
            for address in addresses.drain(..) {
                // create and force random peer id.
                let id = gen_random_peer_id(&peers);
                // dynamically add mqtt peer
                let peer = Peer {
                    id,
                    topic: Topic::Trytes,
                    address,
                    connected: false,
                };
                let _ = tx.send(Event::AddMqtt(peer));
            }
        }
        // create peers from conf_trytes nodes (if any)
        if let Some(mut addresses) = self.conf_trytes.unwrap().take() {
            for address in addresses.drain(..) {
                // create and force random peer id.
                let id = gen_random_peer_id(&peers);
                // dynamically add mqtt peer
                let peer = Peer {
                    id,
                    topic: Topic::ConfTrytes,
                    address,
                    connected: false,
                };
                let _ = tx.send(Event::AddMqtt(peer));
            }
        }
        Supervisor {
            peers,
            clients,
            tx: Some(tx),
            rx,
            max_retries: self.max_retries.unwrap(),
            stream_capacity: self.stream_capacity.unwrap(),
            launcher_tx: self.launcher_tx.unwrap(),
            shutting_down: false,
        }
    }
}

/// The broker supervisor structure.
pub struct Supervisor {
    peers: HashMap<usize, Peer>,
    clients: HashMap<usize, paho_mqtt::AsyncClient>,
    tx: Option<Sender>,
    rx: Receiver,
    max_retries: usize,
    stream_capacity: usize,
    launcher_tx: Box<dyn LauncherTx>,
    shutting_down: bool,
}

impl Supervisor {
    /// Run the event receiving loop of broker supervisor.
    pub async fn run(mut self) {
        // register broker app with launcher
        self.launcher_tx
            .register_app("broker".to_string(), Box::new(Shutdown(self.tx.as_ref().unwrap().clone())));
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::AddMqtt(mut peer) => {
                    if !self.shutting_down {
                        // check if we already have peer with same id so we ignore
                        if let None = self.peers.get(&peer.id) {
                            // build mqtt worker
                            let mut mqtt_worker = mqtt::MqttBuilder::new()
                                .max_retries(self.max_retries)
                                .stream_capacity(self.stream_capacity)
                                .peer(peer.clone())
                                .build();
                            // create stream and connect then subscribe
                            if let Ok(stream) = mqtt_worker.init().await {
                                info!(
                                    "Added MQTT peer 'topic: {}, address: {}, id: {}'",
                                    peer.get_topic_as_string(),
                                    peer.address,
                                    peer.id
                                );
                                // set peer to connected
                                peer.set_connected(true);
                                // take client to manage the mqtt session
                                let client = mqtt_worker.client.take().unwrap();
                                // store client in our state
                                self.clients.insert(peer.id, client);
                                // insert to the peer map
                                self.peers.insert(peer.id, peer);
                                // spawn mqtt
                                tokio::spawn(mqtt_worker.run(self.tx.as_ref().unwrap().clone(), stream));
                            } else {
                                error!(
                                    "Unable to add MQTT peer 'topic: {}, address: {}, id: {}'",
                                    peer.get_topic_as_string(),
                                    peer.address,
                                    peer.id
                                );
                            }
                        } else {
                            error!("Unable to add MQTT peer because of id conflict");
                        }
                    } else {
                        error!(
                            "Unable to add MQTT peer 'topic: {}, address: {}, id: {}', as in progress of shutting down",
                            peer.get_topic_as_string(),
                            peer.address,
                            peer.id
                        );
                    }
                }
                Event::Reconnect(mut mqtt) => {
                    let peer_id = mqtt.peer.id;
                    // update peers
                    self.peers.get_mut(&peer_id).unwrap().set_connected(false);
                    // handle disconnect, first we check if the disconnect was requested by checking clients map
                    if let Some(client) = self.clients.remove(&peer_id) {
                        warn!(
                            "Reconnecting MQTT peer 'topic: {}, address: {}, id: {}'",
                            mqtt.peer.get_topic_as_string(),
                            mqtt.peer.address,
                            peer_id
                        );
                        // return client to mqtt in order to reconnect;
                        mqtt.client.replace(client);
                        // create stream and connect then subscribe
                        if let Ok(stream) = mqtt.reconnect().await {
                            info!(
                                "Reconnected MQTT peer 'topic: {}, address: {}, id: {}'",
                                mqtt.peer.get_topic_as_string(),
                                mqtt.peer.address,
                                peer_id
                            );
                            // update peers
                            self.peers.get_mut(&peer_id).unwrap().set_connected(true);
                            // take client to manage the mqtt session
                            let client = mqtt.client.take().unwrap();
                            // return client to our state
                            self.clients.insert(peer_id, client);
                            // spawn mqtt
                            tokio::spawn(mqtt.run(self.tx.as_ref().unwrap().clone(), stream));
                        } else {
                            error!(
                                "Unable to reconnect MQTT peer 'topic: {}, address: {}, id: {}', will retry every 5 seconds",
                                mqtt.peer.get_topic_as_string(),
                                mqtt.peer.address,
                                peer_id
                            );
                            // return client to our state
                            let client = mqtt.client.take().unwrap();
                            self.clients.insert(peer_id, client);
                            let _ = self.tx.as_ref().unwrap().send(Event::Reconnect(mqtt));
                            delay_for(Duration::from_secs(5)).await;
                        }
                    } else {
                        info!(
                            "Shutdown MQTT peer 'topic: {}, address: {}, id: {}'",
                            mqtt.peer.get_topic_as_string(),
                            mqtt.peer.address,
                            mqtt.peer.id
                        );
                        // remove mqtt.peer from peers
                        self.peers.remove(&mqtt.peer.id);
                        // it was requested so we make sure to check if we have to shutdown and all peers are
                        // disconnected
                        if self.shutting_down && self.peers.iter().all(|p| p.1.connected == false) {
                            break;
                        }
                    }
                }
                Event::Shutdown(_opt_peer_id) => {
                    if let Some(peer_id) = _opt_peer_id {
                        // shutdown peer by removing it and then disconnect
                        if let Some(client) = self.clients.remove(&peer_id) {
                            client.disconnect(None);
                        } else {
                            error!("unable to find client with the peer_id: {}", peer_id);
                        };
                    } else {
                        // shutdown everything
                        self.shutting_down = true;
                        // remove self.tx to gracefully shutdown the rx
                        self.tx = None;
                        for (_peer_id, client) in self.clients.drain() {
                            client.disconnect(None);
                        }
                    }
                }
            }
        }
        self.launcher_tx.aknowledge_shutdown("broker".to_string());
    }
}

fn gen_random_peer_id(peers: &HashMap<usize, Peer>) -> usize {
    let mut id = rand::random();
    while let Some(_) = peers.get(&id) {
        id = rand::random();
    }
    id
}
