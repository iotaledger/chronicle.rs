// WIP
use super::supervisor::{
    Event as SupervisorEvent,
    Peer,
    Sender as SupervisorTx,
};
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker::Error,
};
use paho_mqtt;
use std::time::Duration;
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
pub struct MqttId(Sender);

pub enum Event {
    Void { pid: Box<MqttId> },
    Error { kind: Error, pid: Box<MqttId> },
}
use chronicle_common::actor;

actor!(MqttBuilder {
    id: String,
    peer: Peer,
    client: paho_mqtt::AsyncClient,
    supervisor_tx: SupervisorTx
});

impl MqttBuilder {
    pub fn build(self) -> Mqtt {
        Mqtt {
            pending: 0,
            peer: self.peer.unwrap(),
            client: self.client,
            supervisor_tx: self.supervisor_tx.unwrap(),
        }
    }
}

pub struct Mqtt {
    pub peer: Peer,
    pub client: Option<paho_mqtt::AsyncClient>,
    supervisor_tx: SupervisorTx,
    pending: usize,
}

use paho_mqtt::Message;
use std::fmt::Write;
use tokio::stream::{
    Stream,
    StreamExt,
};

impl Mqtt {
    pub async fn run(
        mut self,
        supervisor_tx: SupervisorTx,
        mut stream: impl Stream<Item = Option<Message>> + std::marker::Unpin,
    ) {
        while let Some(Some(msg)) = stream.next().await {
            println!("{}", msg);
        }
        self.peer.set_connected(false);
        let _ = supervisor_tx.send(SupervisorEvent::Reconnect(self));
    }
    pub async fn init(&mut self) -> Result<impl Stream<Item = Option<Message>>, paho_mqtt::Error> {
        // create mqtt AsyncClient in advance
        let mut client_id = String::new();
        write!(&mut client_id, "chronicle_mqtt_{}", self.peer.id).unwrap();
        let mut cli = paho_mqtt::AsyncClient::new((&self.peer.address[..], &client_id[..]))?;
        let stream = cli.get_stream(100);
        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .finalize();
        cli.connect(conn_opts).await?;
        cli.subscribe(self.peer.get_topic_as_string(), 1).await?;
        self.peer.set_connected(true);
        self.client.replace(cli);
        Ok(stream)
    }
}
