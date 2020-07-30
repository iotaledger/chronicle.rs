// WIP
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker::{
        self,
        Error,
    },
};
use std::{
    env,
    process,
    time::Duration,
};

use super::supervisor::{
    Peer,
    Sender as SupervisorTx,
};
use paho_mqtt;
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
    supervisor_tx: SupervisorTx
});

impl MqttBuilder {
    pub fn build(self) -> Mqtt {
        Mqtt {
            pending: 0,
            peer: self.peer.unwrap(),
            supervisor_tx: self.supervisor_tx.unwrap(),
        }
    }
}

pub struct Mqtt {
    peer: Peer,
    supervisor_tx: SupervisorTx,
    pending: usize,
}

use tokio::stream::StreamExt;
use std::fmt::Write;
impl Mqtt {
    pub async fn run(self) {
        let mut client_id = String::new();
        write!(
            &mut client_id,
            "chronicle_mqtt_{}",
            self.peer.get_id()
        )
        .unwrap();
        let mut cli = paho_mqtt::AsyncClient::new(
            paho_mqtt::CreateOptionsBuilder::new()
                .server_uri("tcp://localhost:1883")
                .client_id(client_id)
                .finalize()
        ).unwrap_or_else(|e| {
            process::exit(1);
        });
        let mut stream = cli.get_stream(20);
        // Define the set of options for the connection
        let lwt = paho_mqtt::Message::new("test", "Async subscriber lost connection", paho_mqtt::QOS_1);

        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .will_message(lwt)
            .finalize();

        cli.connect(conn_opts).await;
        cli.subscribe("trytes", 1).await;

        while let Some(msg_opt) = stream.next().await {
            if let Some(msg) = msg_opt {
                println!("{}", msg);
            } else {
                // A "None" means we were disconnected. Try to reconnect...
                println!("Lost connection. Attempting reconnect.");
                while let Err(err) = cli.reconnect().await {
                    println!("Error reconnecting: {}", err);
                }
            }
        }
    }
}
