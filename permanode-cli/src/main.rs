use clap::{
    load_yaml,
    App,
    ArgMatches,
};
use futures::SinkExt;
use permanode_broker::application::PermanodeBrokerThrough;
use permanode_common::config::Config;
use scylla::application::ScyllaThrough;
use std::process::Command;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};
use url::Url;

#[tokio::main]
async fn main() {
    let yaml = load_yaml!("../cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("start", Some(matches)) => {
            // Assume the permanode exe is in the same location as this one
            let current_exe = std::env::current_exe().unwrap();
            let parent_dir = current_exe.parent().unwrap();
            let permanode_exe = if cfg!(target_os = "windows") {
                parent_dir.join("permanode.exe")
            } else {
                parent_dir.join("permanode")
            };
            if permanode_exe.exists() {
                if cfg!(target_os = "windows") {
                    let mut command = Command::new("cmd");
                    command.args(&["/c", "start"]);
                    if matches.is_present("service") {
                        command.arg("/B");
                    }
                    command.arg("powershell");
                    if matches.is_present("noexit") {
                        command.arg("-noexit");
                    }
                    command
                        .arg(permanode_exe.to_str().unwrap())
                        .spawn()
                        .expect("failed to execute process")
                } else {
                    if matches.is_present("service") {
                        Command::new(permanode_exe.to_str().unwrap())
                            .arg("&")
                            .spawn()
                            .expect("failed to execute process")
                    } else {
                        Command::new("sh")
                            .arg(permanode_exe.to_str().unwrap())
                            .spawn()
                            .expect("failed to execute process")
                    }
                };
            } else {
                panic!("No chronicle exe in the current directory: {}", parent_dir.display());
            }
        }
        ("stop", Some(matches)) => {
            let config = Config::load().expect("No config file found for Chronicle!");
            let (mut stream, _) =
                connect_async(Url::parse(&format!("ws://{}/", config.broker_config.websocket_address)).unwrap())
                    .await
                    .unwrap();
            let message = Message::text(
                serde_json::to_string(&permanode_broker::application::SocketMsg::PermanodeBroker(
                    PermanodeBrokerThrough::ExitProgram,
                ))
                .unwrap(),
            );
            stream.send(message).await.unwrap();
        }
        ("rebuild", Some(matches)) => {
            let config = Config::load().expect("No config file found for Chronicle!");
            let (mut stream, _) =
                connect_async(Url::parse(&format!("ws://{}/", config.storage_config.listen_address)).unwrap())
                    .await
                    .unwrap();
            let message = Message::text(
                serde_json::to_string(&scylla::application::SocketMsg::Scylla(ScyllaThrough::Topology(
                    scylla::application::Topology::BuildRing(1),
                )))
                .unwrap(),
            );
            stream.send(message).await.unwrap();
        }
        ("nodes", Some(matches)) => node(matches).await,
        ("brokers", Some(matches)) => brokers(matches).await,
        ("archive", Some(matches)) => archive(matches).await,
        _ => (),
    }
}

async fn node<'a>(matches: &ArgMatches<'a>) {
    let mut config = Config::load().expect("No config file found for Chronicle!");
    let add_address = matches
        .value_of("add")
        .map(|address| address.parse().expect("Invalid address provided!"));
    let rem_address = matches
        .value_of("remove")
        .map(|address| address.parse().expect("Invalid address provided!"));
    if let Ok((mut stream, _)) =
        connect_async(Url::parse(&format!("ws://{}/", config.storage_config.listen_address)).unwrap()).await
    {
        if matches.is_present("list") {
            todo!("Print list of nodes");
        }
        if let Some(address) = add_address {
            let message = scylla::application::SocketMsg::Scylla(ScyllaThrough::Topology(
                scylla::application::Topology::AddNode(address),
            ));
            let message = Message::text(serde_json::to_string(&message).unwrap());
            stream.send(message).await.unwrap();
        }
        if let Some(address) = rem_address {
            let message = scylla::application::SocketMsg::Scylla(ScyllaThrough::Topology(
                scylla::application::Topology::RemoveNode(address),
            ));
            let message = Message::text(serde_json::to_string(&message).unwrap());
            stream.send(message).await.unwrap();
        }
    }
    if let Some(address) = add_address {
        config.storage_config.nodes.push(address);
        config.save().expect("Failed to save config!");
    }
    if let Some(address) = rem_address {
        let idx = config.storage_config.nodes.iter().position(|a| a == &address);
        if let Some(idx) = idx {
            config.storage_config.nodes.remove(idx);
            config.save().expect("Failed to save config!");
        }
    }
}

async fn brokers<'a>(matches: &ArgMatches<'a>) {
    let mut config = Config::load().expect("No config file found for Chronicle!");
    match matches.subcommand() {
        ("add", Some(subcommand)) => {
            let mqtt_addresses = subcommand
                .values_of("mqtt-address")
                .unwrap()
                .map(|mqtt_address| Url::parse(mqtt_address).unwrap());
            let endpoint_addresses = subcommand.values_of("endpoint-address");
            // TODO add endpoints
            let mut messages = mqtt_addresses.clone().fold(Vec::new(), |mut list, mqtt_address| {
                list.push(Message::text(
                    serde_json::to_string(&permanode_broker::application::SocketMsg::PermanodeBroker(
                        PermanodeBrokerThrough::Topology(permanode_broker::application::Topology::AddMqttMessages(
                            mqtt_address.clone(),
                        )),
                    ))
                    .unwrap(),
                ));
                list.push(Message::text(
                    serde_json::to_string(&permanode_broker::application::SocketMsg::PermanodeBroker(
                        PermanodeBrokerThrough::Topology(
                            permanode_broker::application::Topology::AddMqttMessagesReferenced(mqtt_address),
                        ),
                    ))
                    .unwrap(),
                ));
                list
            });
            if let Ok((mut stream, _)) =
                connect_async(Url::parse(&format!("ws://{}/", config.broker_config.websocket_address)).unwrap()).await
            {
                for message in messages.drain(..) {
                    stream.send(message).await.unwrap();
                }
            }
            config.broker_config.mqtt_brokers.extend(mqtt_addresses);
            config.save().expect("Failed to save config!");
        }
        ("remove", Some(subcommand)) => {
            let id = subcommand.value_of("id").unwrap();
            todo!("Send message");
        }
        ("list", Some(subcommand)) => {
            todo!("List brokers");
        }
        _ => (),
    }
}

async fn archive<'a>(matches: &ArgMatches<'a>) {
    let mut config = Config::load().expect("No config file found for Chronicle!");
    match matches.subcommand() {
        ("import", Some(subcommand)) => {
            let dir = subcommand.value_of("directory");
            let range = subcommand.value_of("range");
            todo!("Send message");
        }
        _ => (),
    }
}
