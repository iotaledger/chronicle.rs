// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use anyhow::{
    anyhow,
    bail,
};
use chronicle::{
    ConfigCommand,
    SocketMsg,
};
use chronicle_broker::application::ChronicleBrokerThrough;
use chronicle_common::config::{
    MqttType,
    VersionedConfig,
};
use clap::{
    load_yaml,
    App,
    ArgMatches,
};
use futures::SinkExt;
use scylla::application::ScyllaThrough;
use std::process::Command;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};
use url::Url;

#[tokio::main]
async fn main() {
    process().await.unwrap();
}

async fn process() -> anyhow::Result<()> {
    let yaml = load_yaml!("../cli.yaml");
    let app = App::from_yaml(yaml).version(std::env!("CARGO_PKG_VERSION"));
    let matches = app.get_matches();

    match matches.subcommand() {
        ("start", Some(matches)) => {
            // Assume the chronicle exe is in the same location as this one
            let current_exe = std::env::current_exe()?;
            let parent_dir = current_exe
                .parent()
                .ok_or_else(|| anyhow!("Failed to get executable directory!"))?;
            let chronicle_exe = if cfg!(target_os = "windows") {
                parent_dir.join("chronicle.exe")
            } else {
                parent_dir.join("chronicle")
            };
            if chronicle_exe.exists() {
                let chronicle_exe = chronicle_exe
                    .to_str()
                    .ok_or_else(|| anyhow!("Failed to stringify executable"))?;
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
                    command.arg(chronicle_exe).spawn().expect("failed to execute process")
                } else {
                    if matches.is_present("service") {
                        Command::new(chronicle_exe)
                            .arg("&")
                            .spawn()
                            .expect("failed to execute process")
                    } else {
                        Command::new("sh")
                            .arg(chronicle_exe)
                            .spawn()
                            .expect("failed to execute process")
                    }
                };
            } else {
                bail!("No chronicle exe in the current directory: {}", parent_dir.display());
            }
        }
        ("stop", Some(_matches)) => {
            let config = VersionedConfig::load(None)?.verify().await?;
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;
            let message = Message::text(serde_json::to_string(&SocketMsg::Broker(
                ChronicleBrokerThrough::ExitProgram,
            ))?);
            stream.send(message).await?;
        }
        ("rebuild", Some(_matches)) => {
            let config = VersionedConfig::load(None)?.verify().await?;
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;
            let message = Message::text(serde_json::to_string(&SocketMsg::Scylla(ScyllaThrough::Topology(
                scylla::application::Topology::BuildRing(1),
            )))?);
            stream.send(message).await?;
        }
        ("config", Some(matches)) => {
            let config = VersionedConfig::load(None)?.verify().await?;
            if matches.is_present("print") {
                println!("{:#?}", config);
            }
            if matches.is_present("rollback") {
                let (mut stream, _) =
                    connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;
                let message = Message::text(serde_json::to_string(&SocketMsg::General(ConfigCommand::Rollback))?);
                stream.send(message).await?;
            }
        }
        ("nodes", Some(matches)) => nodes(matches).await?,
        ("brokers", Some(matches)) => brokers(matches).await?,
        ("archive", Some(matches)) => archive(matches).await?,
        _ => (),
    }
    Ok(())
}

async fn nodes<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let mut config = VersionedConfig::load(None)?.verify().await?;
    let add_address = matches
        .value_of("add")
        .map(|address| address.parse().expect("Invalid address provided!"));
    let rem_address = matches
        .value_of("remove")
        .map(|address| address.parse().expect("Invalid address provided!"));
    if !matches.is_present("skip-connection") {
        let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;

        if matches.is_present("list") {
            todo!("Print list of nodes");
        }
        if let Some(address) = add_address {
            let message = SocketMsg::Scylla(ScyllaThrough::Topology(scylla::application::Topology::AddNode(address)));
            let message = Message::text(serde_json::to_string(&message)?);
            stream.send(message).await?;
        }
        if let Some(address) = rem_address {
            let message = SocketMsg::Scylla(ScyllaThrough::Topology(scylla::application::Topology::RemoveNode(
                address,
            )));
            let message = Message::text(serde_json::to_string(&message)?);
            stream.send(message).await?;
        }
    } else {
        if let Some(address) = add_address {
            config.storage_config.nodes.insert(address);
            config.save(None).expect("Failed to save config!");
        }
        if let Some(address) = rem_address {
            config.storage_config.nodes.remove(&address);
            config.save(None).expect("Failed to save config!");
        }
    }
    Ok(())
}

async fn brokers<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let mut config = VersionedConfig::load(None)?.verify().await?;
    match matches.subcommand() {
        ("add", Some(subcommand)) => {
            let mqtt_addresses = subcommand
                .values_of("mqtt-address")
                .ok_or_else(|| anyhow!("No mqtt addresses received!"))?
                .map(|mqtt_address| Ok(Url::parse(mqtt_address)?))
                .filter_map(|r: anyhow::Result<Url>| r.ok());
            let endpoint_addresses = subcommand.values_of("endpoint-address");
            // TODO add endpoints

            if !matches.is_present("skip-connection") {
                let mut messages = Vec::new();
                for mqtt_address in mqtt_addresses.clone() {
                    messages.push(Message::text(serde_json::to_string(&SocketMsg::Broker(
                        ChronicleBrokerThrough::Topology(chronicle_broker::application::Topology::AddMqttMessages(
                            mqtt_address.clone(),
                        )),
                    ))?));
                    messages.push(Message::text(serde_json::to_string(&SocketMsg::Broker(
                        ChronicleBrokerThrough::Topology(
                            chronicle_broker::application::Topology::AddMqttMessagesReferenced(mqtt_address),
                        ),
                    ))?));
                }
                let (mut stream, _) =
                    connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;
                for message in messages.drain(..) {
                    stream.send(message).await?;
                }
            } else {
                config
                    .broker_config
                    .mqtt_brokers
                    .get_mut(&MqttType::Messages)
                    .map(|m| m.extend(mqtt_addresses.clone()));
                config
                    .broker_config
                    .mqtt_brokers
                    .get_mut(&MqttType::MessagesReferenced)
                    .map(|m| m.extend(mqtt_addresses));
                config.save(None).expect("Failed to save config!");
            }
        }
        ("remove", Some(subcommand)) => {
            let id = subcommand.value_of("id").ok_or_else(|| anyhow!("No id provided!"))?;
            todo!("Send message");
        }
        ("list", Some(subcommand)) => {
            todo!("List brokers");
        }
        _ => (),
    }
    Ok(())
}

async fn archive<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let config = VersionedConfig::load(None)?.verify().await?;
    match matches.subcommand() {
        ("import", Some(subcommand)) => {
            let dir = subcommand.value_of("directory");
            let range = subcommand.value_of("range");
            todo!("Send message");
        }
        _ => (),
    }
    Ok(())
}
