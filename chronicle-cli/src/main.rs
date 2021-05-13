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
use chronicle_broker::{
    merge::{
        LogPaths,
        Merger,
        ValidationLevel,
    },
    BrokerSocketMsg,
    BrokerTopology,
    ChronicleBrokerThrough,
    *,
};
use chronicle_common::config::{
    MqttType,
    VersionedConfig,
};
use clap::{
    load_yaml,
    App,
    ArgMatches,
};
use futures::{
    SinkExt,
    StreamExt,
};
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use regex::Regex;
use scylla_rs::prelude::ScyllaThrough;
use std::{
    path::{
        Path,
        PathBuf,
    },
    process::Command,
};
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
            let message = Message::text(serde_json::to_string(&BrokerSocketMsg::ChronicleBroker(
                ChronicleBrokerThrough::ExitProgram,
            ))?);
            stream.send(message).await?;
        }
        ("rebuild", Some(_matches)) => {
            let config = VersionedConfig::load(None)?.verify().await?;
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;
            let message = Message::text(serde_json::to_string(&SocketMsg::Scylla(ScyllaThrough::Topology(
                scylla_rs::prelude::Topology::BuildRing(1),
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

        if let Some(address) = add_address {
            let message = SocketMsg::Scylla(ScyllaThrough::Topology(scylla_rs::prelude::Topology::AddNode(address)));
            let message = Message::text(serde_json::to_string(&message)?);
            stream.send(message).await?;
        }
        if let Some(address) = rem_address {
            let message = SocketMsg::Scylla(ScyllaThrough::Topology(scylla_rs::prelude::Topology::RemoveNode(
                address,
            )));
            let message = Message::text(serde_json::to_string(&message)?);
            stream.send(message).await?;
        }
        if matches.is_present("list") {
            todo!("Print list of nodes");
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
        if matches.is_present("list") {
            println!("Configured Nodes:");
            config.storage_config.nodes.iter().for_each(|n| {
                println!("\t{}", n);
            });
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
                    messages.push(Message::text(serde_json::to_string(
                        &BrokerSocketMsg::ChronicleBroker(ChronicleBrokerThrough::Topology(
                            BrokerTopology::AddMqttMessages(mqtt_address.clone()),
                        )),
                    )?));
                    messages.push(Message::text(serde_json::to_string(
                        &BrokerSocketMsg::ChronicleBroker(ChronicleBrokerThrough::Topology(
                            BrokerTopology::AddMqttMessagesReferenced(mqtt_address),
                        )),
                    )?));
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
                        ChronicleBrokerThrough::Topology(BrokerTopology::RemoveMqttMessages(mqtt_address.clone())),
                    ))?));
                    messages.push(Message::text(serde_json::to_string(&SocketMsg::Broker(
                        ChronicleBrokerThrough::Topology(BrokerTopology::RemoveMqttMessagesReferenced(mqtt_address)),
                    ))?));
                }
                let (mut stream, _) =
                    connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;
                for message in messages.drain(..) {
                    stream.send(message).await?;
                }
            } else {
                config.broker_config.mqtt_brokers.get_mut(&MqttType::Messages).map(|m| {
                    mqtt_addresses.clone().for_each(|u| {
                        m.remove(&u);
                    })
                });
                config
                    .broker_config
                    .mqtt_brokers
                    .get_mut(&MqttType::MessagesReferenced)
                    .map(|m| {
                        mqtt_addresses.for_each(|u| {
                            m.remove(&u);
                        })
                    });
                config.save(None).expect("Failed to save config!");
            }
        }
        _ => (),
    }
    if matches.is_present("list") {
        if !matches.is_present("skip-connection") {
            todo!("List brokers");
        } else {
            println!("Configured MQTT Addresses:");
            config.broker_config.mqtt_brokers.iter().for_each(|(ty, s)| {
                println!("\t{:?}", ty);
                for url in s.iter() {
                    println!("\t\t{}", url);
                }
            });
        }
    }
    Ok(())
}

async fn archive<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let config = VersionedConfig::load(None)?.verify().await?;
    match matches.subcommand() {
        ("import", Some(subcommand)) => {
            let dir = subcommand.value_of("directory").unwrap_or("");
            let mut path = PathBuf::from(dir);
            if path.is_relative() {
                if let Some(logs_dir) = config.broker_config.logs_dir.as_ref() {
                    path = Path::new(&logs_dir).join(path);
                }
            }
            let (is_url, is_file) = Url::parse(dir)
                .map(|url| (true, Path::new(url.path()).extension().is_some()))
                .unwrap_or_else(|_| (false, path.extension().is_some()));
            let range = subcommand.value_of("range");
            let range = match range {
                Some(s) => {
                    let matches = Regex::new(r"(\d+)\D+(\d+)")?
                        .captures(s)
                        .ok_or_else(|| anyhow!("Malformatted range!"));
                    matches.and_then(|c| {
                        let start = c.get(1).unwrap().as_str().parse::<u32>()?;
                        let end = c.get(2).unwrap().as_str().parse::<u32>()?;
                        Ok(start..end)
                    })?
                }
                _ => 0..u32::MAX,
            };
            println!(
                "Path: {}, is_url: {}, is_file: {}, range: {:?}",
                if is_url { dir.into() } else { path.to_string_lossy() },
                is_url,
                is_file,
                range
            );
            if is_url {
                panic!("URL imports are not currently supported!");
            }
            let sty = ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg} ({eta})",
                )
                .progress_chars("##-");
            let mut active_progress_bars: std::collections::HashMap<(u32, u32), ()> = std::collections::HashMap::new();
            let pb = ProgressBar::new(0);
            pb.set_style(sty.clone());
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", config.websocket_address))?).await?;
            stream
                .send(Message::text(serde_json::to_string(&SocketMsg::Broker(
                    ChronicleBrokerThrough::Topology(BrokerTopology::Import {
                        path,
                        resume: false,
                        import_range: Some(range),
                    }),
                ))?))
                .await?;
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(msg) => {
                        match msg {
                            Message::Text(ref s) => {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
                                    if let Some(service_json) = json.get("ChronicleBroker").cloned() {
                                        if let Ok(session) =
                                            serde_json::from_value::<ImporterSession>(service_json.clone())
                                        {
                                            match session {
                                                ImporterSession::ProgressBar {
                                                    log_file_size,
                                                    from_ms,
                                                    to_ms,
                                                    ms_bytes_size,
                                                    milestone_index,
                                                    skipped,
                                                } => {
                                                    if let Some(()) = active_progress_bars.get_mut(&(from_ms, to_ms)) {
                                                        // advance the pb
                                                        let skipped_or_imported;
                                                        if skipped {
                                                            skipped_or_imported = "skipped"
                                                        } else {
                                                            skipped_or_imported = "imported"
                                                        }
                                                        pb.set_message(format!(
                                                            "{}to{}.log: {} #{}",
                                                            from_ms, to_ms, skipped_or_imported, milestone_index
                                                        ));
                                                        pb.inc(ms_bytes_size as u64);
                                                    } else {
                                                        pb.inc_length(log_file_size);
                                                        // advance the pb
                                                        let skipped_or_imported;
                                                        if skipped {
                                                            skipped_or_imported = "skipped"
                                                        } else {
                                                            skipped_or_imported = "imported"
                                                        }
                                                        pb.set_message(format!(
                                                            "{}to{}.log: {} #{}",
                                                            from_ms, to_ms, skipped_or_imported, milestone_index
                                                        ));
                                                        pb.inc(ms_bytes_size as u64);
                                                        active_progress_bars.insert((from_ms, to_ms), ());
                                                    }
                                                }
                                                ImporterSession::Finish { from_ms, to_ms, msg } => {
                                                    let m = format!("LogFile: {}to{}.log {}", from_ms, to_ms, msg);
                                                    if let Some(()) = active_progress_bars.remove(&(from_ms, to_ms)) {
                                                        pb.set_message(format!(
                                                            "LogFile: {}to{}.log {}",
                                                            from_ms, to_ms, msg
                                                        ));
                                                        pb.println(format!(
                                                            "LogFile: {}to{}.log {}",
                                                            from_ms, to_ms, msg
                                                        ));
                                                    }
                                                }
                                                ImporterSession::PathError { path, msg } => {
                                                    pb.println(format!("ErrorPath: {:?}, msg: {:?}", path, msg))
                                                }
                                                ImporterSession::Close => {
                                                    pb.finish_with_message("done");
                                                    break;
                                                }
                                            }
                                        }
                                    } else {
                                        println!("Json message from Chronicle: {:?}", json);
                                    }
                                } else {
                                    println!("Text message from Chronicle: {:?}", msg);
                                }
                            }
                            Message::Close(c) => {
                                if let Some(c) = c {
                                    println!("Closed connection: {}", c);
                                }
                                break;
                            }
                            _ => (),
                        }
                    }
                    Err(e) => {
                        println!("Error received from Chronicle: {}", e);
                        break;
                    }
                }
            }
        }
        ("cleanup", Some(matches)) => cleanup_archive(matches).await?,
        ("validate", Some(_matches)) => validate_archive().await?,
        _ => (),
    }
    Ok(())
}

async fn cleanup_archive<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let backup_logs = !matches.is_present("no-backup");
    let val_level = matches
        .value_of("validation-level")
        .map(|s| match s {
            "Basic" => ValidationLevel::Basic,
            "Light" => ValidationLevel::Light,
            "Full" => ValidationLevel::Full,
            "JustInTime" => ValidationLevel::JustInTime,
            _ => panic!("Invalid validation level!"),
        })
        .or_else(|| matches.is_present("val-level-basic").then(|| ValidationLevel::Basic))
        .or_else(|| matches.is_present("val-level-light").then(|| ValidationLevel::Light))
        .or_else(|| matches.is_present("val-level-full").then(|| ValidationLevel::Full))
        .or_else(|| matches.is_present("val-level-jit").then(|| ValidationLevel::JustInTime))
        .unwrap_or_default();
    let exit_on_val_err = !matches.is_present("no-exit-on-val-err");
    let include_finalized = matches.is_present("include-finalized");
    let config = VersionedConfig::load(None)?.verify().await?;
    let logs_dir;
    let max_log_size = config.broker_config.max_log_size.clone().unwrap_or(u32::MAX as u64);
    if let Some(dir) = config.broker_config.logs_dir.as_ref() {
        logs_dir = PathBuf::from(dir);
    } else {
        println!("No LogsDir in the config, Chronicle is running without archiver");
        return Ok(());
    }
    Merger::new(
        logs_dir,
        max_log_size,
        backup_logs,
        true,
        val_level,
        exit_on_val_err,
        include_finalized,
    )?
    .cleanup()
    .await?;
    Ok(())
}

async fn validate_archive() -> anyhow::Result<()> {
    let config = VersionedConfig::load(None)?.verify().await?;
    let logs_dir;
    let max_log_size = config.broker_config.max_log_size.clone().unwrap_or(u32::MAX as u64);
    if let Some(dir) = config.broker_config.logs_dir.as_ref() {
        logs_dir = PathBuf::from(dir);
    } else {
        println!("No LogsDir in the config, Chronicle is running without archiver");
        return Ok(());
    }
    LogPaths::new(&logs_dir, true)?.validate(max_log_size, true).await
}
