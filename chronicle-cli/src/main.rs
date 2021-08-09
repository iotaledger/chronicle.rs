// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anyhow::{
    anyhow,
    bail,
};
use chronicle_api::{
    websocket::{
        ChronicleBrokerRequest,
        RequesterTopologyRequest,
        ScyllaRequest,
        WebsocketResult,
    },
    ChronicleRequest,
};
use chronicle_broker::{
    application::ImportType,
    exporter::ExporterStatus,
    merge::{
        LogPaths,
        Merger,
        ValidationLevel,
    },
    *,
};
use chronicle_common::{
    config::{
        MqttType,
        VersionedConfig,
    },
    get_history_mut_async,
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
use scylla_rs::prelude::websocket::Topology;
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
            let (mut stream, _) = connect_async(Url::parse(&format!(
                "ws://{}/",
                config.broker_config.websocket_address
            ))?)
            .await?;
            let message = Message::text(serde_json::to_string(&ChronicleRequest::ExitProgram)?);
            stream.send(message).await?;
            if let Some(Ok(msg)) = stream.next().await {
                handle_websocket_result(msg);
            }
        }
        ("config", Some(matches)) => {
            if matches.is_present("print") {
                let config = VersionedConfig::load(None)?.verify().await?;
                println!("{:#?}", config);
            }
            if matches.is_present("rollback") {
                let mut history = get_history_mut_async().await;
                history.rollback();
            }
        }
        ("cluster", Some(matches)) => cluster(matches).await?,
        ("brokers", Some(matches)) => brokers(matches).await?,
        ("archive", Some(matches)) => archive(matches).await?,
        _ => (),
    }
    Ok(())
}

async fn cluster<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let mut config = VersionedConfig::load(None)?.verify().await?;
    let add_address = matches
        .value_of("add-nodes")
        .map(|address| address.parse().expect("Invalid address provided!"));
    let rem_address = matches
        .value_of("remove-nodes")
        .map(|address| address.parse().expect("Invalid address provided!"));
    if !matches.is_present("skip-connection") {
        let (mut stream, _) = connect_async(Url::parse(&format!(
            "ws://{}/",
            config.broker_config.websocket_address
        ))?)
        .await?;

        if let Some(address) = add_address {
            let message = Message::text(serde_json::to_string(&ChronicleRequest::Scylla(
                ScyllaRequest::Topology(scylla_rs::prelude::websocket::Topology::AddNode(address)),
            ))?);
            stream.send(message).await?;
            if let Some(Ok(msg)) = stream.next().await {
                handle_websocket_result(msg);
            }
        }
        if let Some(address) = rem_address {
            let message = Message::text(serde_json::to_string(&ChronicleRequest::Scylla(
                ScyllaRequest::Topology(scylla_rs::prelude::websocket::Topology::RemoveNode(address)),
            ))?);
            stream.send(message).await?;
            if let Some(Ok(msg)) = stream.next().await {
                handle_websocket_result(msg);
            }
        }

        if matches.is_present("rebuild") {
            let message = Message::text(serde_json::to_string(&ChronicleRequest::Scylla(
                ScyllaRequest::Topology(scylla_rs::prelude::websocket::Topology::BuildRing(1)),
            ))?);
            stream.send(message).await?;
            if let Some(Ok(msg)) = stream.next().await {
                handle_websocket_result(msg);
            }
        }

        if matches.is_present("list") {
            let message = Message::text(serde_json::to_string(&ChronicleRequest::Scylla(
                ScyllaRequest::ListNodes,
            ))?);
            stream.send(message).await?;
            if let Some(Ok(msg)) = stream.next().await {
                match msg {
                    Message::Text(s) => {
                        if let Ok(nodes) = serde_json::from_str::<Vec<String>>(&s) {
                            println!("Running Nodes:");
                            for node in nodes {
                                println!(" - {}", node);
                            }
                        } else {
                            println!("Received invalid response from the websocket! {}", s)
                        }
                    }
                    _ => println!("Received invalid response from the websocket! {}", msg),
                }
            }
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
                println!(" - {}", n);
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
                .map(|addresses| addresses.map(Url::parse).collect::<Result<Vec<_>, _>>())
                .transpose()?;
            let endpoint_addresses = subcommand
                .values_of("endpoint-address")
                .map(|addresses| addresses.map(Url::parse).collect::<Result<Vec<_>, _>>())
                .transpose()?;

            if !matches.is_present("skip-connection") {
                let mut messages = Vec::new();
                if let Some(mqtt_addresses) = mqtt_addresses {
                    for mqtt_address in mqtt_addresses {
                        messages.push(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                            ChronicleBrokerRequest::AddMqttMessages(mqtt_address.clone()),
                        ))?));
                        messages.push(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                            ChronicleBrokerRequest::AddMqttMessagesReferenced(mqtt_address),
                        ))?));
                    }
                }
                if let Some(endpoint_addresses) = endpoint_addresses {
                    for address in endpoint_addresses {
                        messages.push(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                            ChronicleBrokerRequest::Requester(RequesterTopologyRequest::AddEndpoint(address)),
                        ))?));
                    }
                }
                if !messages.is_empty() {
                    let (mut stream, _) = connect_async(Url::parse(&format!(
                        "ws://{}/",
                        config.broker_config.websocket_address
                    ))?)
                    .await?;
                    let msg_len = messages.len();
                    stream
                        .send_all(&mut futures::stream::iter(messages.into_iter().map(Ok)))
                        .await?;
                    for _ in 0..msg_len {
                        if let Some(Ok(msg)) = stream.next().await {
                            handle_websocket_result(msg);
                        }
                    }
                }
            } else {
                let mut config_changed = false;
                if let Some(mqtt_addresses) = mqtt_addresses {
                    config.broker_config.mqtt_brokers.get_mut(&MqttType::Messages).map(|m| {
                        for address in mqtt_addresses.clone() {
                            config_changed = m.insert(address) | config_changed;
                        }
                    });
                    config
                        .broker_config
                        .mqtt_brokers
                        .get_mut(&MqttType::MessagesReferenced)
                        .map(|m| {
                            for address in mqtt_addresses {
                                config_changed = m.insert(address) | config_changed;
                            }
                        });
                    config_changed = true;
                }
                if let Some(endpoint_addresses) = endpoint_addresses {
                    for address in endpoint_addresses {
                        config_changed = config.broker_config.api_endpoints.insert(address) | config_changed;
                    }
                }
                if config_changed {
                    config.save(None).expect("Failed to save config!");
                }
            }
        }
        ("remove", Some(subcommand)) => {
            let mqtt_addresses = subcommand
                .values_of("mqtt-address")
                .map(|addresses| addresses.map(Url::parse).collect::<Result<Vec<_>, _>>())
                .transpose()?;
            let endpoint_addresses = subcommand
                .values_of("endpoint-address")
                .map(|addresses| addresses.map(Url::parse).collect::<Result<Vec<_>, _>>())
                .transpose()?;

            if !matches.is_present("skip-connection") {
                let mut messages = Vec::new();
                if let Some(mqtt_addresses) = mqtt_addresses {
                    for mqtt_address in mqtt_addresses {
                        messages.push(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                            ChronicleBrokerRequest::RemoveMqttMessages(mqtt_address.clone()),
                        ))?));
                        messages.push(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                            ChronicleBrokerRequest::RemoveMqttMessagesReferenced(mqtt_address),
                        ))?));
                    }
                }
                if let Some(endpoint_addresses) = endpoint_addresses {
                    for address in endpoint_addresses {
                        messages.push(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                            ChronicleBrokerRequest::Requester(RequesterTopologyRequest::RemoveEndpoint(address)),
                        ))?));
                    }
                }
                if !messages.is_empty() {
                    let (mut stream, _) = connect_async(Url::parse(&format!(
                        "ws://{}/",
                        config.broker_config.websocket_address
                    ))?)
                    .await?;
                    let msg_len = messages.len();
                    stream
                        .send_all(&mut futures::stream::iter(messages.into_iter().map(Ok)))
                        .await?;
                    for _ in 0..msg_len {
                        if let Some(Ok(msg)) = stream.next().await {
                            handle_websocket_result(msg);
                        }
                    }
                }
            } else {
                let mut config_changed = false;
                if let Some(mqtt_addresses) = mqtt_addresses {
                    config.broker_config.mqtt_brokers.get_mut(&MqttType::Messages).map(|m| {
                        mqtt_addresses.iter().for_each(|u| {
                            config_changed = m.remove(u) | config_changed;
                        })
                    });
                    config
                        .broker_config
                        .mqtt_brokers
                        .get_mut(&MqttType::MessagesReferenced)
                        .map(|m| {
                            mqtt_addresses.iter().for_each(|u| {
                                config_changed = m.remove(u) | config_changed;
                            })
                        });
                }
                if let Some(endpoint_addresses) = endpoint_addresses {
                    for address in endpoint_addresses {
                        config_changed = config.broker_config.api_endpoints.remove(&address) | config_changed;
                    }
                }
                if config_changed {
                    config.save(None).expect("Failed to save config!");
                }
            }
        }
        _ => (),
    }
    if matches.is_present("list") {
        if !matches.is_present("skip-connection") {
            let (mut stream, _) = connect_async(Url::parse(&format!(
                "ws://{}/",
                config.broker_config.websocket_address
            ))?)
            .await?;
            let message = Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                ChronicleBrokerRequest::ListBrokers,
            ))?);
            stream.send(message).await?;
            if let Some(Ok(msg)) = stream.next().await {
                match msg {
                    Message::Text(s) => {
                        if let Ok(brokers) = serde_json::from_str::<Vec<String>>(&s) {
                            println!("Running MQTT brokers:");
                            for broker in brokers {
                                println!(" - {}", broker);
                            }
                        } else {
                            println!("Received invalid response from the websocket! {}", s)
                        }
                    }
                    _ => println!("Received invalid response from the websocket! {}", msg),
                }
            }
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

fn handle_websocket_result(msg: Message) {
    match msg {
        Message::Text(s) => {
            if let Ok(res) = serde_json::from_str::<WebsocketResult>(&s) {
                if let Err(e) = &*res {
                    println!("Error: {}", e);
                }
            } else if let Ok(res) = serde_json::from_str::<Result<Topology, Topology>>(&s) {
                if let Err(e) = res {
                    println!("Error: {:?}", e);
                }
            } else {
                println!("Unknown message: {}", s);
            }
        }
        _ => (),
    }
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
            let resume = subcommand.is_present("resume");
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
                _ => 1..(i32::MAX as u32),
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
            let import_type = if subcommand.is_present("analytics") {
                ImportType::Analytics
            } else {
                ImportType::All
            };
            let sty = ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg} ({eta})",
                )
                .progress_chars("##-");
            let mut active_progress_bars: std::collections::HashMap<(u32, u32), ()> = std::collections::HashMap::new();
            let pb = ProgressBar::new(0);
            pb.set_style(sty.clone());
            let (mut stream, _) = connect_async(Url::parse(&format!(
                "ws://{}/",
                config.broker_config.websocket_address
            ))?)
            .await?;
            stream
                .send(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                    ChronicleBrokerRequest::Import {
                        path,
                        resume,
                        import_range: Some(range),
                        import_type,
                    },
                ))?))
                .await?;
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(msg) => {
                        match msg {
                            Message::Text(ref s) => {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
                                    if let Ok(session) = serde_json::from_value::<ImporterSession>(json) {
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
                                                    pb.set_message(msg);
                                                    pb.println(m);
                                                } else {
                                                    pb.println(m);
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
        ("export", Some(subcommand)) => {
            let range = subcommand.value_of("range").unwrap();
            let matches = Regex::new(r"(\d+)(?:\D+(\d+))?")?
                .captures(range)
                .ok_or_else(|| anyhow!("Malformatted range!"));
            let range = matches.and_then(|c| {
                let start = c.get(1).unwrap().as_str().parse::<u32>()?;
                let end = c
                    .get(2)
                    .map(|s| s.as_str().parse::<u32>())
                    .transpose()?
                    .unwrap_or(start + 1);
                Ok(start..end)
            })?;
            let sty = ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} ({eta})")
                .progress_chars("##-");
            let pb = ProgressBar::new(0);
            pb.set_style(sty.clone());
            pb.set_length(range.len() as u64);
            let (mut stream, _) = connect_async(Url::parse(&format!(
                "ws://{}/",
                config.broker_config.websocket_address
            ))?)
            .await?;
            stream
                .send(Message::text(serde_json::to_string(&ChronicleRequest::Broker(
                    ChronicleBrokerRequest::Export(range),
                ))?))
                .await?;
            pb.enable_steady_tick(200);
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(msg) => match msg {
                        Message::Text(ref s) => {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
                                if let Ok(session) = serde_json::from_value::<ExporterStatus>(json) {
                                    match session {
                                        ExporterStatus::InProgress {
                                            current,
                                            completed,
                                            total: _,
                                        } => {
                                            pb.set_position(completed as u64);
                                            pb.set_message(format!("Current milestone: {}", current));
                                        }
                                        ExporterStatus::Done => {
                                            pb.finish_with_message("Done");
                                            break;
                                        }
                                        ExporterStatus::Failed(e) => {
                                            pb.println(format!("Error: {}", e));
                                            pb.finish_with_message(format!("Error: {}", e));
                                            break;
                                        }
                                    }
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
                    },
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
