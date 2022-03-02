// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anyhow::{
    anyhow,
    bail,
};
use backstage::{
    core::tokio_tungstenite::{
        connect_async,
        tungstenite::Message,
    },
    prefab::websocket::*,
};
use chronicle_broker::{
    application::{
        ImportType,
        ImporterSession,
        TopologyResponse,
    },
    exporter::ExporterStatus,
    merge::{
        LogPaths,
        Merger,
        ValidationLevel,
    },
    *,
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
use std::{
    path::{
        Path,
        PathBuf,
    },
    process::Command,
};
use url::Url;

#[tokio::main]
async fn main() {
    process().await.unwrap();
}

async fn process() -> anyhow::Result<()> {
    dotenv::dotenv()?;
    let websocket_address: std::net::SocketAddr = std::env::var("BACKSERVER_ADDR")?.parse()?;
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
            println!("Connecting to Chronicle backserver: {}", websocket_address);
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", websocket_address))?).await?;
            println!("Chronicle backserver: {}, status: Connected", websocket_address);
            let actor_path = ActorPath::new();
            let shutdown_request = Interface::new(actor_path, Event::shutdown());
            stream.send(shutdown_request.to_message()).await?;
        }
        ("cluster", Some(matches)) => cluster(matches, &websocket_address).await?,
        ("brokers", Some(matches)) => brokers(matches, &websocket_address).await?,
        ("archive", Some(matches)) => archive(matches, &websocket_address).await?,
        _ => (),
    }
    Ok(())
}

async fn cluster<'a>(matches: &ArgMatches<'a>, websocket_address: &std::net::SocketAddr) -> anyhow::Result<()> {
    use scylla_rs::app::cluster::Topology;
    let add_address = matches
        .value_of("add-nodes")
        .map(|address| address.parse().expect("Invalid address provided!"));
    let rem_address = matches
        .value_of("remove-nodes")
        .map(|address| address.parse().expect("Invalid address provided!"));
    println!("Connecting to Chronicle backserver: {}", websocket_address);
    let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", websocket_address))?).await?;
    println!("Chronicle backserver: {}, status: Connected", websocket_address);
    if let Some(address) = add_address {
        let actor_path = ActorPath::new().push("scylla".into()).push("cluster".into());
        let add_node_json = serde_json::to_string(&Topology::AddNode(address))?;
        let add_node_request = Interface::new(actor_path, Event::Call(add_node_json.into()));
        stream.send(add_node_request.to_message()).await?;
        let add_node_response = stream.next().await.ok_or_else(|| {
            anyhow::anyhow!(
                "Stream closed before receiving response for scylla/cluster add node: '{}' request",
                address
            )
        })??;
        println!("{}", add_node_response);
    }
    if let Some(address) = rem_address {
        let actor_path = ActorPath::new().push("scylla".into()).push("cluster".into());
        let remove_node_json = serde_json::to_string(&Topology::RemoveNode(address))?;
        let remove_node_request = Interface::new(actor_path, Event::Call(remove_node_json.into()));
        stream.send(remove_node_request.to_message()).await?;
        let remove_node_response = stream.next().await.ok_or_else(|| {
            anyhow::anyhow!(
                "Stream closed before receiving response for scylla/cluster remove node: '{}' request",
                address
            )
        })??;
        println!("{}", remove_node_response);
    }
    if matches.is_present("rebuild") {
        println!("Connecting to Chronicle backserver: {}", websocket_address);
        let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", websocket_address))?).await?;
        println!("Chronicle backserver: {}, status: Connected", websocket_address);
        let actor_path = ActorPath::new().push("scylla".into()).push("cluster".into());
        let rebuild_event = Topology::BuildRing;
        let rebuild_json = serde_json::to_string(&rebuild_event)?;
        let rebuild_request = Interface::new(actor_path, Event::Call(rebuild_json.into()));
        stream.send(rebuild_request.to_message()).await?;
        let rebuild_response = stream.next().await.ok_or_else(|| {
            anyhow::anyhow!("Stream closed before receiving response for scylla rebuild ring request")
        })??;
        println!("{}", rebuild_response);
    }
    if matches.is_present("list") {
        todo!("Print list of nodes");
    }

    Ok(())
}

async fn brokers<'a>(matches: &ArgMatches<'a>, websocket_address: &std::net::SocketAddr) -> anyhow::Result<()> {
    use chronicle_broker::application::Topology;
    match matches.subcommand() {
        ("add", Some(subcommand)) => {
            let mqtt_addresses = subcommand
                .values_of("mqtt-address")
                .ok_or_else(|| anyhow!("No mqtt addresses received!"))?
                .map(|mqtt_address| Ok(Url::parse(mqtt_address)?))
                .filter_map(|r: anyhow::Result<Url>| r.ok());
            let _endpoint_addresses = subcommand.values_of("endpoint-address");
            // TODO add endpoints
            println!("Connecting to Chronicle backserver: {}", websocket_address);
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", websocket_address))?).await?;
            println!("Chronicle backserver: {}, status: Connected", websocket_address);
            let actor_path = ActorPath::new().push("broker".into());
            for mqtt_address in mqtt_addresses {
                let add_mqtt_json = serde_json::to_string(&Topology::AddMqtt(mqtt_address.clone()))?;
                let add_mqtt_request = Interface::new(actor_path.clone(), Event::Call(add_mqtt_json.into()));
                stream.send(add_mqtt_request.to_message()).await?;
                let add_mqtt_response = stream.next().await.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Stream closed before receiving response for broker add mqtt: '{}' request",
                        mqtt_address
                    )
                })??;
                println!("{}", add_mqtt_response);
            }
        }
        ("remove", Some(subcommand)) => {
            let mqtt_addresses = subcommand
                .values_of("mqtt-address")
                .ok_or_else(|| anyhow!("No mqtt addresses received!"))?
                .map(|mqtt_address| Ok(Url::parse(mqtt_address)?))
                .filter_map(|r: anyhow::Result<Url>| r.ok());
            let _endpoint_addresses = subcommand.values_of("endpoint-address");
            // TODO add endpoints
            println!("Connecting to Chronicle backserver: {}", websocket_address);
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", websocket_address))?).await?;
            println!("Chronicle backserver: {}, status: Connected", websocket_address);
            let actor_path = ActorPath::new().push("broker".into());
            for mqtt_address in mqtt_addresses {
                let remove_mqtt_json = serde_json::to_string(&Topology::RemoveMqtt(mqtt_address.clone()))?;
                let remove_mqtt_request = Interface::new(actor_path.clone(), Event::Call(remove_mqtt_json.into()));
                stream.send(remove_mqtt_request.to_message()).await?;
                let remove_mqtt_response = stream.next().await.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Stream closed before receiving response for broker remove mqtt: '{}' request",
                        mqtt_address
                    )
                })??;
                println!("{}", remove_mqtt_response);
            }
        }
        _ => (),
    }
    if matches.is_present("list") {
        todo!("list broker")
    }
    Ok(())
}

async fn archive<'a>(matches: &ArgMatches<'a>, websocket_address: &std::net::SocketAddr) -> anyhow::Result<()> {
    use chronicle_broker::application::Topology;
    match matches.subcommand() {
        ("import", Some(subcommand)) => {
            let dir = subcommand.value_of("directory").unwrap_or("");
            let path = PathBuf::from(dir);
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
            println!("Connecting to Chronicle backserver: {}", websocket_address);
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", websocket_address))?).await?;
            println!("Chronicle backserver: {}, status: Connected", websocket_address);
            let sty = ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg} ({eta})",
                )
                .progress_chars("##-");
            let mut active_progress_bars: std::collections::HashMap<(u32, u32), ()> = std::collections::HashMap::new();
            let pb = ProgressBar::new(0);
            pb.set_style(sty.clone());
            let actor_path = ActorPath::new().push("broker".into());
            let import_json = serde_json::to_string(&Topology::Import {
                path,
                resume,
                import_range: Some(range),
            })?;
            let import_request = Interface::new(actor_path, Event::Call(import_json.into()));
            stream.send(import_request.to_message()).await?;
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(msg) => {
                        match msg {
                            Message::Text(ref s) => {
                                if let Ok(session) =
                                    serde_json::from_str::<chronicle_broker::application::TopologyResponse>(s)
                                {
                                    match session {
                                        Ok(TopologyOk::Import(ImporterSession::ProgressBar {
                                            log_file_size,
                                            from_ms,
                                            to_ms,
                                            ms_bytes_size,
                                            milestone_index,
                                            skipped,
                                        })) => {
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
                                        Ok(TopologyOk::Import(ImporterSession::Finish { from_ms, to_ms, msg })) => {
                                            let m = format!("LogFile: {}to{}.log {}", from_ms, to_ms, msg);
                                            if let Some(()) = active_progress_bars.remove(&(from_ms, to_ms)) {
                                                pb.set_message(msg);
                                                pb.println(m);
                                            } else {
                                                pb.println(m);
                                            }
                                        }
                                        Ok(TopologyOk::Import(ImporterSession::PathError { path, msg })) => {
                                            pb.println(format!("ErrorPath: {:?}, msg: {:?}", path, msg))
                                        }
                                        Ok(TopologyOk::Import(ImporterSession::Close)) => {
                                            pb.finish_with_message("done");
                                            break;
                                        }
                                        _r => {
                                            let m = format!("Received Unexpected topology response, {:?}", r);
                                            pb.println(m);
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
            println!("Connecting to Chronicle backserver: {}", websocket_address);
            let (mut stream, _) = connect_async(Url::parse(&format!("ws://{}/", websocket_address))?).await?;
            println!("Chronicle backserver: {}, status: Connected", websocket_address);
            let sty = ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} ({eta})")
                .progress_chars("##-");
            let pb = ProgressBar::new(0);
            pb.set_style(sty.clone());
            pb.set_length(range.len() as u64);
            let actor_path = ActorPath::new().push("broker".into());
            let export_json = serde_json::to_string(&Topology::Export { range })?;
            let export_request = Interface::new(actor_path, Event::Call(export_json.into()));
            stream.send(export_request.to_message()).await?;
            pb.enable_steady_tick(200);
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(msg) => match msg {
                        Message::Text(ref s) => {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
                                if let Some(res) = json.get("Response") {
                                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(res.as_str().unwrap()) {
                                        match serde_json::from_value::<TopologyResponse>(json) {
                                            Ok(session) => match session {
                                                Ok(o) => match o {
                                                    application::TopologyOk::Export(e) => match e {
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
                                                    },
                                                    _ => {
                                                        println!("Invalid Export response: {:?}", o);
                                                        break;
                                                    }
                                                },
                                                Err(e) => {
                                                    println!("Error: {:?}", e);
                                                    break;
                                                }
                                            },
                                            Err(e) => {
                                                println!("Invalid message from Chronicle: {:?}", e);
                                                break;
                                            }
                                        }
                                    } else {
                                        println!("Json message from Chronicle: {:?}", json);
                                    }
                                } else {
                                    println!("Json message from Chronicle: {:#}", json);
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
        ("validate", Some(matches)) => validate_archive(matches).await?,
        _ => (),
    }
    Ok(())
}

async fn cleanup_archive<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let max_log_size: u64;
    if let Some(log_size) = matches.value_of("log-size") {
        max_log_size = log_size.parse()?;
    } else {
        println!("Please provide the log size");
        return Ok(());
    };
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
    let dir = matches.value_of("directory").unwrap_or("");
    let logs_dir = PathBuf::from(dir);
    let is_empty = logs_dir.read_dir()?.next().is_none();
    if is_empty {
        println!("LogsDir is empty, probably Chronicle is running without archiver");
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

async fn validate_archive<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let max_log_size: u64;
    if let Some(log_size) = matches.value_of("log-size") {
        max_log_size = log_size.parse()?;
    } else {
        println!("Please provide the log size");
        return Ok(());
    };
    let logs_dir;
    if let Some(dir) = matches.value_of("directory") {
        logs_dir = PathBuf::from(dir);
    } else {
        println!("Please provide the logs directory");
        return Ok(());
    };
    let is_empty = logs_dir.read_dir()?.next().is_none();
    if is_empty {
        println!("LogsDir is empty, probably Chronicle is running without archiver");
        return Ok(());
    }
    LogPaths::new(&logs_dir, true)?.validate(max_log_size, true).await
}
