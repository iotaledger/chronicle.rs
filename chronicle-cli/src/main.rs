// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anyhow::{
    anyhow,
    bail,
};
use backstage::prefab::websocket::*;
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
use chronicle_storage::access::{
    Address,
    Ed25519Address,
    Essence,
    LedgerInclusionState,
    MilestoneData,
    Output,
    Payload,
    SignatureUnlock,
    UnlockBlock,
};
use chrono::NaiveDate;
use clap::{
    load_yaml,
    App,
    ArgMatches,
};
use crypto::{
    hashes::{
        blake2b::Blake2b256,
        Digest,
    },
    signatures::ed25519::{
        PublicKey,
        Signature,
    },
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
use serde::Serialize;
use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    path::{
        Path,
        PathBuf,
    },
    process::Command,
};
use tokio::{
    fs::OpenOptions,
    io::{
        AsyncBufReadExt,
        BufReader,
    },
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
                import_type,
            })?;
            let import_request = Interface::new(actor_path, Event::Call(import_json.into()));
            stream.send(import_request.to_message()).await?;
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
        ("report", Some(matches)) => report_archive(matches).await?,
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

#[derive(Clone, Debug, Default)]
struct ReportData {
    pub total_addresses: HashSet<Address>,
    pub recv_addresses: HashSet<Address>,
    pub send_addresses: HashSet<Address>,
    pub message_count: usize,
    pub included_transaction_count: usize,
    pub conflicting_transaction_count: usize,
    pub total_transaction_count: usize,
    pub transferred_tokens: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ReportRow {
    pub date: NaiveDate,
    pub total_addresses: usize,
    pub recv_addresses: usize,
    pub send_addresses: usize,
    pub message_count: usize,
    pub included_transaction_count: usize,
    pub conflicting_transaction_count: usize,
    pub total_transaction_count: usize,
    pub transferred_tokens: usize,
}

impl From<(NaiveDate, ReportData)> for ReportRow {
    fn from((t, d): (NaiveDate, ReportData)) -> Self {
        Self {
            date: t,
            total_addresses: d.total_addresses.len(),
            recv_addresses: d.recv_addresses.len(),
            send_addresses: d.send_addresses.len(),
            message_count: d.message_count,
            included_transaction_count: d.included_transaction_count,
            conflicting_transaction_count: d.conflicting_transaction_count,
            total_transaction_count: d.total_transaction_count,
            transferred_tokens: d.transferred_tokens,
        }
    }
}

async fn report_archive<'a>(matches: &ArgMatches<'a>) -> anyhow::Result<()> {
    let dir = matches.value_of("directory").unwrap_or("");
    let range = matches.value_of("range");
    let range = range
        .map(|r| {
            let matches = Regex::new(r"(\d+)(?:\D+(\d+))?")?
                .captures(r)
                .ok_or_else(|| anyhow!("Malformatted range!"));
            matches.and_then(|c| {
                let start = c.get(1).unwrap().as_str().parse::<u32>()?;
                let end = c
                    .get(2)
                    .map(|s| s.as_str().parse::<u32>())
                    .transpose()?
                    .unwrap_or(start + 1);
                Ok(start..end)
            })
        })
        .transpose()?
        .unwrap_or(0..u32::MAX);
    if range.is_empty() {
        println!("Empty range!");
        return Ok(());
    }
    let sty = ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} ({eta})")
        .progress_chars("##-");
    let pb = ProgressBar::new(0);
    pb.set_style(sty.clone());
    let split_filename = |v: Result<PathBuf, _>| match v {
        Ok(path) => {
            let file_name = path.file_stem().unwrap();
            let mut split = file_name.to_str().unwrap().split(".").next().unwrap().split("to");
            let (start, end) = (
                split.next().unwrap().parse::<u32>().unwrap(),
                split.next().map(|s| s.parse::<u32>().unwrap()).unwrap_or(u32::MAX),
            );
            Some((start, end, path))
        }
        Err(_) => None,
    };
    let mut paths = glob::glob(&format!("{}/*to*.log*", dir))
        .unwrap()
        .filter_map(split_filename)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        println!("No logs found in {}", dir);
        return Ok(());
    }
    paths.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut len = 0;
    for (start, end, _) in paths
        .iter()
        .filter(|(start, end, _)| start <= &range.end || end >= &range.start)
    {
        len += end - start;
    }
    pb.set_length(len as u64);
    let mut report = BTreeMap::<_, ReportData>::new();
    pb.enable_steady_tick(200);
    for (start, end, path) in paths {
        let contained_range = start.max(range.start)..end.min(range.end);
        if contained_range.is_empty() {
            if start > range.end {
                break;
            }
            continue;
        }
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let reader = BufReader::new(&mut file);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await.map_err(|e| anyhow!(e))? {
            let data = serde_json::from_str::<MilestoneData>(&line)?;
            if contained_range.contains(&data.milestone_index()) {
                pb.set_message(format!("Processing milestone {}", data.milestone_index()));
                let date = chrono::NaiveDateTime::from_timestamp(
                    data.milestone()
                        .ok_or_else(|| anyhow!("No milestone data for {}", data.milestone_index()))?
                        .essence()
                        .timestamp() as i64,
                    0,
                )
                .date();
                let report = report.entry(date).or_default();
                report.message_count += data.messages().len();
                for (metadata, payload) in data.messages().values().filter_map(|f| match f.0.payload() {
                    Some(Payload::Transaction(t)) => Some((&f.1, &**t)),
                    _ => None,
                }) {
                    if metadata.ledger_inclusion_state == Some(LedgerInclusionState::Included) {
                        report.included_transaction_count += 1;
                    }
                    if metadata.ledger_inclusion_state == Some(LedgerInclusionState::Conflicting) {
                        report.conflicting_transaction_count += 1;
                    }
                    report.total_transaction_count += 1;
                    let Essence::Regular(regular_essence) = payload.essence();
                    {
                        for output in regular_essence.outputs() {
                            match output {
                                // Accumulate the transferred token amount
                                Output::SignatureLockedSingle(output) => {
                                    report.transferred_tokens += output.amount() as usize;
                                    report.total_addresses.insert(output.address().clone());
                                    report.recv_addresses.insert(output.address().clone());
                                }
                                Output::SignatureLockedDustAllowance(output) => {
                                    report.transferred_tokens += output.amount() as usize
                                }
                                _ => (),
                            }
                        }
                    }
                    for unlock in payload.unlock_blocks().iter() {
                        if let UnlockBlock::Signature(SignatureUnlock::Ed25519(sig)) = unlock {
                            let address =
                                Address::Ed25519(Ed25519Address::new(Blake2b256::digest(sig.public_key()).into()));
                            report.total_addresses.insert(address);
                            report.send_addresses.insert(address);
                        }
                    }
                }
                pb.inc(1);
            }
        }
    }
    let mut writer =
        csv::Writer::from_path(PathBuf::from(dir).join(format!("report_{}to{}.csv", range.start, range.end)))?;
    pb.set_length(report.len() as u64);
    for (date, data) in report {
        pb.set_message(format!("Writing data for date {}", date));
        writer.serialize(ReportRow::from((date, data)))?;
        pb.inc(1);
    }
    Ok(())
}
