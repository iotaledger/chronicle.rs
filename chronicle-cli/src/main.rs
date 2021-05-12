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
    application::{
        ChronicleBrokerThrough,
        ImporterSession,
    },
    solidifier::MilestoneData,
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
use serde::Deserialize;
use serde_json::Value;
use std::{
    fmt::{
        Debug,
        Display,
    },
    ops::Range,
    path::{
        Path,
        PathBuf,
    },
    process::Command,
};
use thiserror::Error;
use tokio::{
    fs::{
        metadata,
        File,
        OpenOptions,
    },
    io::{
        AsyncBufReadExt,
        AsyncSeekExt,
        AsyncWriteExt,
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
                        ChronicleBrokerThrough::Topology(chronicle_broker::application::Topology::RemoveMqttMessages(
                            mqtt_address.clone(),
                        )),
                    ))?));
                    messages.push(Message::text(serde_json::to_string(&SocketMsg::Broker(
                        ChronicleBrokerThrough::Topology(
                            chronicle_broker::application::Topology::RemoveMqttMessagesReferenced(mqtt_address),
                        ),
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
                    ChronicleBrokerThrough::Topology(chronicle_broker::application::Topology::Import {
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
        _ => (),
    }
    Ok(())
}

#[derive(Error, Debug)]
enum LogFileError {
    #[error("File is empty: {0}")]
    EmptyFile(PathBuf),
    #[error("Missing milestones {} to {}: {path}", .range.start, .range.end)]
    MissingMilestones { range: Range<u32>, path: PathBuf },
    #[error("{num} extra milestones found: {path}")]
    ExtraMilestones { num: u32, path: PathBuf },
    #[error("Milestone {milestone} is outside of the file range: {path}")]
    OutsideMilestone { milestone: u32, path: PathBuf },
    #[error("Duplicate milestone {milestone} found: {path}")]
    DuplicateMilestone { milestone: u32, path: PathBuf },
    #[error("Malformatted milestone {milestone}: {path}")]
    MalformattedMilestone { milestone: u32, path: PathBuf },
    #[error("Invalid range specified: {} to {}: {path}", .range.start, .range.end)]
    InvalidRange { range: Range<u32>, path: PathBuf },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ValidationLevel {
    /// Validate only the length and filename
    Basic,
    /// Validate the milestone indexes
    Light,
    /// Validate all data formatting
    Full,
    /// Validate all data formatting as it is about to be appended
    JustInTime,
}

impl Display for ValidationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationLevel::Basic => write!(f, "Basic"),
            ValidationLevel::Light => write!(f, "Light"),
            ValidationLevel::Full => write!(f, "Full"),
            ValidationLevel::JustInTime => write!(f, "JustInTime"),
        }
    }
}

impl Default for ValidationLevel {
    fn default() -> Self {
        ValidationLevel::JustInTime
    }
}

#[derive(Debug, Deserialize)]
struct LightMilestoneData {
    milestone_index: u32,
    milestone: Value,
    messages: Value,
    pending: Value,
    created_by: Value,
}

impl LightMilestoneData {
    /// Get the milestone index from this milestone data
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
}

struct LogFile {
    start: u32,
    end: u32,
    file_path: PathBuf,
    file: tokio::fs::File,
    len: u64,
}

impl LogFile {
    pub fn new(start: u32, end: u32, file_path: PathBuf, file: File, len: u64) -> Self {
        Self {
            file_path,
            file,
            len,
            start,
            end,
        }
    }
    pub fn len(&self) -> u64 {
        self.len
    }
    /// Append a new line to the active log file
    pub async fn append_line(&mut self, line: &String) -> anyhow::Result<()> {
        let bytes = line.as_bytes();
        // append to the file
        if let Err(e) = self.file.write_all(bytes).await {
            bail!(
                "Unable to append milestone data line into the log file: {:?}, error: {:?}",
                self.file_path,
                e
            );
        };
        self.end += 1;
        // update bytes size length;
        self.len += bytes.len() as u64;
        Ok(())
    }

    pub async fn verify(&mut self, level: ValidationLevel) -> Result<(), LogFileError> {
        if self.len == 0 {
            return Err(LogFileError::EmptyFile(self.file_path.clone()));
        }
        if self.start >= self.end {
            return Err(LogFileError::InvalidRange {
                range: self.start..self.end,
                path: self.file_path.clone(),
            });
        }
        match level {
            ValidationLevel::Light | ValidationLevel::Full => {
                let path = self.file_path.clone();
                let reader = BufReader::new(&mut self.file);
                let mut idx = self.start;
                let mut lines = reader.lines();
                let mut extra = 0;
                while let Some(line) = lines.next_line().await.map_err(|e| anyhow!(e))? {
                    // If we've exceeded our claimed range, just add up the extras
                    if idx >= self.end {
                        extra += 1;
                        continue;
                    } else if idx < self.start {
                        extra += 1;
                        idx += 1;
                        continue;
                    }
                    let milestone_index = match level {
                        ValidationLevel::Light => serde_json::from_str::<LightMilestoneData>(&line)
                            .map_err(|_| LogFileError::MalformattedMilestone {
                                milestone: idx,
                                path: path.clone(),
                            })?
                            .milestone_index(),
                        ValidationLevel::Full => serde_json::from_str::<MilestoneData>(&line)
                            .map_err(|_| LogFileError::MalformattedMilestone {
                                milestone: idx,
                                path: path.clone(),
                            })?
                            .milestone_index(),
                        _ => panic!(),
                    };
                    if milestone_index > idx {
                        return Err(LogFileError::MissingMilestones {
                            range: idx..milestone_index,
                            path,
                        });
                    } else if milestone_index < idx {
                        extra += 1;
                        if milestone_index >= self.start && milestone_index < self.end {
                            return Err(LogFileError::DuplicateMilestone {
                                milestone: milestone_index,
                                path,
                            });
                        }
                    }
                    idx += 1;
                }
                if extra > 0 {
                    return Err(LogFileError::ExtraMilestones { num: extra, path });
                }
                self.file
                    .seek(tokio::io::SeekFrom::Start(0))
                    .await
                    .map_err(|e| anyhow!(e))?;
            }
            _ => (),
        }

        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.file.flush().await?;
        if self.file.metadata().await.is_ok() {
            let new_path = self
                .file_path
                .parent()
                .unwrap()
                .join(&format!("{}to{}.log", self.start, self.end));
            if self.file_path != new_path {
                tokio::fs::rename(&self.file_path, new_path).await?;
            }
        }
        Ok(())
    }
}

impl std::ops::Drop for LogFile {
    fn drop(&mut self) {
        futures::executor::block_on(self.close()).unwrap();
    }
}

pub struct Merger {
    paths: Vec<(u32, u32, PathBuf)>,
    logs_dir: PathBuf,
    max_log_size: u64,
    progress_bar: Option<ProgressBar>,
    backup_dir: Option<PathBuf>,
    validation_level: ValidationLevel,
    exit_on_val_err: bool,
}

impl Merger {
    /// Create new merger to merge the log files in the logs dir
    pub fn new(
        logs_dir: PathBuf,
        max_log_size: u64,
        backup_logs: bool,
        progress_bar: bool,
        validation_level: ValidationLevel,
        exit_on_val_err: bool,
    ) -> anyhow::Result<Self> {
        let mut progress_bar = progress_bar.then(|| {
            let style = ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg} ({eta})",
                )
                .progress_chars("##-");
            ProgressBar::new(0).with_style(style)
        });
        if let Some(pb) = progress_bar.as_mut() {
            pb.println(format!("Gathering log files from {}", logs_dir.to_string_lossy()));
        }
        if let Some(dir) = logs_dir.to_str() {
            let mut paths = glob::glob(&format!("{}/*to*.log", dir))
                .unwrap()
                .filter_map(|v| match v {
                    Ok(path) => {
                        let file_name = path.file_stem().unwrap();
                        let mut split = file_name.to_str().unwrap().split("to");
                        let (start, end) = (
                            split.next().unwrap().parse::<u32>().unwrap(),
                            split.next().unwrap().parse::<u32>().unwrap(),
                        );
                        if let Some(pb) = progress_bar.as_mut() {
                            if let Ok(metadata) = std::fs::metadata(&path) {
                                pb.inc_length(metadata.len());
                            }
                        }
                        Some((start, end, path))
                    }
                    Err(_) => None,
                })
                .collect::<Vec<_>>();
            paths.sort_unstable_by(|a, b| b.0.cmp(&a.0));
            let backup_dir = backup_logs.then(|| logs_dir.join("backup"));
            Ok(Self {
                logs_dir,
                max_log_size,
                progress_bar,
                paths,
                backup_dir,
                validation_level,
                exit_on_val_err,
            })
        } else {
            bail!("Logs directory is malformatted! Found: {}", logs_dir.to_string_lossy());
        }
    }

    pub async fn cleanup(mut self) -> anyhow::Result<()> {
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.enable_steady_tick(100);
            pb.println("Merging logs with the following configuration:");
            pb.println(format!(" - validation level: {}", self.validation_level));
            pb.println(format!(" - backup: {}", self.backup_dir.is_some()));
            pb.println(format!(" - exit on validation err: {}", self.exit_on_val_err));
        }
        if let Some(ref dir) = self.backup_dir {
            if let Err(e) = tokio::fs::create_dir(dir).await {
                match e.kind() {
                    std::io::ErrorKind::AlreadyExists => (),
                    _ => bail!(e),
                }
            }
        }
        // Take the first path as our dest file
        if let Some(mut writer) = {
            let mut res = None;
            while let Some((start, end, path)) = self.paths.pop() {
                let mut writer = self.open_write(&path, start, end).await?;
                if let Some(pb) = self.progress_bar.as_mut() {
                    pb.set_message(format!("Verifying {}", path.to_string_lossy()));
                }
                if let Err(e) = writer.verify(self.validation_level).await {
                    match e {
                        LogFileError::EmptyFile(_) => {
                            tokio::fs::remove_file(path).await?;
                        }
                        _ => {
                            self.handle_error(e, writer.len())?;
                        }
                    }
                } else {
                    res = Some(writer);
                    break;
                }
            }
            res
        } {
            while let Some((start, end, path)) = self.paths.pop() {
                // The previous file and this one match up
                // or there is an overlap between this log and the previous one
                if start <= writer.end {
                    if start < writer.end {
                        if let Some(pb) = self.progress_bar.as_ref() {
                            pb.println(format!("Found overlap in logs from {} to {}", start, writer.end));
                        }
                    }
                    self.merge(start, end, path, &mut writer).await?;

                // There is a gap in the logs
                } else if start > writer.end {
                    if let Some(pb) = self.progress_bar.as_ref() {
                        pb.println(format!("Found gap in logs from {} to {}", writer.end, start));
                    }
                    writer = self.open_write(&path, start, end).await?;
                }
            }
        } else {
            if let Some(pb) = self.progress_bar.as_ref() {
                pb.println("No valid log files to merge");
            }
        }
        if let Some(pb) = self.progress_bar.as_ref() {
            pb.finish_with_message("Finished merging files!");
        }
        Ok(())
    }

    async fn merge(&mut self, start: u32, end: u32, path: PathBuf, active: &mut LogFile) -> anyhow::Result<()> {
        if start >= end {
            let err = LogFileError::InvalidRange {
                range: start..end,
                path: path.clone(),
            };
            if self.exit_on_val_err {
                bail!(err);
            } else {
                if let Some(pb) = self.progress_bar.as_mut() {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        pb.inc(metadata.len());
                    }
                    pb.println(format!("{}", err))
                }
                return Ok(());
            }
        }
        let mut consumed_file = self.open_read(&path, start, end).await?;
        let total_bytes = consumed_file.len();
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.set_message(format!("Verifying {}", path.to_string_lossy()));
        }
        if let Err(e) = consumed_file.verify(self.validation_level).await {
            match e {
                LogFileError::EmptyFile(_) => {
                    tokio::fs::remove_file(&path).await?;
                }
                _ => {
                    return self.handle_error(e, total_bytes);
                }
            }
            return Ok(());
        }
        let mut buf_reader = BufReader::new(&mut consumed_file.file);
        let mut line_buffer = String::new();
        let mut milestone_index = start;
        let mut total_read_bytes = 0;
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.set_message(format!("Consuming {}", path.to_string_lossy()));
        }
        loop {
            match buf_reader.read_line(&mut line_buffer).await {
                Ok(bytes) => {
                    total_read_bytes += bytes as u64;
                    let ms_line = std::mem::take(&mut line_buffer);
                    if bytes == 0 {
                        // if let Some(pb) = self.progress_bar.as_mut() {
                        //    pb.println(format!("Removing log file {}", path.to_string_lossy()));
                        //}
                        tokio::fs::remove_file(&path).await?;
                        break;
                    } else {
                        // Perform validation if JIT is enabled or we are looking at an overlapping milestone
                        if milestone_index < active.end || self.validation_level == ValidationLevel::JustInTime {
                            if let Ok(idx) =
                                serde_json::from_str::<MilestoneData>(&ms_line).map(|data| data.milestone_index())
                            {
                                if milestone_index < idx {
                                    let err = LogFileError::MissingMilestones {
                                        range: milestone_index..idx,
                                        path,
                                    };
                                    return self.handle_error(err, total_bytes - total_read_bytes);
                                } else if milestone_index > idx {
                                    let err = LogFileError::DuplicateMilestone { milestone: idx, path };
                                    return self.handle_error(err, total_bytes - total_read_bytes);
                                } else if idx < start || idx >= end {
                                    let err = LogFileError::OutsideMilestone { milestone: idx, path };
                                    return self.handle_error(err, total_bytes - total_read_bytes);
                                }
                            } else {
                                let err = LogFileError::MalformattedMilestone {
                                    milestone: milestone_index,
                                    path,
                                };
                                return self.handle_error(err, total_bytes - total_read_bytes);
                            }
                        }
                        // We can fit this line in the writer file
                        if active.len() + (bytes as u64) < self.max_log_size {
                            // if let Some(pb) = self.progress_bar.as_mut() {
                            //    pb.println(format!("Appending to log file {}", active.file_path.to_string_lossy()));
                            //}

                            // Handle overlapping files by skipping milestones until we reach
                            // the end of the active log
                            if milestone_index == active.end {
                                active.append_line(&ms_line).await?;
                            }
                            if let Some(pb) = self.progress_bar.as_mut() {
                                pb.inc(bytes as u64);
                            }

                        // Adding this line would go over our limit
                        } else {
                            // if let Some(pb) = self.progress_bar.as_mut() {
                            //    pb.println("Exceeded file size!");
                            //}
                            // Create a new file to funnel the remainder of the milestones to
                            *active = self.create_active(milestone_index).await?;
                            // Add the line we just read
                            active.append_line(&ms_line).await?;
                            if let Some(pb) = self.progress_bar.as_mut() {
                                pb.inc(bytes as u64);
                            }
                        }
                    }
                    milestone_index += 1;
                }
                Err(e) => {
                    return self.handle_error(e, total_bytes - total_read_bytes);
                }
            }
        }
        match self.validation_level {
            ValidationLevel::Basic | ValidationLevel::JustInTime => {
                if milestone_index < end {
                    let err = LogFileError::MissingMilestones {
                        range: milestone_index..end,
                        path,
                    };
                    return self.handle_error(err, total_bytes - total_read_bytes);
                } else if milestone_index > end {
                    let err = LogFileError::ExtraMilestones {
                        num: milestone_index - end,
                        path,
                    };
                    return self.handle_error(err, total_bytes - total_read_bytes);
                }
            }
            _ => (),
        }
        Ok(())
    }

    fn handle_error<E: Display + Into<anyhow::Error>>(&mut self, err: E, inc_bytes: u64) -> anyhow::Result<()> {
        if self.exit_on_val_err {
            bail!(err);
        } else {
            if let Some(pb) = self.progress_bar.as_mut() {
                pb.inc(inc_bytes);
                pb.println(format!("{}", err));
            }
            return Ok(());
        }
    }

    async fn open_write(&mut self, file_path: &PathBuf, start: u32, end: u32) -> anyhow::Result<LogFile> {
        // if let Some(pb) = self.progress_bar.as_mut() {
        //    pb.println(format!("Opening file for writes: {}", file_path.to_string_lossy()));
        //}
        let active_file_path = self.logs_dir.join(&format!("{}.part", start));
        // Copy the file to the backup first, if asked
        if let Some(ref dir) = self.backup_dir {
            tokio::fs::copy(file_path, dir.join(file_path.file_name().unwrap())).await?;
        }
        tokio::fs::rename(file_path, &active_file_path).await?;
        let active_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active_file_path)
            .await?;
        let active_len = active_file.metadata().await?.len();
        Ok(LogFile::new(start, end, active_file_path, active_file, active_len))
    }

    async fn open_read(&mut self, file_path: &PathBuf, start: u32, end: u32) -> anyhow::Result<LogFile> {
        // if let Some(pb) = self.progress_bar.as_mut() {
        //    pb.println(format!("Opening file for reads: {}", file_path.to_string_lossy()));
        //}
        // Copy the file to the backup first, if asked
        if let Some(ref dir) = self.backup_dir {
            tokio::fs::copy(file_path, dir.join(file_path.file_name().unwrap())).await?;
        }
        let file = OpenOptions::new().read(true).open(&file_path).await?;
        let len = file.metadata().await?.len();
        Ok(LogFile::new(start, end, file_path.clone(), file, len))
    }

    async fn create_active(&mut self, milestone_index: u32) -> anyhow::Result<LogFile> {
        let file_path = self.logs_dir.join(&format!("{}.part", milestone_index));
        let file: File = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)
            .await
            .map_err(|e| {
                anyhow!(
                    "Unable to create active log file: {}, error: {}",
                    file_path.to_string_lossy(),
                    e
                )
            })?;
        let len = file.metadata().await?.len();
        Ok(LogFile::new(milestone_index, milestone_index, file_path, file, len))
    }
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
    let config = VersionedConfig::load(None)?.verify().await?;
    let logs_dir;
    let max_log_size = config
        .broker_config
        .max_log_size
        .clone()
        .unwrap_or(chronicle_broker::archiver::MAX_LOG_SIZE);
    if let Some(dir) = config.broker_config.logs_dir.as_ref() {
        logs_dir = PathBuf::from(dir);
    } else {
        println!("No LogsDir in the config, Chronicle is running without archiver");
        return Ok(());
    }
    Merger::new(logs_dir, max_log_size, backup_logs, true, val_level, exit_on_val_err)?
        .cleanup()
        .await?;
    Ok(())
}
