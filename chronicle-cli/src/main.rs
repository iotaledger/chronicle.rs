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
use std::{
    path::{
        Path,
        PathBuf,
    },
    process::Command,
};
use tokio::{
    fs::{
        File,
        OpenOptions,
    },
    io::{
        AsyncBufReadExt,
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
                                                        let m = format!(
                                                            "{}to{}.log: {} #{}",
                                                            from_ms, to_ms, skipped_or_imported, milestone_index
                                                        );
                                                        pb.set_message(&m);
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
                                                        let m = format!(
                                                            "{}to{}.log: {} #{}",
                                                            from_ms, to_ms, skipped_or_imported, milestone_index
                                                        );
                                                        pb.set_message(&m);
                                                        pb.inc(ms_bytes_size as u64);
                                                        active_progress_bars.insert((from_ms, to_ms), ());
                                                    }
                                                }
                                                ImporterSession::Finish { from_ms, to_ms, msg } => {
                                                    if let Some(()) = active_progress_bars.remove(&(from_ms, to_ms)) {
                                                        let m = format!("LogFile: {}to{}.log {}", from_ms, to_ms, msg);
                                                        pb.set_message(&m);
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
        ("cleanup", Some(_subcommand)) => cleanup_archive().await?,
        _ => (),
    }
    Ok(())
}

struct ActiveMerge {
    start: usize,
    end: usize,
    file_path: PathBuf,
    file: tokio::fs::File,
    len: u64,
}

impl ActiveMerge {
    pub fn new(start: usize, end: usize, file_path: PathBuf, file: File, len: u64) -> Self {
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
    pub async fn append_line(&mut self, line: &Vec<u8>) -> anyhow::Result<()> {
        // append to the file
        if let Err(e) = self.file.write_all(line.as_ref()).await {
            bail!(
                "Unable to append milestone data line into the log file: {:?}, error: {:?}",
                self.file_path,
                e
            );
        };
        self.end += 1;
        // update bytes size length;
        self.len += line.len() as u64;
        Ok(())
    }
}

struct Merger {
    active: Option<ActiveMerge>,
    paths: Vec<(usize, usize, PathBuf)>,
    logs_dir: PathBuf,
    max_log_size: u64,
    progress_bar: Option<ProgressBar>,
    remove_file: bool,
}

impl Merger {
    /// Create new merger to merge the log files in the logs dir
    #[allow(unused)]
    pub fn new(logs_dir: PathBuf, max_log_size: u64, remove_files: bool) -> anyhow::Result<Self> {
        let merger = Self {
            active: None,
            logs_dir,
            max_log_size,
            progress_bar: None,
            paths: Vec::new(),
            remove_file: remove_files,
        };
        merger.gather()
    }
    /// Create new merger to merge the log files in the logs dir with progress_bar, this is helpful for CLI
    pub fn with_progress_bar(logs_dir: PathBuf, max_log_size: u64, remove_files: bool) -> anyhow::Result<Self> {
        let style = ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} ({eta})")
            .progress_chars("##-");
        let merger = Self {
            active: None,
            logs_dir,
            max_log_size,
            progress_bar: Some(ProgressBar::new(0).with_style(style)),
            paths: Vec::new(),
            remove_file: remove_files,
        };
        merger.gather()
    }
    fn gather(mut self) -> anyhow::Result<Self> {
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.println("Gathering log files...");
        }
        if let Some(logs_dir) = self.logs_dir.to_str() {
            self.paths = glob::glob(&format!("{}/*to*.log", logs_dir))
                .unwrap()
                .filter_map(|v| match v {
                    Ok(path) => {
                        let file_name = path.file_stem().unwrap();
                        let mut split = file_name.to_str().unwrap().split("to");
                        let (start, end) = (
                            split.next().unwrap().parse::<usize>().unwrap(),
                            split.next().unwrap().parse::<usize>().unwrap(),
                        );
                        Some((start, end, path))
                    }
                    Err(_) => None,
                })
                .collect::<Vec<_>>();
            self.paths.sort_unstable_by(|a, b| b.0.cmp(&a.0));
            if let Some(pb) = self.progress_bar.as_mut() {
                pb.inc_length(self.paths.len() as u64);
            }
        } else {
            bail!("No LogsDir");
        }
        Ok(self)
    }
    async fn merge(&mut self, mut start: usize, end: usize, path: PathBuf, position: usize) -> anyhow::Result<()> {
        if start + position >= end || start >= end {
            bail!(
                "Invalid log file, start: {}, end: {}, position: {}",
                start,
                end,
                position
            );
        }
        start += position;
        let consumed_file = OpenOptions::new().read(true).open(&path).await?;
        let mut buf_reader = BufReader::new(consumed_file);
        // discard all lines up to the position
        for _ in 0..position {
            buf_reader.read_line(&mut String::new()).await?;
        }
        let mut line_buffer: String = String::new();
        let mut processed_lines: usize = 0;
        while let Ok(n) = buf_reader.read_line(&mut line_buffer).await {
            let ms_line: Vec<u8> = std::mem::take(&mut line_buffer).into();
            if n == 0 {
                // reached EOF
                break;
            } else {
                processed_lines += 1;
                // merge file on top of the active log up to max_log_size,
                if let Some(active) = self.active.as_mut() {
                    if active.len() + (n as u64) < self.max_log_size {
                        active.append_line(&ms_line).await?;
                    } else {
                        // close active
                        self.close_active().await?;
                        if let Ok(milestone_data) = serde_json::from_slice::<MilestoneData>(&ms_line) {
                            let milestone_index = milestone_data.milestone_index();
                            // create new file starting from milestone_data_line as active.
                            self.create_active(milestone_index).await?;
                            self.active
                                .as_mut()
                                .expect("Expected active log file")
                                .append_line(&ms_line)
                                .await?;
                        } else {
                            bail!("Corrupted milestone data")
                        };
                    }
                } else {
                    bail!("Expected active file")
                }
            }
        }
        if end - start != processed_lines {
            bail!(
                "Corrupted active log file, start: {}, end: {}, processed_lines: {}",
                start,
                end,
                processed_lines
            );
        }
        if self.remove_file {
            tokio::fs::remove_file(path).await?;
            if let Some(pb) = self.progress_bar.as_ref() {
                pb.println(format!("Removed file: {}to{}.log", start - position, end));
            }
        }
        Ok(())
    }
    async fn cleanup(mut self) -> anyhow::Result<()> {
        if let Some((prev_start, mut prev_end, prev_path)) = self.paths.pop() {
            // set the first active file
            self.open_active(&prev_path, prev_start, prev_end).await?;
            while let Some((start, end, path)) = self.paths.pop() {
                // check if it's continuous range to the previous one
                if prev_end == start {
                    if let Some(pb) = self.progress_bar.as_ref() {
                        pb.println(format!("Continuous merge: {}..{}", start, end));
                    }
                    // this will merge the file from position 0 (without discarding any line)
                    self.merge(start, end, path, 0).await?;
                } else if start > prev_end {
                    if let Some(pb) = self.progress_bar.as_ref() {
                        pb.println(format!("Identified missed log file: {}to{}.log", prev_end, start));
                    }
                    // this is invoked when there is gap in the log files,
                    // close the active file;
                    self.close_active().await?;
                    // switch to next one;
                    self.open_active(&path, start, end).await?;
                } else if start < prev_end && end > prev_end {
                    // This is an overlap, however we have to skip some lines
                    let position = prev_end - start;
                    if let Some(pb) = self.progress_bar.as_ref() {
                        pb.println(format!(
                            "Overlapped merge: {}..{} fetched from {}to{}.log",
                            start + position,
                            end,
                            start,
                            end
                        ));
                    }
                    self.merge(start, end, path, position).await?;
                } else {
                    if self.remove_file {
                        tokio::fs::remove_file(&path).await?;
                        if let Some(pb) = self.progress_bar.as_ref() {
                            pb.println(format!("Removed unnecessary file: {}to{}.log", start, end));
                        }
                    }
                }
                // update prev_end
                prev_end = end;
                if let Some(pb) = self.progress_bar.as_ref() {
                    pb.inc(1);
                }
            }
            // close active file (if any)
            self.close_active().await?;
        } else {
            if let Some(pb) = self.progress_bar.as_ref() {
                pb.println("No Logfiles to merge");
            }
        }
        if let Some(pb) = self.progress_bar.as_ref() {
            pb.finish_with_message("done");
        }
        Ok(())
    }
    async fn open_active(&mut self, file_path: &PathBuf, start: usize, end: usize) -> anyhow::Result<()> {
        if let Some(pb) = self.progress_bar.as_ref() {
            pb.println(format!("Opening {}to{}.log as active: {}.part", start, end, start));
        }
        let part_file_name = format!("{}.part", start);
        let active_file_path = self.logs_dir.join(&part_file_name);
        if let Err(e) = tokio::fs::rename(file_path, &active_file_path).await {
            bail!(e)
        };
        let active_file = OpenOptions::new().append(true).open(&active_file_path).await?;
        let active_len = active_file.metadata().await?.len();
        self.active
            .replace(ActiveMerge::new(start, end, active_file_path, active_file, active_len));
        Ok(())
    }
    async fn close_active(&mut self) -> anyhow::Result<()> {
        if let Some(ActiveMerge {
            file_path, start, end, ..
        }) = self.active.take()
        {
            let log_file_name = format!("{}to{}.log", start, end);
            let log_file_path = self.logs_dir.join(&log_file_name);
            if let Err(e) = tokio::fs::rename(&file_path, log_file_path).await {
                bail!(e)
            };
            if let Some(pb) = self.progress_bar.as_ref() {
                pb.println(format!("Closed active: {}.part as {}to{}.log", start, start, end));
            }
        };
        Ok(())
    }
    async fn create_active(&mut self, milestone_index: u32) -> anyhow::Result<()> {
        let filename = format!("{}.part", milestone_index);
        let file_path = self.logs_dir.join(&filename);
        let file: File = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)
            .await
            .map_err(|e| anyhow!("Unable to create active log file: {}, error: {}", filename, e))?;
        let len = file.metadata().await?.len();
        let active_file = ActiveMerge::new(milestone_index as usize, milestone_index as usize, file_path, file, len);
        if let Some(pb) = self.progress_bar.as_ref() {
            pb.println(format!("Created active: {}.part", milestone_index));
        }
        self.active.replace(active_file);
        Ok(())
    }
}

async fn cleanup_archive() -> anyhow::Result<()> {
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
    Merger::with_progress_bar(logs_dir, max_log_size, true)?
        .cleanup()
        .await?;
    Ok(())
}
