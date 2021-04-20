// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::syncer::Ascending;
use crate::{
    application::*,
    solidifier::*,
};
use anyhow::{
    anyhow,
    bail,
};
use std::{
    collections::BinaryHeap,
    convert::TryFrom,
    ops::{
        Deref,
        DerefMut,
    },
    path::PathBuf,
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
    sync::oneshot::Receiver,
};
mod event_loop;
mod init;
mod terminating;

/// The maximum bytes size for a given log file;
pub const MAX_LOG_SIZE: u64 = u32::MAX as u64;

// Archiver builder
builder!(ArchiverBuilder {
    keyspace: ChronicleKeyspace,
    max_log_size: u64,
    oneshot: Receiver<u32>,
    solidifiers_count: u8,
    retries_per_query: usize,
    dir_path: PathBuf
});

/// ArchiverHandle to be passed to the supervisor and solidifers
#[derive(Clone)]
pub struct ArchiverHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<ArchiverEvent>,
}
/// ArchiverInbox is used to recv events from solidifier(s)
pub struct ArchiverInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<ArchiverEvent>,
}
impl Deref for ArchiverHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<ArchiverEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for ArchiverHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Shutdown for ArchiverHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}
type UpperLimit = u32;

/// Archiver events
pub enum ArchiverEvent {
    /// Milestone data to be archived
    MilestoneData(MilestoneData, Option<UpperLimit>),
    /// Close the milestone with given index
    Close(u32),
}

#[derive(Debug)]
/// Write ahead file which stores ordered milestones data by milestone index.
pub struct LogFile {
    len: u64,
    filename: String,
    /// Included milestone data
    from_ms_index: u32,
    /// NotIncluded (yet) milestone data
    to_ms_index: u32,
    upper_ms_limit: u32,
    file: BufReader<File>,
    /// Identifier if it had io error
    maybe_corrupted: bool,
    finished: bool,
}

impl LogFile {
    /// Create a new Write-ahead-log file for a starting milestone index
    pub async fn create(
        dir_path: &PathBuf,
        milestone_index: u32,
        opt_upper_limit: Option<u32>,
    ) -> anyhow::Result<LogFile> {
        let filename = format!("{}.part", milestone_index);
        let file_path = dir_path.join(&filename);
        let file: File = OpenOptions::new()
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .map_err(|e| anyhow!("Unable to create log file: {}, error: {}", filename, e))?;
        Ok(Self {
            len: 0,
            filename,
            from_ms_index: milestone_index,
            to_ms_index: milestone_index,
            upper_ms_limit: opt_upper_limit.unwrap_or(u32::MAX),
            file: BufReader::new(file),
            maybe_corrupted: false,
            finished: false,
        })
    }

    /// Complete a log file and save it to the given directory
    pub async fn finish(&mut self, dir_path: &PathBuf) -> anyhow::Result<()> {
        let new_file_name = format!("{}to{}.log", self.from_ms_index, self.to_ms_index);
        let new_file_path = dir_path.join(&new_file_name);
        let old_file_path = dir_path.join(&self.filename);
        if let Err(e) = tokio::fs::rename(old_file_path, new_file_path).await {
            self.maybe_corrupted = true;
            bail!(e)
        };
        if let Err(e) = self.file.get_mut().sync_all().await {
            self.maybe_corrupted = true;
            bail!(e)
        };
        Ok(())
    }

    /// Append a new line to the log file
    pub async fn append_line(&mut self, line: &Vec<u8>) -> anyhow::Result<()> {
        // append to the file
        if let Err(e) = self.file.write_all(line.as_ref()).await {
            self.maybe_corrupted = true;
            bail!(
                "Unable to append milestone data line into the log file: {}, error: {}",
                self.filename,
                e
            );
        };
        self.to_ms_index += 1;
        // update bytes size length;
        self.len += line.len() as u64;
        Ok(())
    }
    /// Fetch the next milestone data from the log file.
    /// Note: this supposed to be used by importer
    pub async fn next(&mut self) -> Result<Option<MilestoneData>, std::io::Error> {
        if self.maybe_corrupted {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Cannot fetch next milestone data from maybe corrupted LogFile",
            ));
        }
        if self.len == 0 {
            self.finished = true;
            return Ok(None);
        }
        let mut milestone_data_line: String = String::new();
        match self.file.read_line(&mut milestone_data_line).await {
            Ok(n) => {
                if n == 0 {
                    self.finished = true;
                    return Ok(None);
                }
                let milestone_data: MilestoneData = serde_json::from_str(&milestone_data_line).map_err(|e| {
                    self.maybe_corrupted = true;
                    let error_fmt = format!("Unable to deserialize milestone data bytes. Error: {}", e);
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error_fmt)
                })?;
                self.len -= milestone_data_line.len() as u64;
                Ok(Some(milestone_data))
            }
            Err(err) => {
                self.maybe_corrupted = true;
                return Err(err);
            }
        }
    }

    /// Get the file length
    pub fn len(&self) -> u64 {
        self.len
    }

    fn set_finished(&mut self) {
        self.finished = true;
    }

    /// Get the file milestone range
    pub fn milestones_range(&self) -> u32 {
        self.to_ms_index - self.from_ms_index
    }
    /// Get the file starting milestone index
    pub fn from_ms_index(&self) -> u32 {
        self.from_ms_index
    }

    /// Get the file ending milestone index
    pub fn to_ms_index(&self) -> u32 {
        self.to_ms_index
    }
}
/// Archiver state
pub struct Archiver {
    service: Service,
    dir_path: PathBuf,
    logs: Vec<LogFile>,
    max_log_size: u64,
    cleanup: Vec<u32>,
    processed: Vec<std::ops::Range<u32>>,
    milestones_data: BinaryHeap<Ascending<MilestoneData>>,
    oneshot: Option<tokio::sync::oneshot::Receiver<u32>>,
    keyspace: ChronicleKeyspace,
    retries_per_query: usize,
    solidifiers_count: u8,
    handle: Option<ArchiverHandle>,
    inbox: ArchiverInbox,
}
impl Archiver {
    /// Take the held archiver handle, leaving None in its place
    pub fn take_handle(&mut self) -> Option<ArchiverHandle> {
        self.handle.take()
    }
}
impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for ArchiverBuilder {}

/// implementation of builder
impl Builder for ArchiverBuilder {
    type State = Archiver;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(ArchiverHandle { tx });
        let inbox = ArchiverInbox { rx };
        let dir_path = self.dir_path.expect("Expected log dictionary path");
        Self::State {
            service: Service::new(),
            dir_path,
            logs: Vec::new(),
            cleanup: Vec::with_capacity(2),
            max_log_size: self.max_log_size.unwrap_or(MAX_LOG_SIZE),
            processed: Vec::new(),
            keyspace: self.keyspace.unwrap(),
            solidifiers_count: self.solidifiers_count.unwrap(),
            milestones_data: std::collections::BinaryHeap::new(),
            oneshot: self.oneshot,
            retries_per_query: self.retries_per_query.unwrap_or(10),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Archiver
impl Name for Archiver {
    fn set_name(mut self) -> Self {
        self.service.update_name("Archiver".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> AknShutdown<Archiver> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Archiver, status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Archiver(_state.service.clone(), status));
        let _ = self.send(event);
    }
}
impl TryFrom<PathBuf> for LogFile {
    type Error = anyhow::Error;
    fn try_from(file_path: PathBuf) -> Result<Self, Self::Error> {
        if let Some(filename) = file_path.file_stem() {
            let filename = filename
                .to_str()
                .ok_or(anyhow::anyhow!("Invalid filename!"))?
                .to_owned();
            let split = filename.split("to").collect::<Vec<_>>();
            anyhow::ensure!(split.len() == 2, "Invalid filename!");
            let (from_ms_index, to_ms_index) = (split[0].parse()?, split[1].parse()?);
            let std_file = std::fs::OpenOptions::new().write(false).read(true).open(file_path)?;
            let len = std_file.metadata()?.len();
            let file = tokio::fs::File::from_std(std_file);
            Ok(LogFile {
                len,
                filename,
                from_ms_index,
                to_ms_index,
                upper_ms_limit: to_ms_index,
                file: BufReader::new(file),
                maybe_corrupted: false,
                finished: false,
            })
        } else {
            anyhow::bail!("File path does not point to a file!");
        }
    }
}
