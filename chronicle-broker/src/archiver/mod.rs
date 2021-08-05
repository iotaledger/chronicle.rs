// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::syncer::Ascending;
use anyhow::{
    anyhow,
    bail,
};
use bee_message::prelude::MilestoneIndex;
use chronicle_common::Wrapper;
use chronicle_storage::access::ChronicleKeyspace;
use std::{
    collections::BinaryHeap,
    convert::TryFrom,
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
};

/// The maximum bytes size for a given log file;
pub const MAX_LOG_SIZE: u64 = u32::MAX as u64;

type UpperLimit = u32;

/// Archiver state
pub struct Archiver {
    dir_path: PathBuf,
    logs: Vec<LogFile>,
    max_log_size: u64,
    cleanup: Vec<u32>,
    processed: Vec<std::ops::Range<u32>>,
    milestones_data: BinaryHeap<Ascending<MilestoneData>>,
    keyspace: ChronicleKeyspace,
    retries_per_query: usize,
    solidifiers_count: u8,
    next: u32,
}

#[build]
pub fn build_archiver(
    keyspace: ChronicleKeyspace,
    max_log_size: Option<u64>,
    solidifiers_count: u8,
    retries_per_query: Option<usize>,
    dir_path: PathBuf,
) -> Archiver {
    Archiver {
        dir_path,
        logs: Vec::new(),
        cleanup: Vec::with_capacity(2),
        max_log_size: max_log_size.unwrap_or(MAX_LOG_SIZE),
        processed: Vec::new(),
        keyspace,
        solidifiers_count,
        milestones_data: std::collections::BinaryHeap::new(),
        retries_per_query: retries_per_query.unwrap_or(10),
        next: 0,
    }
}

#[async_trait]
impl Actor for Archiver {
    type Dependencies = ();
    type Event = ArchiverEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        // create directory first
        if let Err(e) = tokio::fs::create_dir(self.dir_path.clone().into_boxed_path()).await {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                // do nothing
            } else {
                return Err(anyhow::anyhow!("Unable to create log directory, error: {}", e).into());
            }
        };
        let sync_data = SyncData::try_fetch(&self.keyspace, &SyncRange::default(), 3).await?;
        self.next = sync_data.gaps.first().map(|r| r.start).unwrap_or(0);
        info!(
            "Archiver will write ahead log files for new incoming data starting with milestone {}",
            self.next
        );
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(event) = rt.next_event().await {
            match event {
                ArchiverEvent::Close(milestone_index) => {
                    // to prevent overlap, we ensure to only close syncer milestone_index when it's less than next
                    if milestone_index < self.next {
                        self.close_log_file(milestone_index).await?;
                    }
                }
                ArchiverEvent::MilestoneData(milestone_data, opt_upper_limit) => {
                    info!(
                        "Archiver received milestone data for index: {}, upper_ms_limit: {:?}",
                        milestone_data.milestone_index(),
                        opt_upper_limit
                    );
                    match milestone_data.created_by() {
                        CreatedBy::Incoming | CreatedBy::Expected => {
                            self.milestones_data.push(Ascending::new(milestone_data));
                            while let Some(ms_data) = self.milestones_data.pop() {
                                let ms_index = ms_data.milestone_index();
                                if self.next.eq(&ms_index) {
                                    self.handle_milestone_data(ms_data.into_inner(), opt_upper_limit)
                                        .await?;
                                    self.next += 1;
                                } else if ms_index > self.next {
                                    // Safety check to prevent potential rare race condition
                                    // check if we buffered too much.
                                    if self.milestones_data.len() > self.solidifiers_count as usize {
                                        error!("Identified gap in the new incoming data: {}..{}", self.next, ms_index);
                                        // Close the file which we're unable atm to append on top.
                                        self.close_log_file(self.next).await?;
                                        // this supposed to create new file
                                        self.handle_milestone_data(ms_data.into_inner(), opt_upper_limit)
                                            .await?;
                                        // reset next
                                        self.next = ms_index + 1;
                                    } else {
                                        self.milestones_data.push(ms_data);
                                        break;
                                    }
                                } else {
                                    warn!("Expected: {}, Dropping milestone_data: {}, as the syncer will eventually fill it up", self.next, ms_index);
                                }
                            }
                        }
                        CreatedBy::Syncer | CreatedBy::Exporter => {
                            // to prevent overlap, we ensure to only handle syncer milestone_data when it's less than
                            // next
                            if milestone_data.milestone_index() < self.next {
                                // handle syncer milestone data;
                                self.handle_milestone_data(milestone_data, opt_upper_limit).await?;
                                // it overlaps with the incoming flow.
                            } else if milestone_data.milestone_index() == self.next {
                                // we handle the milestone_data from syncer as Incoming without upper_ms_limit
                                self.handle_milestone_data(milestone_data, None).await?;
                                self.next += 1;
                            } else {
                                // we received a futuristic milestone_data from syncer.
                                self.milestones_data.push(Ascending::new(milestone_data));
                            }
                        }
                    }
                }
            }
        }
        for log in self.logs.iter_mut() {
            if let Err(e) = log.finish(&self.dir_path).await {
                info!("Unable to finish in progress log file: {}, error: {}", log.filename, e);
            } else {
                info!("Finished in progress log file: {}", log.filename);
            };
        }
        Ok(())
    }
}

impl Archiver {
    async fn close_log_file(&mut self, milestone_index: u32) -> anyhow::Result<()> {
        if let Some((i, log_file)) = self
            .logs
            .iter_mut()
            .enumerate()
            .find(|(_, log)| log.to_ms_index == milestone_index)
        {
            Self::finish_log_file(log_file, &self.dir_path).await?;
            // remove finished log file
            let log_file = self.logs.remove(i);
            self.push_to_processed(log_file);
        };
        Ok(())
    }
    async fn handle_milestone_data(
        &mut self,
        milestone_data: MilestoneData,
        mut opt_upper_limit: Option<u32>,
    ) -> anyhow::Result<()> {
        let milestone_index = milestone_data.milestone_index();
        let mut milestone_data_json = serde_json::to_string(&milestone_data).unwrap();
        milestone_data_json.push('\n');
        let milestone_data_line: Vec<u8> = milestone_data_json.into();
        // check the logs files to find if any has already existing log file
        if let Some(log_file) = self
            .logs
            .iter_mut()
            .find(|log| log.to_ms_index == milestone_index && log.upper_ms_limit > milestone_index)
        {
            // append milestone data to the log file if the file_size still less than max limit
            if (milestone_data_line.len() as u64) + log_file.len() < self.max_log_size {
                Self::append(
                    log_file,
                    &milestone_data_line,
                    milestone_index,
                    &self.keyspace,
                    self.retries_per_query,
                )
                .await?;
                // check if now the log_file reached an upper limit to finish the file
                if log_file.upper_ms_limit == log_file.to_ms_index {
                    self.cleanup.push(log_file.from_ms_index);
                    Self::finish_log_file(log_file, &self.dir_path).await?;
                }
            } else {
                // push it into cleanup
                self.cleanup.push(log_file.from_ms_index);
                // Finish it;
                Self::finish_log_file(log_file, &self.dir_path).await?;
                info!(
                    "{} hits filesize limit: {} bytes, contains: {} milestones data",
                    log_file.filename,
                    log_file.len(),
                    log_file.milestones_range()
                );
                // check if the milestone_index already belongs to an existing processed logs
                let not_processed = !self.processed.iter().any(|r| r.contains(&milestone_index));
                if not_processed {
                    // create new file
                    info!(
                        "Creating new log file starting from milestone index: {}",
                        milestone_index
                    );
                    opt_upper_limit.replace(log_file.upper_ms_limit);
                    self.create_and_append(milestone_index, &milestone_data_line, opt_upper_limit)
                        .await?;
                }
            }
        } else {
            // check if the milestone_index already belongs to an existing processed files/ranges;
            let mut already_processed = false;
            if let Some(idx) = self.processed.iter().position(|r| r.contains(&milestone_index)) {
                let processed = self.processed.get(idx).unwrap();
                let filename = format!("{}to{}.log", processed.start, processed.end);
                let file_path = self.dir_path.join(&filename);
                if !file_path.exists() {
                    self.processed.remove(idx);
                } else {
                    already_processed = true;
                }
            }
            if !already_processed {
                info!(
                    "Creating new log file starting from milestone index: {}",
                    milestone_index
                );
                self.create_and_append(milestone_index, &milestone_data_line, opt_upper_limit)
                    .await?;
            };
        };
        // remove finished log file
        while let Some(from_ms_index) = self.cleanup.pop() {
            let i = self
                .logs
                .iter()
                .position(|item| item.from_ms_index == from_ms_index)
                .unwrap();
            let log_file = self.logs.remove(i);
            self.push_to_processed(log_file);
        }
        Ok(())
    }
    async fn create_and_append(
        &mut self,
        milestone_index: u32,
        milestone_data_line: &Vec<u8>,
        opt_upper_limit: Option<u32>,
    ) -> anyhow::Result<()> {
        let mut log_file = LogFile::create(&self.dir_path, milestone_index, opt_upper_limit).await?;
        Self::append(
            &mut log_file,
            milestone_data_line,
            milestone_index,
            &self.keyspace,
            self.retries_per_query,
        )
        .await?;
        // check if we hit an upper_ms_limit, as this is possible when the log_file only needs 1 milestone data.
        if log_file.upper_ms_limit == log_file.to_ms_index {
            // finish it
            Self::finish_log_file(&mut log_file, &self.dir_path).await?;
            // add it to processed
            self.push_to_processed(log_file);
        } else {
            // push it to the active log files
            self.logs.push(log_file);
            self.logs.sort_by(|a, b| a.from_ms_index.cmp(&b.from_ms_index));
            // iterate in reverse
            let mut log_files = self.logs.iter_mut().rev();
            // extract the last log_file
            if let Some(mut prev_log) = log_files.next() {
                // iterate in reverse to adjust the upper_ms_limit
                while let Some(l) = log_files.next() {
                    if l.upper_ms_limit > prev_log.from_ms_index {
                        l.upper_ms_limit = prev_log.from_ms_index;
                    }
                    // check if the L file needs to be closed
                    if l.upper_ms_limit.eq(&l.to_ms_index) && !l.finished {
                        // push it into cleanup to get removed and pushed to processed
                        self.cleanup.push(l.from_ms_index);
                        // finish the file
                        Self::finish_log_file(l, &self.dir_path).await?;
                    }

                    prev_log = l;
                }
            }
        }
        Ok(())
    }
    fn push_to_processed(&mut self, log_file: LogFile) {
        let r = std::ops::Range {
            start: log_file.from_ms_index,
            end: log_file.to_ms_index,
        };
        info!("Logged Range: {:?}", r);
        self.processed.push(r);
        self.processed.sort_by(|a, b| b.start.cmp(&a.start));
    }
    async fn append(
        log_file: &mut LogFile,
        milestone_data_line: &Vec<u8>,
        ms_index: u32,
        keyspace: &ChronicleKeyspace,
        retries_per_query: usize,
    ) -> anyhow::Result<()> {
        log_file.append_line(&milestone_data_line).await?;
        // insert into the DB, without caring about the response
        let sync_key = chronicle_common::Synckey;
        let synced_record = SyncRecord::new(MilestoneIndex(ms_index), None, Some(0));
        keyspace
            .insert(&sync_key, &synced_record)
            .consistency(Consistency::One)
            .build()?
            .send_local(InsertWorker::boxed(
                keyspace.clone(),
                sync_key,
                synced_record,
                retries_per_query,
            ));
        Ok(())
    }
    async fn finish_log_file(log_file: &mut LogFile, dir_path: &PathBuf) -> anyhow::Result<()> {
        log_file.finish(dir_path).await?;
        log_file.set_finished();
        info!(
            "Finished {}.part, LogFile: {}to{}.log",
            log_file.from_ms_index, log_file.from_ms_index, log_file.to_ms_index
        );
        Ok(())
    }
}

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
