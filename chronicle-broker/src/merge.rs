// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::MilestoneData;
use anyhow::{anyhow, bail};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fmt::Display,
    ops::{Deref, DerefMut, Range},
    path::PathBuf,
};
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};

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
    #[error("File exceeds max file size of {max}: {path}")]
    TooBig { max: u64, path: PathBuf },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl LogFileError {
    fn additional_info(&self) -> &'static str {
        match self {
            LogFileError::EmptyFile(_) => "Empty files will be removed by the merger.",
            LogFileError::MissingMilestones { .. } => {
                "Either milestone are missing from a file or there is a gap between files.
                The merger will fill this gap if the milestones are provided later."
            }
            LogFileError::ExtraMilestones { .. } => {
                "Extra milestones were found in a file which were not declared by the file name.
                The merger will ignore this file if validation level is Full or Light.
                For JustInTime validation, this will be reported as an `OutsideMilestone`."
            }
            LogFileError::OutsideMilestone { .. } => {
                "A milestone was found in a file which was not expected.
                The merger will not use this milestone."
            }
            LogFileError::DuplicateMilestone { .. } => {
                "A duplicate milestone was found. The merger will not use this milestone."
            }
            LogFileError::MalformattedMilestone { .. } => {
                "A milestone was determined to be malformatted, i.e could not be deserialized properly.
                The merger will not use this file if validation level is Full or Light.
                For JustInTime validation, the merger will ignore the rest of the file after this."
            }
            LogFileError::InvalidRange { .. } => {
                "This file has an invalid range defined in its file name. The file will not be used."
            }
            LogFileError::TooBig { .. } => {
                "This file exceeds the requested maximum file size.
                The merger will skip this file."
            }
            LogFileError::Other(_) => "An unknown error occurred.",
        }
    }
}

/// Defines levels of validation checking for log files during a merge
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
    #[allow(unused)]
    milestone: Value,
    #[allow(unused)]
    messages: Value,
    #[allow(unused)]
    pending: Value,
    #[allow(unused)]
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
    file: File,
    len: u64,
    pub err: bool,
    pub finalized: bool,
}

impl LogFile {
    pub fn new(start: u32, end: u32, file_path: PathBuf, file: File, len: u64) -> Self {
        Self {
            file,
            len,
            start,
            end,
            err: file_path.extension().map(|ext| ext == "err").unwrap_or(false),
            finalized: file_path.extension().map(|ext| ext == "fin").unwrap_or(false),
            file_path,
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

    pub async fn verify(
        &mut self,
        level: ValidationLevel,
        progress_bar: &mut Option<ProgressBar>,
    ) -> Result<(), LogFileError> {
        if self.finalized {
            return Ok(());
        }
        if self.len == 0 {
            self.err = true;
            return Err(LogFileError::EmptyFile(self.file_path.clone()));
        }
        if self.start >= self.end {
            self.err = true;
            return Err(LogFileError::InvalidRange {
                range: self.start..self.end,
                path: self.file_path.clone(),
            });
        }
        match level {
            ValidationLevel::Light | ValidationLevel::Full => {
                if let Some(pb) = progress_bar.as_mut() {
                    pb.set_position(0);
                    pb.set_length(self.len);
                    pb.set_message(format!("Validating {}", self.file_path.to_string_lossy()));
                }
                let path = self.file_path.clone();
                let reader = BufReader::new(&mut self.file);
                let mut est_idx = self.start;
                let mut lines = reader.lines();
                let mut extra = 0;
                while let Some(line) = lines.next_line().await.map_err(|e| anyhow!(e))? {
                    // If we've exceeded our claimed range, just add up the extras
                    if est_idx >= self.end {
                        extra += 1;
                        continue;
                    } else if est_idx < self.start {
                        extra += 1;
                        est_idx += 1;
                        continue;
                    }
                    let milestone_index = match level {
                        ValidationLevel::Light => match serde_json::from_str::<LightMilestoneData>(&line) {
                            Ok(milestone) => milestone.milestone_index(),
                            Err(_) => {
                                self.err = true;
                                return Err(LogFileError::MalformattedMilestone {
                                    milestone: est_idx,
                                    path,
                                });
                            }
                        },
                        ValidationLevel::Full => match serde_json::from_str::<MilestoneData>(&line) {
                            Ok(milestone) => milestone.milestone_index(),
                            Err(_) => {
                                self.err = true;
                                return Err(LogFileError::MalformattedMilestone {
                                    milestone: est_idx,
                                    path,
                                });
                            }
                        },
                        _ => panic!(),
                    };
                    if milestone_index > est_idx {
                        self.err = true;
                        return Err(LogFileError::MissingMilestones {
                            range: est_idx..milestone_index,
                            path,
                        });
                    } else if milestone_index < est_idx {
                        extra += 1;
                        if milestone_index >= self.start && milestone_index < self.end {
                            self.err = true;
                            return Err(LogFileError::DuplicateMilestone {
                                milestone: milestone_index,
                                path,
                            });
                        }
                    }

                    est_idx += 1;
                    if let Some(pb) = progress_bar.as_mut() {
                        pb.inc(line.as_bytes().len() as u64);
                    }
                }
                if extra > 0 {
                    self.err = true;
                    return Err(LogFileError::ExtraMilestones { num: extra, path });
                }
                self.file
                    .seek(tokio::io::SeekFrom::Start(0))
                    .await
                    .map_err(|e| anyhow!(e))?;
                self.end = est_idx;
            }
            _ => (),
        }

        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.file.flush().await?;
        if self.file.metadata().await.is_ok() {
            let new_path = self.file_path.parent().unwrap().join(&format!(
                "{}to{}.{}",
                self.start,
                self.end,
                if self.err {
                    "err"
                } else if self.finalized {
                    "log.fin"
                } else {
                    "log"
                }
            ));
            if self.file_path != new_path {
                tokio::fs::rename(&self.file_path, new_path).await?;
            }
        }
        Ok(())
    }
}

impl std::ops::Drop for LogFile {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| futures::executor::block_on(self.close())).unwrap();
    }
}

/// Sorted log paths with start and end milestones
#[derive(Default)]
pub struct LogPaths(Vec<(u32, u32, PathBuf)>);

impl LogPaths {
    /// Create a new paths structure from a log directory
    pub fn new(logs_dir: &PathBuf, include_finalized: bool) -> anyhow::Result<Self> {
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
        if let Some(dir) = logs_dir.to_str() {
            let paths = glob::glob(&format!("{}/*to*.log", dir))
                .unwrap()
                .filter_map(split_filename)
                .chain(
                    glob::glob(&format!("{}/*.log.active", dir))
                        .unwrap()
                        .filter_map(split_filename),
                );
            let mut paths = if include_finalized {
                paths
                    .chain(
                        glob::glob(&format!("{}/*to*.log.fin", dir))
                            .unwrap()
                            .filter_map(split_filename),
                    )
                    .collect::<Vec<_>>()
            } else {
                paths.collect::<Vec<_>>()
            };
            paths.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            Ok(Self(paths))
        } else {
            bail!("Logs directory is malformatted! Found: {}", logs_dir.to_string_lossy());
        }
    }

    /// Perform validation of the logs defined by these paths
    pub async fn validate(self, max_log_size: u64, progress_bar: bool) -> anyhow::Result<()> {
        let mut progress_bar = progress_bar.then(|| {
            let style = ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg} ({eta})",
                )
                .progress_chars("##-");
            ProgressBar::new(0).with_style(style)
        });
        if let Some(pb) = progress_bar.as_mut() {
            for (_, _, path) in self.0.iter() {
                if let Ok(metadata) = std::fs::metadata(path) {
                    pb.inc_length(metadata.len());
                }
            }
            pb.println("Validating logs...");
        }
        let mut prev_end = None;
        for (start, end, path) in self.0.into_iter().rev() {
            if let Some(prev_end) = prev_end {
                if start > prev_end {
                    let e = LogFileError::MissingMilestones {
                        range: prev_end..start,
                        path: path.clone(),
                    };
                    Self::handle_err(&mut progress_bar, e)?;
                }
            }
            let file = OpenOptions::new().read(true).open(&path).await?;
            let len = file.metadata().await?.len();
            if len > max_log_size {
                let e = LogFileError::TooBig {
                    max: max_log_size,
                    path: path.clone(),
                };
                Self::handle_err(&mut progress_bar, e)?;
            }
            let mut log = LogFile::new(start, end, path.clone(), file, len);
            if let Err(e) = log.verify(ValidationLevel::Full, &mut progress_bar).await {
                Self::handle_err(&mut progress_bar, e)?;
            }
            prev_end = Some(end);
        }
        Ok(())
    }

    fn handle_err(pb: &mut Option<ProgressBar>, e: LogFileError) -> anyhow::Result<()> {
        if let Some(pb) = pb.as_mut() {
            pb.println(format!("Validation Error: {}\n\t{}", e, e.additional_info()));
        } else {
            bail!("Validation Error: {}", e);
        }
        Ok(())
    }

    /// Consume the paths and return an iterator over the inner vector
    pub fn into_iter(self) -> std::iter::Rev<std::vec::IntoIter<(u32, u32, PathBuf)>> {
        self.0.into_iter().rev()
    }
}

impl Deref for LogPaths {
    type Target = Vec<(u32, u32, PathBuf)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LogPaths {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Hold configuration and state for merging log files
pub struct Merger {
    logs_dir: PathBuf,
    max_log_size: u64,
    progress_bar: Option<ProgressBar>,
    backup_dir: Option<PathBuf>,
    validation_level: ValidationLevel,
    exit_on_val_err: bool,
    include_finalized: bool,
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
        include_finalized: bool,
    ) -> anyhow::Result<Self> {
        let progress_bar = progress_bar.then(|| {
            let style = ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg} ({eta})",
                )
                .progress_chars("##-");
            ProgressBar::new(0).with_style(style)
        });
        let backup_dir = backup_logs.then(|| logs_dir.join("backup"));
        Ok(Self {
            logs_dir,
            max_log_size,
            progress_bar,
            backup_dir,
            validation_level,
            exit_on_val_err,
            include_finalized,
        })
    }

    /// Begin cleaning up (merging) log files using the Merger instance
    pub async fn cleanup(mut self) -> anyhow::Result<()> {
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.println("Merging logs with the following configuration:");
            pb.println(format!(" - validation level: {}", self.validation_level));
            pb.println(format!(" - backup: {}", self.backup_dir.is_some()));
            pb.println(format!(" - exit on validation err: {}", self.exit_on_val_err));
            pb.println(format!(" - include finalized: {}", self.include_finalized));
        }
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.println(format!("Gathering log files from {}", self.logs_dir.to_string_lossy()));
        }
        let mut paths = LogPaths::new(&self.logs_dir, self.include_finalized)?;
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
            while let Some((start, end, path)) = paths.pop() {
                let mut writer = self.open_write(&path, start, end).await?;
                if let Err(e) = writer
                    .verify(
                        match self.validation_level {
                            ValidationLevel::Basic => ValidationLevel::Basic,
                            _ => ValidationLevel::Full,
                        },
                        &mut self.progress_bar,
                    )
                    .await
                {
                    match e {
                        LogFileError::EmptyFile(_) => {
                            tokio::fs::remove_file(path).await?;
                        }
                        _ => {
                            writer.err = true;
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
            while let Some((start, end, path)) = paths.pop() {
                // The previous file and this one match up
                // or there is an overlap between this log and the previous one
                if start <= writer.end {
                    if start < writer.end {
                        if let Some(pb) = self.progress_bar.as_ref() {
                            pb.println(format!("Found overlap in logs from {} to {}", start, writer.end));
                        }
                    }
                    writer = self.merge(start, end, path, writer).await?;

                // There is a gap in the logs
                } else if start > writer.end {
                    if let Some(pb) = self.progress_bar.as_ref() {
                        pb.println(format!("Found gap in logs from {} to {}", writer.end, start));
                    }
                    writer = self.open_write(&path, start, end).await?;
                    if let Err(e) = writer
                        .verify(
                            match self.validation_level {
                                ValidationLevel::Basic => ValidationLevel::Basic,
                                _ => ValidationLevel::Full,
                            },
                            &mut self.progress_bar,
                        )
                        .await
                    {
                        match e {
                            LogFileError::EmptyFile(_) => {
                                tokio::fs::remove_file(path).await?;
                            }
                            _ => {
                                writer.err = true;
                                self.handle_error(e, writer.len())?;
                            }
                        }
                    }
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

    async fn merge(&mut self, start: u32, end: u32, path: PathBuf, mut active: LogFile) -> anyhow::Result<LogFile> {
        let mut consumed_file = self.open_read(&path, start, end).await?;
        let total_bytes = consumed_file.len();
        if let Err(e) = consumed_file
            .verify(self.validation_level, &mut self.progress_bar)
            .await
        {
            match e {
                LogFileError::EmptyFile(_) => {
                    tokio::fs::remove_file(&path).await?;
                }
                _ => {
                    return self.handle_error(e, total_bytes).map(|_| active);
                }
            }
            return Ok(active);
        }

        let mut buf_reader = BufReader::new(&mut consumed_file.file);
        let mut line_buffer = String::new();
        let mut milestone_index = start;
        let mut total_read_bytes = 0;
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.set_position(0);
            pb.set_length(total_bytes);
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
                                if idx < start || idx >= end {
                                    consumed_file.err = true;
                                    let err = LogFileError::OutsideMilestone { milestone: idx, path };
                                    return self.handle_error(err, total_bytes - total_read_bytes).map(|_| active);
                                } else if milestone_index < idx {
                                    consumed_file.err = true;
                                    let err = LogFileError::MissingMilestones {
                                        range: milestone_index..idx,
                                        path,
                                    };
                                    return self.handle_error(err, total_bytes - total_read_bytes).map(|_| active);
                                } else if milestone_index > idx {
                                    consumed_file.err = true;
                                    let err = LogFileError::DuplicateMilestone { milestone: idx, path };
                                    return self.handle_error(err, total_bytes - total_read_bytes).map(|_| active);
                                }
                            } else {
                                consumed_file.err = true;
                                let err = LogFileError::MalformattedMilestone {
                                    milestone: milestone_index,
                                    path,
                                };
                                return self.handle_error(err, total_bytes - total_read_bytes).map(|_| active);
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
                            active.finalized = true;
                            // If we read more than just a single line from the file
                            if total_read_bytes != bytes as u64 {
                                // Create a new file to funnel the remainder of the milestones to
                                active = self.create_active(milestone_index).await?;
                                // Add the line we just read
                                active.append_line(&ms_line).await?;

                            // Otherwise we shouldn't copy it line-by-line, just set the active file
                            } else {
                                // Drop our reader file so we can access it as a writer
                                drop(consumed_file);
                                // Drop the writer ahead of reassignment so we don't conflict names
                                drop(active);
                                // We already validated the file above, so it's not needed here
                                // Just return the new active file
                                return Ok(self.open_write(&path, start, end).await?);
                            }
                            if let Some(pb) = self.progress_bar.as_mut() {
                                pb.inc(bytes as u64);
                            }
                        }
                    }
                    milestone_index += 1;
                }
                Err(e) => {
                    return self.handle_error(e, total_bytes - total_read_bytes).map(|_| active);
                }
            }
        }
        match self.validation_level {
            ValidationLevel::Basic | ValidationLevel::JustInTime => {
                if milestone_index < end {
                    consumed_file.err = true;
                    let err = LogFileError::MissingMilestones {
                        range: milestone_index..end,
                        path,
                    };
                    return self.handle_error(err, total_bytes - total_read_bytes).map(|_| active);
                } else if milestone_index > end {
                    consumed_file.err = true;
                    let err = LogFileError::ExtraMilestones {
                        num: milestone_index - end,
                        path,
                    };
                    return self.handle_error(err, total_bytes - total_read_bytes).map(|_| active);
                }
            }
            _ => (),
        }
        Ok(active)
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
        let active_file_path = self.logs_dir.join(&format!("{}.log.active", start));
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
        let file_path = self.logs_dir.join(&format!("{}.log.active", milestone_index));
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
