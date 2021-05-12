use crate::MilestoneData;
use anyhow::{
    anyhow,
    bail,
};
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fmt::Display,
    ops::Range,
    path::PathBuf,
};
use thiserror::Error;
use tokio::{
    fs::{
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
    file: File,
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

/// Hold configuration and state for merging log files
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

    /// Begin cleaning up (merging) log files using the Merger instance
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
                    if let Some(pb) = self.progress_bar.as_mut() {
                        pb.inc(writer.len());
                    }
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
                    if let Some(pb) = self.progress_bar.as_mut() {
                        pb.inc(writer.len());
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
