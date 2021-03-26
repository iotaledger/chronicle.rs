// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    application::*,
    solidifier::*,
};
use std::{
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
    io::AsyncWriteExt,
};

mod event_loop;
mod init;
mod terminating;

/// The maximum bytes size for a given log file;
pub const MAX_LOG_SIZE: u32 = u32::MAX;

// Logger builder
builder!(LoggerBuilder { dir_path: PathBuf });

/// LoggerHandle to be passed to the supervisor and solidifers
#[derive(Clone)]
pub struct LoggerHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<LoggerEvent>,
}
/// LoggerInbox is used to recv events from solidifier(s)
pub struct LoggerInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<LoggerEvent>,
}
impl Deref for LoggerHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<LoggerEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for LoggerHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Shutdown for LoggerHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}

pub enum LoggerEvent {
    MilestoneData(MilestoneData),
}

#[derive(Debug)]
pub struct LogFile {
    len: u32,
    filename: String,
    /// Included milestone data
    from_ms_index: u32,
    /// NotIncluded (yet) milestone data
    to_ms_index: u32,
    upper_ms_limit: u32,
    file: File,
    /// Identifier if it had io error
    maybe_corrupted: bool,
}

impl LogFile {
    pub async fn create(dir_path: &PathBuf, milestone_index: u32) -> Result<LogFile, String> {
        let filename = format!("{}.part", milestone_index);
        let file_path = dir_path.join(&filename);
        let file: File = OpenOptions::new()
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .map_err(|e| format!("Unable to create log file: {}, error: {}", filename, e))?;
        Ok(Self {
            len: 0,
            filename,
            from_ms_index: milestone_index,
            to_ms_index: milestone_index,
            upper_ms_limit: u32::MAX,
            file,
            maybe_corrupted: false,
        })
    }
    pub async fn finish(&mut self, dir_path: &PathBuf) -> Result<(), std::io::Error> {
        let new_file_name = format!("{}to{}.log", self.from_ms_index, self.to_ms_index);
        let new_file_path = dir_path.join(&new_file_name);
        let old_file_path = dir_path.join(&self.filename);
        if let Err(e) = tokio::fs::rename(old_file_path, new_file_path).await {
            self.maybe_corrupted = true;
            return Err(e);
        };
        if let Err(e) = self.file.sync_all().await {
            self.maybe_corrupted = true;
            return Err(e);
        };
        Ok(())
    }
    pub async fn append_line(&mut self, line: &str) -> Result<(), String> {
        // append to the file
        if let Err(e) = self.file.write_all(line.as_ref()).await {
            self.maybe_corrupted = true;
            return Err(format!(
                "Unable to append milestone data line into the log file: {}, error: {}",
                self.filename, e
            ));
        };
        self.to_ms_index += 1;
        // update bytes size length;
        self.len += line.len() as u32;
        Ok(())
    }
    pub fn len(&self) -> u32 {
        self.len
    }
    pub fn milestones_range(&self) -> u32 {
        self.to_ms_index - self.from_ms_index
    }
}
// Logger state
pub struct Logger {
    service: Service,
    dir_path: PathBuf,
    logs: Vec<LogFile>,
    processed: Vec<std::ops::Range<u32>>,
    handle: Option<LoggerHandle>,
    inbox: LoggerInbox,
}
impl Logger {
    pub fn take_handle(&mut self) -> Option<LoggerHandle> {
        self.handle.take()
    }
}
impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for LoggerBuilder {}

/// implementation of builder
impl Builder for LoggerBuilder {
    type State = Logger;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(LoggerHandle { tx });
        let inbox = LoggerInbox { rx };
        let dir_path = self.dir_path.expect("Expected log dictionary path");
        Self::State {
            service: Service::new(),
            dir_path,
            logs: Vec::new(),
            processed: Vec::new(),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Logger
impl Name for Logger {
    fn set_name(mut self) -> Self {
        self.service.update_name("logger".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> AknShutdown<Logger> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Logger, status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Logger(_state.service.clone(), status));
        let _ = self.send(event);
    }
}
