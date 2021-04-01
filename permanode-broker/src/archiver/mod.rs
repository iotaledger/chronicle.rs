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

// Archiver builder
builder!(ArchiverBuilder {
    keyspace: PermanodeKeyspace,
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
        todo!()
    }
}
pub type UpperLimit = u32;
pub enum ArchiverEvent {
    MilestoneData(MilestoneData, Option<UpperLimit>),
    Close(u32),
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
    pub async fn append_line(&mut self, line: &Vec<u8>) -> Result<(), String> {
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
// Archiver state
pub struct Archiver {
    service: Service,
    dir_path: PathBuf,
    logs: Vec<LogFile>,
    processed: Vec<std::ops::Range<u32>>,
    keyspace: PermanodeKeyspace,
    handle: Option<ArchiverHandle>,
    inbox: ArchiverInbox,
}
impl Archiver {
    pub fn take_handle(&mut self) -> Option<ArchiverHandle> {
        self.handle.take()
    }
}
impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for ArchiverBuilder {}

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
            processed: Vec::new(),
            keyspace: self.keyspace.unwrap(),
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
impl<H: PermanodeBrokerScope> AknShutdown<Archiver> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Archiver, status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Archiver(_state.service.clone(), status));
        let _ = self.send(event);
    }
}
