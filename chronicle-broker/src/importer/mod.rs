// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    application::{
        BrokerEvent,
        BrokerHandle,
        ImporterSession,
    },
    archiver::LogFile,
};
use std::{
    ops::Range,
    path::PathBuf,
};

type ImporterHandle = UnboundedHandle<ImporterEvent>;

/// Importer events
#[derive(Debug, Clone)]
pub enum ImporterEvent {
    /// The result of an milestone data importer for a given milestone data by its milestone index
    FromMilestoneDataImporter(ScopeId, Result<u32, u32>),
    /// Used by MilestoneData Importer to push their service
    Microservice(ScopeId, Service, Option<ActorResult<()>>),
    /// Shutdown the importer
    Shutdown,
}

impl ShutdownEvent for ImporterEvent {
    fn shutdown_event() -> Self {
        ImporterEvent::Shutdown
    }
}

impl<T> EolEvent<T> for ImporterEvent {
    fn eol_event(scope: ScopeId, service: Service, _actor: T, r: ActorResult<()>) -> Self {
        Self::Microservice(scope, service, Some(r))
    }
}

impl<T> ReportEvent<T> for ImporterEvent {
    fn report_event(scope: ScopeId, service: Service) -> Self {
        Self::Microservice(scope, service, None)
    }
}

/// Importer state
pub struct Importer<T: FilterBuilder> {
    /// The file path of the importer
    file_path: PathBuf,
    /// The log file
    log_file: Option<LogFile>,
    /// LogFile total_size,
    log_file_size: u64,
    /// from milestone index
    from_ms: u32,
    /// to milestone index
    to_ms: u32,
    /// The default Chronicle keyspace
    default_keyspace: ChronicleKeyspace,
    /// The number of retires per query
    retries: u8,
    /// The chronicle id
    chronicle_id: u8,
    /// The number of parallelism
    parallelism: u8,
    /// The resume flag
    resume: bool,
    /// The range of requested milestones to import
    import_range: std::ops::Range<u32>,
    /// The database sync data
    sync_data: SyncData,
    /// In progress milestones data with thier bytes size
    in_progress_milestones_data_bytes_size: HashMap<u32, usize>,
    /// The flag of end of file
    eof: bool,
    /// The user defined actor handle
    uda_handle: <<T::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
    /// filter
    filter_builder: T,
}
impl<T: FilterBuilder> Importer<T> {
    pub(crate) fn new(
        default_keyspace: ChronicleKeyspace,
        file_path: PathBuf,
        resume: bool,
        import_range: Range<u32>,
        uda_handle: <<T::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        filter_builder: T,
        parallelism: u8,
        retries: u8,
    ) -> Self {
        Self {
            file_path,
            log_file: None,
            log_file_size: 0,
            from_ms: 0,
            to_ms: 0,
            default_keyspace,
            parallelism,
            chronicle_id: 0,
            in_progress_milestones_data_bytes_size: HashMap::new(),
            retries,
            resume,
            import_range,
            sync_data: SyncData::default(),
            eof: false,
            uda_handle,
            filter_builder,
        }
    }
}

#[async_trait]
impl<T: FilterBuilder> Actor<BrokerHandle> for Importer<T> {
    type Data = ();
    type Channel = UnboundedChannel<ImporterEvent>;

    async fn init(&mut self, rt: &mut Rt<Self, BrokerHandle>) -> ActorResult<Self::Data> {
        info!(
            "Importer {:?} is Initializing, with permanode keyspace: {}",
            self.file_path,
            self.default_keyspace.name()
        );
        let log_file = LogFile::try_from(self.file_path.clone()).map_err(|e| {
            error!("Unable to create LogFile. Error: {}", e);
            let event = BrokerEvent::ImporterSession(ImporterSession::PathError {
                path: self.file_path.clone(),
                msg: "Invalid LogFile path".into(),
            });
            rt.supervisor_handle().send(event).ok();
            ActorError::aborted(e)
        })?;
        let from = log_file.from_ms_index();
        let to = log_file.to_ms_index();
        self.log_file_size = log_file.len();
        self.from_ms = from;
        self.to_ms = to;
        let importer_session = ImporterSession::ProgressBar {
            log_file_size: self.log_file_size,
            from_ms: from,
            to_ms: to,
            ms_bytes_size: 0,
            milestone_index: 0,
            skipped: true,
        };
        // fetch sync data from the keyspace
        if self.resume {
            let sync_range = SyncRange { from, to };
            self.sync_data = SyncData::try_fetch(&self.default_keyspace, sync_range, 10)
                .await
                .map_err(|e| {
                    error!("Unable to fetch SyncData {}", e);
                    let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                        from_ms: self.from_ms,
                        to_ms: self.to_ms,
                        msg: "failed".into(),
                    });
                    rt.supervisor_handle().send(event).ok();
                    ActorError::aborted(e)
                })?;
        }
        self.log_file.replace(log_file);
        self.init_importing(rt).await?;
        rt.supervisor_handle()
            .send(BrokerEvent::ImporterSession(importer_session))
            .ok();
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, BrokerHandle>, idle_md_importer: Self::Data) -> ActorResult<()> {
        info!("Importer LogFile: {:?} is running", self.file_path);
        // check if it's already EOF and nothing to progress
        if self.eof {
            warn!("Skipped already imported LogFile: {:?}", self.file_path);
            let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                from_ms: self.from_ms,
                to_ms: self.to_ms,
                msg: "ok".into(),
            });
            rt.supervisor_handle().send(event).ok();
            return Ok(());
        }
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ImporterEvent::FromMilestoneDataImporter(scope_id, result) => {
                    match result {
                        Ok(milestone_index) => {
                            info!("Imported milestone data for milestone index: {}", milestone_index);
                            let ms_bytes_size = self
                                .in_progress_milestones_data_bytes_size
                                .remove(&milestone_index)
                                .expect("Expected size-entry for a milestone data");
                            let skipped = false;
                            Self::imported(
                                rt.supervisor_handle(),
                                self.from_ms,
                                self.to_ms,
                                self.log_file_size,
                                milestone_index,
                                ms_bytes_size,
                                skipped,
                            );
                            // Mark the milestone index as synced and logged in the database.
                            self.insert_sync_record(rt, milestone_index).await?;
                            // check if we should process more
                            if !rt.service().is_stopping() {
                                // process one more
                                if let Some(milestone_data) =
                                    self.next_milestone_data(rt.supervisor_handle()).await.map_err(|e| {
                                        error!("Unable to fetch next milestone data. Error: {}", e);
                                        let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                                            from_ms: self.from_ms,
                                            to_ms: self.to_ms,
                                            msg: "failed".into(),
                                        });
                                        rt.supervisor_handle().send(event).ok();
                                        ActorError::aborted(e)
                                    })?
                                {
                                    rt.send(scope_id, milestone_data).await.map_err(|e| {
                                        error!("{}", e);
                                        let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                                            from_ms: self.from_ms,
                                            to_ms: self.to_ms,
                                            msg: "failed".into(),
                                        });
                                        rt.supervisor_handle().send(event).ok();
                                        ActorError::aborted(e)
                                    })?;
                                } else {
                                    // no more milestone data.
                                    if self.in_progress_milestones_data_bytes_size.is_empty() {
                                        // shut it down
                                        info!("Imported the LogFile: {:?}", self.file_path);
                                        rt.stop();
                                    }
                                };
                            }
                        }
                        Err(milestone_index) => {
                            let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                                from_ms: self.from_ms,
                                to_ms: self.to_ms,
                                msg: "failed".into(),
                            });
                            rt.supervisor_handle().send(event).ok();
                            // likely an outage in scylla so we abort
                            return Err(ActorError::aborted_msg(format!(
                                "Unable to import LogFile {:?}",
                                self.file_path,
                            )));
                        }
                    }
                }
                ImporterEvent::Microservice(scope_id, service, r_opt) => {
                    rt.upsert_microservice(scope_id, service);
                    if rt.microservices_stopped() {
                        break;
                    }
                }
                ImporterEvent::Shutdown => {
                    rt.update_status(ServiceStatus::Stopping).await;
                    let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                        from_ms: self.from_ms,
                        to_ms: self.to_ms,
                        msg: "failed".into(),
                    });
                    rt.supervisor_handle().send(event).ok();
                    return Err(ActorError::aborted_msg("Got shutdown in middle of importing session"));
                }
            }
        }
        let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            msg: "ok".into(),
        });
        rt.supervisor_handle().send(event).ok();
        Ok(())
    }
}
impl<T: FilterBuilder> Importer<T> {
    async fn init_importing(
        &mut self,
        rt: &mut Rt<Self, BrokerHandle>,
    ) -> anyhow::Result<<Self as Actor<BrokerHandle>>::Data> {
        for id in 0..self.parallelism {
            if let Some(milestone_data) = self.next_milestone_data(rt.supervisor_handle()).await? {
                let milestone_data_importer = milestone_data_importer::MilestoneDataImporter::new(
                    self.filter_builder.clone(),
                    self.uda_handle.clone(),
                );
                rt.start(format!("{}", id), milestone_data_importer)
                    .await?
                    .send(milestone_data)?;
            } else {
                self.eof = true;
                break;
            }
        }
        Ok(())
    }
    pub(crate) async fn next_milestone_data(
        &mut self,
        supervisor: &BrokerHandle,
    ) -> anyhow::Result<Option<MilestoneData>> {
        let log_file = self
            .log_file
            .as_mut()
            .ok_or_else(|| anyhow!("No LogFile in importer state"))?;
        let mut scan_budget: usize = 100;
        loop {
            let pre_len = log_file.len();
            if let Some(milestone_data) = log_file.next().await? {
                let milestone_index = milestone_data.milestone_index().0;
                let not_in_import_range = !self.import_range.contains(&milestone_index);
                let resume = self.resume && self.sync_data.completed.iter().any(|r| r.contains(&milestone_index));
                if resume || not_in_import_range {
                    warn!(
                        "Skipping imported milestone data for milestone index: {}",
                        milestone_index
                    );
                    let skipped = true;
                    let ms_bytes_size = (pre_len - log_file.len()) as usize;
                    Self::imported(
                        supervisor,
                        log_file.from_ms_index(),
                        log_file.to_ms_index(),
                        self.log_file_size,
                        milestone_index,
                        ms_bytes_size,
                        skipped,
                    );
                    // skip this synced milestone data, todo, maybe remove this, as tokio implemented budget
                    if scan_budget > 0 {
                        scan_budget -= 1;
                    } else {
                        scan_budget = 100;
                        tokio::task::yield_now().await;
                    }
                } else {
                    let ms_bytes_size = (pre_len - log_file.len()) as usize;
                    self.in_progress_milestones_data_bytes_size
                        .insert(milestone_index, ms_bytes_size);
                    return Ok(Some(milestone_data));
                }
            } else {
                return Ok(None);
            }
        }
    }
    pub(crate) fn imported(
        supervisor: &BrokerHandle,
        from_ms: u32,
        to_ms: u32,
        log_file_size: u64,
        milestone_index: u32,
        ms_bytes_size: usize,
        skipped: bool,
    ) {
        let importer_session = ImporterSession::ProgressBar {
            log_file_size,
            from_ms,
            to_ms,
            ms_bytes_size,
            milestone_index,
            skipped,
        };
        supervisor.send(BrokerEvent::ImporterSession(importer_session)).ok();
    }
    async fn insert_sync_record(&mut self, rt: &mut Rt<Self, BrokerHandle>, milestone_index: u32) -> ActorResult<()> {
        let sync_record = SyncRecord::new(
            bee_message::milestone::MilestoneIndex(milestone_index),
            Some(0),
            Some(0),
        );
        self.default_keyspace
            .insert(&sync_record, &())
            .consistency(Consistency::Quorum)
            .build()?
            .worker()
            .with_retries(self.retries as usize)
            .get_local()
            .await
            .map_err(|e| {
                warn!("Scylla cluster is likely going through an outage");
                ActorError::aborted_msg("Unable to insert sync record due to potential outage")
            })
    }
}

mod milestone_data_importer;
