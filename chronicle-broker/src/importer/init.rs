// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use anyhow::anyhow;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> Init<BrokerHandle<H>> for Importer {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        info!(
            "{} is Initializing, with permanode keyspace: {}",
            self.get_name(),
            self.default_keyspace.name()
        );
        if let Some(supervisor) = supervisor {
            self.service.update_status(ServiceStatus::Initializing);
            let event = BrokerEvent::Children(BrokerChild::Importer(self.service.clone(), Ok(()), self.parallelism));
            supervisor.send(event).ok();
            let log_file = LogFile::try_from(self.file_path.clone()).map_err(|e| {
                error!("Unable to create LogFile. Error: {}", e);
                Need::Abort
            })?;
            let from = log_file.from_ms_index();
            let to = log_file.to_ms_index();
            let log_file_size = log_file.len();
            let importer_session = ImporterSession::ProgressBar {
                log_file_size,
                from_ms: from,
                to_ms: to,
                ms_bytes_size: 0,
                milestone_index: 0,
                skipped: true,
            };
            supervisor.send(BrokerEvent::Importer(importer_session)).ok();
            // fetch sync data from the keyspace
            if self.resume {
                let sync_range = SyncRange { from, to };
                self.sync_data = SyncData::try_fetch(&self.default_keyspace, &sync_range, 10)
                    .await
                    .map_err(|e| {
                        error!("Unable to fetch SyncData {}", e);
                        Need::Abort
                    })?;
            }
            self.log_file.replace(log_file);
            self.init_importing(supervisor).await.map_err(|e| {
                error!("Unable to init importing process. Error: {}", e);
                Need::Abort
            })?;
            status
        } else {
            Err(Need::Abort)
        }
    }
}

impl Importer {
    async fn init_importing<H: ChronicleBrokerScope>(&mut self, supervisor: &BrokerHandle<H>) -> anyhow::Result<()> {
        for _ in 0..self.parallelism {
            if let Some(milestone_data) = self.next_milestone_data(supervisor).await? {
                let milestone_index = milestone_data.milestone_index();
                let mut iterator = milestone_data.into_iter();
                self.insert_some_messages(milestone_index, &mut iterator)?;
                self.in_progress_milestones_data.insert(milestone_index, iterator);
            } else {
                self.eof = true;
                break;
            }
        }
        Ok(())
    }
    pub(crate) async fn next_milestone_data<H: ChronicleBrokerScope>(
        &mut self,
        supervisor: &BrokerHandle<H>,
    ) -> anyhow::Result<Option<MilestoneData>> {
        let log_file = self
            .log_file
            .as_mut()
            .ok_or_else(|| anyhow!("No LogFile in importer state"))?;
        let mut scan_budget: usize = 100;
        loop {
            let pre_len = log_file.len();
            if let Some(milestone_data) = log_file.next().await? {
                let milestone_index = milestone_data.milestone_index();
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
                    // skip this synced milestone data
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
    pub(crate) fn imported<H: ChronicleBrokerScope>(
        supervisor: &BrokerHandle<H>,
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
        supervisor.send(BrokerEvent::Importer(importer_session)).ok();
    }
    pub(crate) fn insert_some_messages(
        &mut self,
        milestone_index: u32,
        milestone_data: &mut IntoIter<MessageId, FullMessage>,
    ) -> anyhow::Result<()> {
        let importer_handle = self
            .handle
            .clone()
            .ok_or_else(|| anyhow!("No importer handle available!"))?;
        let keyspace = self.get_keyspace();
        let inherent_worker =
            MilestoneDataWorker::new(importer_handle, keyspace, milestone_index, self.retries_per_query);
        for _ in 0..self.parallelism {
            if let Some((message_id, FullMessage(message, metadata))) = milestone_data.next() {
                // Insert the message
                self.insert_message_with_metadata(&inherent_worker, message_id, message, metadata)?;
            } else {
                // break for loop
                break;
            }
        }
        Ok(())
    }
}
