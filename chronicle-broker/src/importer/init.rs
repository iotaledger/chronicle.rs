// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> Init<BrokerHandle<H>> for Importer {
    async fn init(&mut self, status: Result<(), Need>, _supervisor: &mut Option<BrokerHandle<H>>) -> Result<(), Need> {
        info!(
            "{} is Initializing, with permanode keyspace: {}",
            self.get_name(),
            self.default_keyspace.name()
        );
        self.service.update_status(ServiceStatus::Initializing);
        let event = BrokerEvent::Children(BrokerChild::Importer(self.service.clone(), Ok(())));
        let _ = _supervisor
            .as_mut()
            .expect("Importer expected BrokerHandle")
            .send(event);
        let log_file = LogFile::try_from(self.file_path.clone()).map_err(|e| {
            error!("Unable to create LogFile. Error: {}", e);
            Need::Abort
        })?;

        // fetch sync data from the keyspace
        if self.resume {
            let from = log_file.from_ms_index();
            let to = log_file.to_ms_index();
            let sync_range = SyncRange { from, to };
            self.sync_data = SyncData::try_fetch(&self.default_keyspace, &sync_range, 10)
                .await
                .map_err(|e| {
                    error!("Unable to fetch SyncData {}", e);
                    Need::Abort
                })?;
        }
        self.log_file.replace(log_file);
        self.init_importing().await.map_err(|e| {
            error!("Unable to init importing process. Error: {}", e);
            Need::Abort
        })?;
        status
    }
}

impl Importer {
    async fn init_importing(&mut self) -> Result<(), std::io::Error> {
        for _ in 0..self.parallelism {
            if let Some(milestone_data) = self.next_milestone_data().await? {
                let milestone_index = milestone_data.milestone_index();
                let iterator = self.insert_some_messages(milestone_index, milestone_data.into_iter());
                self.in_progress_milestones_data.insert(milestone_index, iterator);
            } else {
                self.eof = true;
                break;
            }
        }
        Ok(())
    }
    pub(crate) async fn next_milestone_data(&mut self) -> Result<Option<MilestoneData>, std::io::Error> {
        if let Some(ref mut log_file) = self.log_file.as_mut() {
            let mut scan_budget: usize = 100;
            loop {
                if let Some(milestone_data) = log_file.next().await? {
                    let milestone_index = milestone_data.milestone_index();
                    if self.resume && self.sync_data.completed.iter().any(|r| r.contains(&milestone_index)) {
                        // skip this synced milestone data
                        if scan_budget > 0 {
                            scan_budget -= 1;
                            warn!(
                                "Skipping imported milestone data for milestone index: {}",
                                milestone_index
                            );
                            tokio::task::yield_now().await;
                        }
                    } else {
                        return Ok(Some(milestone_data));
                    }
                } else {
                    return Ok(None);
                }
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No LogFile in importer state",
            ))
        }
    }
    pub(crate) fn insert_some_messages(
        &mut self,
        milestone_index: u32,
        mut milestone_data: IntoIter<MessageId, FullMessage>,
    ) -> IntoIter<MessageId, FullMessage> {
        let importer_handle = self.handle.clone().expect("Expected handle");
        let keyspace = self.get_keyspace();
        let inherent_worker =
            MilestoneDataWorker::new(importer_handle, keyspace, milestone_index, self.retries_per_query);
        for _ in 0..self.parallelism {
            if let Some((message_id, FullMessage(message, metadata))) = milestone_data.next() {
                // Insert the message
                self.insert_message_with_metadata(&inherent_worker, message_id, message, metadata);
            } else {
                // break for loop
                break;
            }
        }
        milestone_data
    }
}