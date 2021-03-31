// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Archiver {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        status?;
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                ArchiverEvent::MilestoneData(milestone_data) => {
                    info!(
                        "Archiver received milestone data for index: {}",
                        milestone_data.milestone_index()
                    );
                    let milestone_index = milestone_data.milestone_index();
                    let mut milestone_data_line = serde_json::to_string(&milestone_data).unwrap();
                    milestone_data_line.push('\n');
                    let mut finished_log_file_i;
                    // check the logs files to find if any has already existing log file
                    if let Some((i, log_file)) = self
                        .logs
                        .iter_mut()
                        .enumerate()
                        .find(|(_, log)| log.to_ms_index == milestone_index)
                    {
                        // discard if the log_file reached an upper limit
                        if log_file.upper_ms_limit == milestone_index {
                            finished_log_file_i = Some(i);
                            Self::finish_log_file(log_file, &self.dir_path).await?;
                        } else {
                            // append milestone data to the log file if the file_size still less than max limit
                            if (milestone_data_line.len() as u32) + log_file.len() < MAX_LOG_SIZE {
                                Self::append(log_file, &milestone_data_line, milestone_index, &self.keyspace).await?;
                                // check if now the log_file reached an upper limit to finish the file
                                if log_file.upper_ms_limit == milestone_index {
                                    finished_log_file_i = Some(i);
                                    Self::finish_log_file(log_file, &self.dir_path).await?;
                                } else {
                                    finished_log_file_i = None;
                                }
                            } else {
                                // Finish it ;
                                finished_log_file_i = Some(i);
                                Self::finish_log_file(log_file, &self.dir_path).await?;
                                // check if the milestone_index already belongs to an existing processed logs
                                if !self.processed.iter().any(|r| r.contains(&milestone_index)) {
                                    // create new file
                                    info!(
                                        "{} hits filesize limit: {} bytes, contains: {} milestones data",
                                        log_file.filename,
                                        log_file.len(),
                                        log_file.milestones_range()
                                    );
                                    info!(
                                        "Creating new log file starting from milestone index: {}",
                                        milestone_index
                                    );
                                    self.create_and_append(milestone_index, &milestone_data_line).await?;
                                    // adjust i as we just created and pushed new file to logs;
                                    finished_log_file_i = Some(i);
                                }
                            }
                        }
                    } else {
                        finished_log_file_i = None;
                        // check if the milestone_index already belongs to an existing processed files/ranges;
                        if !self.processed.iter().any(|r| r.contains(&milestone_index)) {
                            info!(
                                "Creating new log file starting from milestone index: {}",
                                milestone_index
                            );
                            self.create_and_append(milestone_index, &milestone_data_line).await?;
                            self.logs.sort_by(|a, b| a.from_ms_index.cmp(&b.from_ms_index));
                        };
                    };
                    // remove finished log file
                    if let Some(i) = finished_log_file_i {
                        let log_file = self.logs.remove(i);
                        self.push_to_processed(log_file);
                        self.logs.sort_by(|a, b| a.from_ms_index.cmp(&b.from_ms_index));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Archiver {
    async fn create_and_append(&mut self, milestone_index: u32, milestone_data_line: &str) -> Result<(), Need> {
        let mut log_file = LogFile::create(&self.dir_path, milestone_index).await.map_err(|e| {
            error!("{}", e);
            return Need::Abort;
        })?;
        Self::append(&mut log_file, milestone_data_line, milestone_index, &self.keyspace).await?;
        self.logs.push(log_file);
        Ok(())
    }
    fn push_to_processed(&mut self, log_file: LogFile) {
        let r = std::ops::Range {
            start: log_file.from_ms_index,
            end: log_file.to_ms_index,
        };
        self.processed.push(r);
        self.processed.sort_by(|a, b| b.start.cmp(&a.start));
    }
    async fn append(
        log_file: &mut LogFile,
        milestone_data_line: &str,
        ms_index: u32,
        keyspace: &PermanodeKeyspace,
    ) -> Result<(), Need> {
        log_file.append_line(&milestone_data_line).await.map_err(|e| {
            error!("{}", e);
            return Need::Abort;
        })?;
        // insert into the DB, without caring about the response
        let sync_key = permanode_common::Synckey;
        let synced_record = SyncRecord::new(MilestoneIndex(ms_index), None, Some(0));
        keyspace
            .insert(&sync_key, &synced_record)
            .consistency(Consistency::One)
            .build()
            .send_local(InsertWorker::boxed(keyspace.clone(), sync_key, synced_record));
        Ok(())
    }
    async fn finish_log_file(log_file: &mut LogFile, dir_path: &PathBuf) -> Result<(), Need> {
        log_file.finish(dir_path).await.map_err(|e| {
            error!("{}", e);
            return Need::Abort;
        })?;
        Ok(())
    }
}
