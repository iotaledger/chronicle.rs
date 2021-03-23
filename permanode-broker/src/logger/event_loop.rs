// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Logger {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        status?;
        while let Some(event) = self.inbox.rx.recv().await {
            match event {
                LoggerEvent::MilestoneData(milestone_data) => {
                    let milestone_index = milestone_data.milestone_index();
                    let mut milestone_data_line = serde_json::to_string(&milestone_data).unwrap();
                    milestone_data_line.push('\n');
                    let finished_log_file_i;
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
                            Self::finish_log_file(log_file).await?;
                        } else {
                            // append milestone data to the log file if the file_size still less than max limit
                            if (milestone_data_line.len() as u32) + log_file.len() < MAX_LOG_SIZE {
                                finished_log_file_i = None;
                                // discard the milestone_data_line
                                Self::append(log_file, &milestone_data_line).await?;
                            } else {
                                // Finish it ;
                                finished_log_file_i = Some(i);
                                Self::finish_log_file(log_file).await?;
                                // check if the milestone_index already belongs to an existing processed logs
                                if !self.processed.iter().any(|r| r.contains(&milestone_index)) {
                                    self.creata_and_append(milestone_index, &milestone_data_line).await?;
                                };
                            }
                        }
                    } else {
                        finished_log_file_i = None;
                        // check if the milestone_index already belongs to an existing processed files/ranges;
                        if !self.processed.iter().any(|r| r.contains(&milestone_index)) {
                            self.creata_and_append(milestone_index, &milestone_data_line).await?;
                        };
                    };
                    // remove finished log file
                    if let Some(i) = finished_log_file_i {
                        let log_file = self.logs.remove(i);
                        self.push_to_processed(log_file)
                    }
                }
            }
        }
        Ok(())
    }
}

impl Logger {
    async fn creata_and_append(&mut self, milestone_index: u32, milestone_data_line: &str) -> Result<(), Need> {
        let mut log_file = LogFile::create(&self.dir_path, milestone_index).await.map_err(|e| {
            error!("{}", e);
            return Need::Abort;
        })?;
        Self::append(&mut log_file, milestone_data_line).await?;
        self.logs.push(log_file);
        // Sort logs
        self.logs.sort_by(|a, b| a.from_ms_index.cmp(&b.from_ms_index));
        Ok(())
    }
    fn push_to_processed(&mut self, log_file: LogFile) {
        let r = std::ops::Range {
            start: log_file.from_ms_index,
            end: log_file.to_ms_index,
        };
        self.processed.push(r);
        self.processed.sort_by(|a, b| a.start.cmp(&b.start));
    }
    async fn append(log_file: &mut LogFile, milestone_data_line: &str) -> Result<(), Need> {
        log_file.append_line(&milestone_data_line).await.map_err(|e| {
            error!("{}", e);
            return Need::Abort;
        })?;
        Ok(())
    }
    async fn finish_log_file(log_file: &mut LogFile) -> Result<(), Need> {
        log_file.finish().await.map_err(|e| {
            error!("{}", e);
            return Need::Abort;
        })?;
        Ok(())
    }
}
