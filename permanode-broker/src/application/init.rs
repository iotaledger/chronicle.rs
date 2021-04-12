use super::*;

#[async_trait]
impl<H: PermanodeBrokerScope> Init<H> for PermanodeBroker<H> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let Some(ref mut supervisor) = supervisor {
            supervisor.status_change(self.service.clone());
            // Query sync table
            self.query_sync_table().await?;
            info!("Current: {:#?}", self.sync_data);
            // Get the gap_start
            let gap_start = self.sync_data.gaps.first().unwrap().start;
            // create syncer_builder
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let syncer_handle = SyncerHandle { tx };
            let syncer_inbox = SyncerInbox { rx };
            let (one, recv) = tokio::sync::oneshot::channel();
            let syncer_builder = SyncerBuilder::new()
                .sync_data(self.sync_data.clone())
                .handle(syncer_handle.clone())
                .oneshot(one)
                .inbox(syncer_inbox);
            // create archiver_builder
            let mut archiver = ArchiverBuilder::new()
                .dir_path(self.logs_dir_path.clone())
                .keyspace(self.default_keyspace.clone())
                .solidifiers_count(self.collectors_count)
                .oneshot(recv)
                .build();
            let archiver_handle = archiver.take_handle();
            // start archiver
            tokio::spawn(archiver.start(self.handle.clone()));
            let mut collector_builders: Vec<CollectorBuilder> = Vec::new();
            let mut solidifier_builders: Vec<SolidifierBuilder> = Vec::new();
            let reqwest_client = reqwest::Client::new();
            let config = get_config_async().await;
            for partition_id in 0..self.collectors_count {
                // create collector_builder
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let collector_handle = CollectorHandle { tx };
                let collector_inbox = CollectorInbox { rx };
                self.collector_handles.insert(partition_id, collector_handle.clone());
                let collector_builder = CollectorBuilder::new()
                    .collectors_count(self.collectors_count)
                    .handle(collector_handle)
                    .inbox(collector_inbox)
                    .api_endpoints(config.broker_config.api_endpoints.iter().cloned().collect())
                    .reqwest_client(reqwest_client.clone())
                    .partition_id(partition_id);

                collector_builders.push(collector_builder);
                // create solidifier_builder
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let solidifier_handle = SolidifierHandle { tx };
                let solidifier_inbox = SolidifierInbox { rx };
                self.solidifier_handles.insert(partition_id, solidifier_handle.clone());
                let solidifier_builder = SolidifierBuilder::new()
                    .collectors_count(self.collectors_count)
                    .syncer_handle(syncer_handle.clone())
                    .archiver_handle(archiver_handle.clone().unwrap())
                    .gap_start(gap_start)
                    .keyspace(self.default_keyspace.clone())
                    .handle(solidifier_handle)
                    .inbox(solidifier_inbox)
                    .partition_id(partition_id);
                solidifier_builders.push(solidifier_builder);
            }
            // store copy of syncer_handle in broker state in order to be able to shut it down
            self.syncer_handle.replace(syncer_handle);
            // Finalize and Spawn Syncer
            let syncer = syncer_builder
                .solidifier_handles(self.solidifier_handles.clone())
                .archiver_handle(archiver_handle.unwrap())
                .first_ask(AskSyncer::Complete)
                .build();
            tokio::spawn(syncer.start(self.handle.clone()));
            // Spawn mqtt brokers
            for broker_url in config
                .broker_config
                .mqtt_brokers
                .get(&MqttType::Messages)
                .iter()
                .flat_map(|v| v.iter())
                .cloned()
            {
                if let Some(mqtt) = self.add_mqtt(Messages, MqttType::Messages, broker_url) {
                    tokio::spawn(mqtt.start(self.handle.clone()));
                }
            }
            for broker_url in config
                .broker_config
                .mqtt_brokers
                .get(&MqttType::MessagesReferenced)
                .iter()
                .flat_map(|v| v.iter())
                .cloned()
            {
                if let Some(mqtt) = self.add_mqtt(MessagesReferenced, MqttType::MessagesReferenced, broker_url) {
                    tokio::spawn(mqtt.start(self.handle.clone()));
                }
            }
            // we finalize them
            for collector_builder in collector_builders {
                let collector = collector_builder
                    .solidifier_handles(self.solidifier_handles.clone())
                    .build();
                tokio::spawn(collector.start(self.handle.clone()));
            }
            for solidifier_builder in solidifier_builders {
                let solidifier = solidifier_builder
                    .collector_handles(self.collector_handles.clone())
                    .build();
                tokio::spawn(solidifier.start(self.handle.clone()));
            }
            status
        } else {
            Err(Need::Abort)
        }
    }
}

impl<H: PermanodeBrokerScope> PermanodeBroker<H> {
    pub(crate) async fn query_sync_table(&mut self) -> Result<(), Need> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = self
            .default_keyspace
            .select(&self.sync_range)
            .consistency(Consistency::One)
            .build()
            .map_err(|e| {
                error!("{}", e);
                Need::Abort
            })?
            .send_local(ValueWorker::boxed(
                tx,
                self.default_keyspace.clone(),
                self.sync_range,
                std::marker::PhantomData,
            ));
        let select_response = rx.recv().await;
        match select_response {
            Some(Ok(opt_sync_rows)) => {
                if let Some(mut sync_rows) = opt_sync_rows {
                    // Get the first row, note: the first row is always with the largest milestone_index
                    let SyncRecord {
                        milestone_index,
                        logged_by,
                        ..
                    } = sync_rows.next().unwrap();
                    // push missing row/gap (if any)
                    self.process_gaps(self.sync_range.to, *milestone_index);
                    self.process_rest(&logged_by, *milestone_index, &None);
                    let mut pre_ms = milestone_index;
                    let mut pre_lb = logged_by;
                    // Generate and identify missing gaps in order to fill them
                    while let Some(SyncRecord {
                        milestone_index,
                        logged_by,
                        ..
                    }) = sync_rows.next()
                    {
                        // check if there are any missings
                        self.process_gaps(*pre_ms, *milestone_index);
                        self.process_rest(&logged_by, *milestone_index, &pre_lb);
                        pre_ms = milestone_index;
                        pre_lb = logged_by;
                    }
                    // pre_ms is the most recent milestone we processed
                    // it's also the lowest milestone index in the select response
                    // so anything < pre_ms && anything >= (self.sync_range.from - 1)
                    // (lower provided sync bound) are missing
                    // push missing row/gap (if any)
                    self.process_gaps(*pre_ms, self.sync_range.from - 1);
                    Ok(())
                } else {
                    // Everything is missing as gaps
                    self.process_gaps(self.sync_range.to, self.sync_range.from - 1);
                    Ok(())
                }
            }
            Some(Err(e)) => {
                error!("Unable to query sync table: {}", e);
                let reschedule_after = Need::RescheduleAfter(std::time::Duration::from_secs(5));
                return Err(reschedule_after);
            }
            None => {
                let reschedule_after = Need::RescheduleAfter(std::time::Duration::from_secs(5));
                return Err(reschedule_after);
            }
        }
    }

    fn process_rest(&mut self, logged_by: &Option<u8>, milestone_index: u32, pre_lb: &Option<u8>) {
        if logged_by.is_some() {
            // process logged
            let completed = &mut self.sync_data.completed;
            Self::proceed(completed, milestone_index, pre_lb.is_some());
        } else {
            // process_unlogged
            let unlogged = &mut self.sync_data.synced_but_unlogged;
            Self::proceed(unlogged, milestone_index, pre_lb.is_none());
        }
    }
    fn process_gaps(&mut self, pre_ms: u32, milestone_index: u32) {
        let gap_start = milestone_index + 1;
        if gap_start != pre_ms {
            // create missing gap
            let gap = Range {
                start: gap_start,
                end: pre_ms,
            };
            self.sync_data.gaps.push(gap);
        }
    }
    fn proceed(ranges: &mut Vec<Range<u32>>, milestone_index: u32, check: bool) {
        let end_ms = milestone_index + 1;
        if let Some(Range { start, .. }) = ranges.last_mut() {
            if check && *start == end_ms {
                *start = milestone_index;
            } else {
                let range = Range {
                    start: milestone_index,
                    end: end_ms,
                };
                ranges.push(range)
            }
        } else {
            let range = Range {
                start: milestone_index,
                end: end_ms,
            };
            ranges.push(range);
        };
    }
}
