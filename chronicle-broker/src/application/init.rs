// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait]
impl<H: ChronicleBrokerScope> Init<H> for ChronicleBroker<H> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let Some(ref mut supervisor) = supervisor {
            let config = get_config_async().await;
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
            let mut syncer_builder = SyncerBuilder::new()
                .sync_data(self.sync_data.clone())
                .handle(syncer_handle.clone())
                .first_ask(AskSyncer::FillGaps)
                .oneshot(one)
                .inbox(syncer_inbox);
            let archiver_handle;
            if let Some(dir_path) = self.logs_dir_path.as_ref() {
                let max_log_size = config.broker_config.max_log_size.unwrap_or(MAX_LOG_SIZE);
                // create archiver_builder
                let mut archiver = ArchiverBuilder::new()
                    .dir_path(dir_path.clone())
                    .keyspace(self.default_keyspace.clone())
                    .solidifiers_count(self.collector_count)
                    .max_log_size(max_log_size)
                    .oneshot(recv)
                    .build();
                archiver_handle = archiver.take_handle();
                syncer_builder = syncer_builder
                    .first_ask(AskSyncer::Complete)
                    .archiver_handle(archiver_handle.clone().expect("Expected archiver handle"));
                // start archiver
                tokio::spawn(archiver.start(self.handle.clone()));
            } else {
                info!("Initializing Broker without Archiver");
                archiver_handle = None;
            }
            let mut collector_builders: Vec<CollectorBuilder> = Vec::new();
            let mut solidifier_builders: Vec<SolidifierBuilder> = Vec::new();
            let reqwest_client = reqwest::Client::builder()
                .timeout(Duration::from_secs(config.broker_config.request_timeout_secs))
                .build()
                .expect("Expected reqwest client to build correctly");
            for partition_id in 0..self.collector_count {
                // create requesters senders
                let mut requesters_senders = Vec::new();
                let mut requesters_channels = Vec::new();
                for _ in 0..config.broker_config.requester_count {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    requesters_senders.push(tx.clone());
                    requesters_channels.push((tx, rx));
                }
                // create collector_builder
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let collector_handle = CollectorHandle { tx, requesters_senders };
                let collector_inbox = CollectorInbox { rx };
                self.collector_handles.insert(partition_id, collector_handle.clone());
                let collector_builder = CollectorBuilder::new()
                    .collector_count(self.collector_count)
                    .requester_count(config.broker_config.requester_count)
                    .handle(collector_handle)
                    .inbox(collector_inbox)
                    .api_endpoints(config.broker_config.api_endpoints.iter().cloned().collect())
                    .storage_config(config.storage_config.clone())
                    .reqwest_client(reqwest_client.clone())
                    .retries_per_query(config.broker_config.retries_per_query)
                    .retries_per_endpoint(config.broker_config.retries_per_endpoint)
                    .requesters_channels(requesters_channels)
                    .partition_id(partition_id);

                collector_builders.push(collector_builder);
                // create solidifier_builder
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let solidifier_handle = SolidifierHandle { tx };
                let solidifier_inbox = SolidifierInbox { rx };
                self.solidifier_handles.insert(partition_id, solidifier_handle.clone());
                let mut solidifier_builder = SolidifierBuilder::new()
                    .collector_count(self.collector_count)
                    .syncer_handle(syncer_handle.clone());
                if let Some(archiver_handle) = archiver_handle.clone().take() {
                    solidifier_builder = solidifier_builder.archiver_handle(archiver_handle);
                }
                solidifier_builder = solidifier_builder
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
                .sync_range(self.sync_range)
                .parallelism(self.parallelism)
                .update_sync_data_every(self.complete_gaps_interval)
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

impl<H: ChronicleBrokerScope> ChronicleBroker<H> {
    pub(crate) async fn query_sync_table(&mut self) -> Result<(), Need> {
        self.sync_data = SyncData::try_fetch(&self.default_keyspace, &self.sync_range, 10)
            .await
            .map_err(|e| {
                error!("{}", e);
                Need::Abort
            })?;
        Ok(())
    }
}
