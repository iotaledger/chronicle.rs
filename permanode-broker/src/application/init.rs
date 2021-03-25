use super::*;

#[async_trait]
impl<H: PermanodeBrokerScope> Init<H> for PermanodeBroker<H> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let Some(ref mut supervisor) = supervisor {
            supervisor.status_change(self.service.clone());
            // Query sync table

            // create logger_builder
            let mut logger = LoggerBuilder::new().dir_path(self.logs_dir_path.clone()).build();
            let logger_handle = logger.take_handle();
            // start logger
            tokio::spawn(logger.start(self.handle.clone()));
            let mut collector_builders: Vec<CollectorBuilder> = Vec::new();
            let mut solidifier_builders: Vec<SolidifierBuilder> = Vec::new();
            let reqwest_client = reqwest::Client::new();
            for partition_id in 0..self.collectors_count {
                // create collector_builder
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let collector_handle = CollectorHandle { tx };
                let collector_inbox = CollectorInbox { rx };
                self.collector_handles.insert(partition_id, collector_handle.clone());
                let mut collector_builder = CollectorBuilder::new()
                    .collectors_count(self.collectors_count)
                    .handle(collector_handle)
                    .inbox(collector_inbox)
                    .api_endpoints(self.broker_config.api_endpoints.clone())
                    .reqwest_client(reqwest_client.clone())
                    .partition_id(partition_id);
                if let Some(ref config) = self.storage_config {
                    collector_builder = collector_builder.storage_config(config.clone());
                }
                collector_builders.push(collector_builder);
                // create solidifier_builder
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let solidifier_handle = SolidifierHandle { tx };
                let solidifier_inbox = SolidifierInbox { rx };
                self.solidifier_handles.insert(partition_id, solidifier_handle);
                let solidifier_builder = SolidifierBuilder::new()
                    .collectors_count(self.collectors_count)
                    .logger_handle(logger_handle.clone().unwrap())
                    .inbox(solidifier_inbox)
                    .partition_id(partition_id);
                solidifier_builders.push(solidifier_builder);
            }
            // Spawn mqtt brokers
            for broker_url in self.broker_config.mqtt_brokers.clone() {
                if let Some(mqtt) = self.add_mqtt(Messages, broker_url.clone()) {
                    tokio::spawn(mqtt.start(self.handle.clone()));
                }
                if let Some(mqtt) = self.add_mqtt(MessagesReferenced, broker_url) {
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
