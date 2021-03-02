use super::*;

#[async_trait]
impl<H: PermanodeBrokerScope> Init<H> for PermanodeBroker<H> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let Some(ref mut supervisor) = supervisor {
            supervisor.status_change(self.service.clone());
            for partition_id in 0..self.collectors_count {
                // Build collector
                let mut collector = CollectorBuilder::new()
                    .collectors_count(self.collectors_count)
                    .partition_id(partition_id)
                    .build();
                let collector_handle = collector.take_handle().unwrap();
                self.collector_handles.insert(partition_id, collector_handle);
                tokio::spawn(collector.start(self.handle.clone()));
            }
            status
        } else {
            Err(Need::Abort)
        }
    }
}
