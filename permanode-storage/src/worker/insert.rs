use super::*;

pub struct InsertWorker<S: Insert<K, V>, K, V> {
    pub handle: tokio::sync::mpsc::UnboundedSender<Event>,
    pub keyspace: S,
    pub key: K,
    pub value: V,
}

impl<S, K, V> Worker for InsertWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        info!("{:?}", giveload);
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
        error!("{:?}, reporter running: {}", error, reporter.is_some());
        if let WorkerError::Cql(mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let statement = self.keyspace.insert_statement::<K, V>();
                info!("Attempting to prepare statement '{}', id: '{:?}'", statement, id);
                let prepare = Prepare::new().statement(&statement).build();
                let prepare_worker = PrepareWorker {
                    id,
                    statement: statement.to_string(),
                };
                let prepare_request = ReporterEvent::Request {
                    worker: Box::new(prepare_worker),
                    payload: prepare.0,
                };
                reporter.send(prepare_request).ok();
                let req = self
                    .keyspace
                    .insert_query(&self.key, &self.value)
                    .consistency(Consistency::One)
                    .build();
                let payload = req.into_payload();
                let retry_request = ReporterEvent::Request { worker: self, payload };
                reporter.send(retry_request).ok();
                return ();
            }
            // TODO handle remaining cql errors
        }
        // TODO handling remaining worker errors
    }
}
