use scylla::{
    access::{
        GetInsertRequest,
        GetInsertStatement,
        Insert,
        Request,
    },
    application::{
        error,
        info,
    },
    stage::{
        ReporterEvent,
        ReporterHandle,
    },
    Worker,
    WorkerError,
};
use scylla_cql::{
    Consistency,
    Prepare,
};

#[derive(Debug)]
pub struct InsertWorker<S: Insert<K, V>, K, V> {
    pub keyspace: S,
    pub key: K,
    pub value: V,
}

impl<S, K, V> Worker for InsertWorker<S, K, V>
where
    S: 'static + Insert<K, V> + std::fmt::Debug,
    K: 'static + Send + std::fmt::Debug + Clone,
    V: 'static + Send + std::fmt::Debug + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        info!("{:?}", giveload);
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
        error!("{:?}, is_none {}", error, reporter.is_none());
        if let WorkerError::Cql(mut cql_error) = error {
            if let (Some(_), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let statement = self.keyspace.insert_statement::<K, V>();
                info!("Attempting to prepare statement '{}' with 3 retries left", statement);
                let prepare = Prepare::new().statement(&statement).build();
                let prepare_worker = PrepareWorker {
                    retries: 3,
                    payload: prepare.0.clone(),
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
                let payload = req.payload().clone();
                let retry_request = ReporterEvent::Request { worker: self, payload };
                reporter.send(retry_request).ok();
            }
        }
    }
}

#[derive(Debug)]
pub struct PrepareWorker {
    pub retries: usize,
    pub payload: Vec<u8>,
}

impl Worker for PrepareWorker {
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) {
        info!("Successfully prepared statement!");
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
        error!("Failed to prepare statement: {}", error);
        if self.retries > 0 {
            if let Some(reporter) = reporter {
                info!("Attempting to prepare statement with {} retries left", self.retries - 1);
                let prepare_worker = PrepareWorker {
                    retries: self.retries - 1,
                    payload: self.payload.clone(),
                };
                let request = ReporterEvent::Request {
                    worker: Box::new(prepare_worker),
                    payload: self.payload.clone(),
                };
                reporter.send(request).ok();
            } else {
                error!("Impossible de réessayer de préparer l'instruction car le rapporteur est disparu");
            }
        }
    }
}
