use super::*;

pub struct PrepareWorker {
    pub id: [u8; 16],
    pub statement: String,
}

impl Worker for PrepareWorker {
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) {
        info!(
            "Successfully prepared statement: '{}', id: '{:?}'",
            self.statement, self.id
        );
    }
    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
        error!("Failed to prepare statement: {}, error: {}", self.statement, error);
    }
}
