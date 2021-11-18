use scylla_rs::prelude::{
    send_global,
    ReporterEvent,
    RingSendError,
};
pub(crate) fn retry_send(
    keyspace: &str,
    mut r: RingSendError,
    mut retries: u8,
) -> Result<(), Box<dyn scylla_rs::prelude::Worker>> {
    loop {
        if let ReporterEvent::Request { worker, payload } = r.into() {
            if retries > 0 {
                if let Err(still_error) = send_global(keyspace, rand::random(), payload, worker) {
                    r = still_error;
                    retries -= 1;
                } else {
                    break;
                };
            } else {
                return Err(worker);
            }
        } else {
            unreachable!("the reporter variant must be request");
        }
    }
    Ok(())
}
