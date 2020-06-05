use super::{
    addresses,
    addresses::Rows as AddressesRows,
    approvees,
    approvees::Rows as ApproveesRows,
    bundles,
    bundles::Rows as BundlesRows,
    hints,
    hints::{
        Hint,
        Rows as HintsRows,
    },
};
use crate::api::types::Trytes81;
use chronicle_common::actor;
use chronicle_cql::{
    compression::MyCompression,
    frame::decoder::{
        Decoder,
        Frame,
    },
};
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker::{
        Error,
        Worker,
    },
};
use hyper::{
    Body,
    Response,
};
use serde::Serialize;
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
pub struct FindTransactionsId(Sender);

actor!(FindTransactionsBuilder {
    addresses: Option<Vec<Trytes81>>,
    bundles: Option<Vec<Trytes81>>,
    approvees: Option<Vec<Trytes81>>,
    hints: Option<Vec<Hint>>
});

impl FindTransactionsBuilder {
    pub fn build(self) -> FindTransactions {
        FindTransactions {
            addresses: self.addresses.unwrap(),
            bundles: self.bundles.unwrap(),
            approvees: self.approvees.unwrap(),
            hints: self.hints.unwrap(),
        }
    }
}

#[allow(unused)]
pub struct FindTransactions {
    addresses: Option<Vec<Trytes81>>,
    bundles: Option<Vec<Trytes81>>,
    approvees: Option<Vec<Trytes81>>,
    hints: Option<Vec<Hint>>,
}

#[derive(Serialize, Default)]
struct ResTransactions {
    hashes: Option<Vec<Trytes81>>,
    hints: Option<Vec<Hint>>,
}

impl FindTransactions {
    pub async fn run(mut self) -> Response<Body> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let worker = Box::new(FindTransactionsId(tx));
        let mut res_txs = ResTransactions::default();
        // process param by param, starting from bundles
        match self.process_bundles(&mut res_txs, worker, &mut rx).await {
            Ok(worker) => {
                // process approvees
                match self.process_approvees(&mut res_txs, worker, &mut rx).await {
                    Ok(worker) => {
                        // process addresses
                        match self.process_addresses(&mut res_txs, worker, &mut rx).await {
                            Ok(worker) => {
                                // process hints
                                match self.process_hints(&mut res_txs, worker, &mut rx).await {
                                    Ok(_) => {
                                        // complete result is ready
                                        response!(body: serde_json::to_string(&res_txs).unwrap())
                                    }
                                    Err(response) => return response,
                                }
                            }
                            Err(response) => return response,
                        }
                    }
                    Err(response) => return response,
                }
            }
            Err(response) => return response,
        }
    }

    async fn process_bundles(
        &mut self,
        res_txs: &mut ResTransactions,
        mut worker: Box<FindTransactionsId>,
        rx: &mut Receiver,
    ) -> Result<Box<FindTransactionsId>, Response<Body>> {
        // create empty hashes;
        let mut hashes: Vec<Trytes81> = Vec::new();
        if let Some(bundles) = self.bundles.take() {
            for bundle in bundles {
                // create request
                let payload = bundles::query(&bundle);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(0, request);
                loop {
                    match rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                hashes = bundles::Hashes::new(decoder, hashes).decode().finalize();
                                break;
                            } else {
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing a bundle"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error {
                            kind: _error,
                            pid: _pid,
                        } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing a bundle"}"#),
                            );
                        }
                    }
                }
            }
        }
        // update hashes
        res_txs.hashes.replace(hashes);
        Ok(worker)
    }

    async fn process_approvees(
        &mut self,
        res_txs: &mut ResTransactions,
        mut worker: Box<FindTransactionsId>,
        rx: &mut Receiver,
    ) -> Result<Box<FindTransactionsId>, Response<Body>> {
        if let Some(approvees) = self.approvees.take() {
            let mut hashes = res_txs.hashes.take().unwrap();
            for approvee in approvees {
                // create request
                let payload = approvees::query(&approvee);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(0, request);
                loop {
                    match rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                hashes = approvees::Hashes::new(decoder, hashes).decode().finalize();
                                break;
                            } else {
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing an approvee"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error { kind: _, pid: _ } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing an approvee"}"#),
                            );
                        }
                    }
                }
            }
            res_txs.hashes.replace(hashes);
        }
        Ok(worker)
    }

    async fn process_addresses(
        &mut self,
        res_txs: &mut ResTransactions,
        mut worker: Box<FindTransactionsId>,
        rx: &mut Receiver,
    ) -> Result<Box<FindTransactionsId>, Response<Body>> {
        // create empty hints
        let mut hints: Vec<Hint> = Vec::new();
        if let Some(addresses) = self.addresses.take() {
            let mut hashes = res_txs.hashes.take().unwrap();
            for address in addresses {
                // create request
                let payload = addresses::query(&address);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(0, request);
                loop {
                    match rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                let (updated_hashes, updated_hints) =
                                    addresses::Hashes::new(decoder, hashes, hints, false, address)
                                        .decode()
                                        .finalize();
                                hashes = updated_hashes;
                                hints = updated_hints;
                                break;
                            } else {
                                println!("{:?}", decoder.get_error());
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing an address"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error { kind: _, pid: _ } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing an address"}"#),
                            );
                        }
                    }
                }
            }
            res_txs.hashes.replace(hashes);
        }
        res_txs.hints.replace(hints);
        Ok(worker)
    }

    async fn process_hints(
        &mut self,
        res_txs: &mut ResTransactions,
        mut worker: Box<FindTransactionsId>,
        rx: &mut Receiver,
    ) -> Result<Box<FindTransactionsId>, Response<Body>> {
        if let Some(hints) = self.hints.take() {
            // take ownership of the already found (hashes, hints)
            let mut hashes = res_txs.hashes.take().unwrap();
            let mut hintz = res_txs.hints.take().unwrap();
            for hint in hints {
                // create request
                let payload = hints::query(&hint);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(0, request);
                loop {
                    match rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                let (updated_hashes, updated_hints) =
                                    hints::Hashes::new(decoder, hashes, hintz, hint).decode().finalize();
                                hashes = updated_hashes;
                                hintz = updated_hints;
                                break;
                            } else {
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing a hint"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error { kind: _, pid: _ } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing a hint"}"#),
                            );
                        }
                    }
                }
            }
            res_txs.hashes.replace(hashes);
            res_txs.hints.replace(hintz);
        }
        Ok(worker)
    }
}

pub enum Event {
    Response {
        giveload: Vec<u8>,
        pid: Box<FindTransactionsId>,
    },
    Error {
        kind: Error,
        pid: Box<FindTransactionsId>,
    },
}

// implementation!
impl Worker for FindTransactionsId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        // to enable reusable self(Sender), we will do unsafe trick
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Response { giveload, pid };
            // now we can use raw to send self through itself.
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: Error) {
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Error { kind, pid };
            // now we can use raw to send itself through itself.
            let _ = (*raw).0.send(event);
        }
    }
}
