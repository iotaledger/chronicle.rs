// walk from milestone one to another to identify the gaps
use super::validator_worker;
use chronicle_common::actor;
use log::*;
use std::{
    fmt::Write,
    io::{prelude::*, Write as IoWrite},
    time::SystemTime,
};

use lru::LruCache;
use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fs::File,
    io::BufReader,
    num::ParseIntError,
    str::FromStr,
};
use tokio::sync::mpsc;

actor!(ValidatorBuilder {
    start_ms: u32,
    stop_ms: u32,
    max_retries: usize,
    lru_size: usize,
    force_correctness: bool,
    milestones_file: String
});

/// The `Validator` event.
pub enum Event {
    Tip(Option<[String; 2]>, usize, validator_worker::Sender),
    Gap(String, usize, validator_worker::Sender),
    Unreachable(String),
}
pub struct Milestone {
    hash: String,
    index: u32,
}

impl FromStr for Milestone {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut s = s.split(',');
        let hash: String = s.next().expect("invalid line").to_string();
        let index: u32 = s.next().expect("invalid line").parse().expect("invalid milestone index");
        Ok(Milestone { hash, index })
    }
}

pub struct Validator {
    gaps_file: File,
    milestones: Vec<(String, u32)>,
    visited: LruCache<String, ()>,
    current_ms: u32,
    txs_count_in_current_ms: usize,
    workers_count: usize,
    ready_workers: VecDeque<(usize, validator_worker::Sender)>,
    tip_txs: HashMap<usize, validator_worker::Sender>,
    gap: HashMap<String, u32>,
    max_retries: usize,
    force_correctness: bool,
    tx: Option<mpsc::UnboundedSender<Event>>,
    rx: mpsc::UnboundedReceiver<Event>,
}
impl ValidatorBuilder {
    /// Build a Validator.
    pub fn build(self) -> Validator {
        let start_ms = self.start_ms.unwrap();
        let stop_ms = self.stop_ms.unwrap();
        if start_ms > stop_ms || stop_ms > 1537346 {
            panic!("start_ms should be less than stop_ms and both <= 1537346")
        }
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let visited = LruCache::new(self.lru_size.unwrap_or(10_000_000));
        let milestones_file =
            File::open(self.milestones_file.expect("please provide milestones file path")).expect("unable to open milestones file");
        let buf_reader = BufReader::new(milestones_file);
        let mut lines = buf_reader.lines();
        let mut milestones = Vec::new();
        while let Some(Ok(line)) = lines.next() {
            let Milestone { hash, index } = line.parse().expect("invalid milestone line");
            if index >= start_ms && index <= stop_ms {
                milestones.push((hash, index))
            }
        }
        milestones.reverse();
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let mut gaps_file_name = String::new();
        write!(&mut gaps_file_name, "gaps_{}.txt", now);
        let gaps_file = File::create(gaps_file_name).unwrap();
        Validator {
            gaps_file,
            milestones,
            visited,
            current_ms: 0,
            txs_count_in_current_ms: 0,
            workers_count: 1,
            ready_workers: VecDeque::new(),
            tip_txs: HashMap::new(),
            gap: HashMap::new(),
            max_retries: self.max_retries.unwrap(),
            force_correctness: self.force_correctness.unwrap_or(false),
            tx: Some(tx),
            rx,
        }
    }
}

impl Validator {
    pub async fn run(mut self) -> Result<(), Box<dyn Error>> {
        // start traversing from oldest milestone to the newer ones
        let (milestone_hash, milestone_index) = self.milestones.pop().expect("no milestone");
        self.current_ms = milestone_index;
        // spawn worker
        let validator_worker = validator_worker::ValidatorWorkerBuilder::new()
            .force_correctness(self.force_correctness)
            .id(self.workers_count)
            .max_retries(self.max_retries)
            .build();
        let tip_tx = validator_worker.tx.as_ref().unwrap().clone();
        self.tip_txs.insert(self.workers_count, tip_tx.clone());
        let _ = self.tip_txs.get(&self.workers_count).unwrap().send(validator_worker::Event::Tip(
            milestone_hash.clone(),
            self.current_ms,
            tip_tx,
        ));
        // identify as visited
        self.visited.put(milestone_hash, ());
        tokio::spawn(validator_worker.run(self.tx.as_ref().unwrap().clone()));
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Tip(tips_opt, worker_id, worker_tx) => {
                    // return worker back to ready_workers
                    self.ready_workers.push_back((worker_id, worker_tx));
                    if let Some([trunk, branch]) = tips_opt {
                        // asking for further traversing;
                        // check if trunk visited
                        if let None = self.visited.get(&trunk) {
                            // increment the txs count for the current_ms
                            self.txs_count_in_current_ms += 1;
                            // takes worker and proceed,
                            let (worker_id, worker_tx) = self.ready_workers.pop_front().unwrap();
                            let _ = self.tip_txs.get(&worker_id).unwrap().send(validator_worker::Event::Tip(
                                trunk.clone(),
                                self.current_ms,
                                worker_tx,
                            ));
                            // mark it as visited
                            self.visited.put(trunk, ());
                        }
                        // check if branch visited
                        if let None = self.visited.get(&branch) {
                            // increment the txs count for the current_ms
                            self.txs_count_in_current_ms += 1;
                            // takes worker and proceed
                            if let Some((worker_id, worker_tx)) = self.ready_workers.pop_front() {
                                let _ = self.tip_txs.get(&worker_id).unwrap().send(validator_worker::Event::Tip(
                                    branch.clone(),
                                    self.current_ms,
                                    worker_tx,
                                ));
                            } else {
                                // spawn worker
                                self.workers_count += 1;
                                let validator_worker = validator_worker::ValidatorWorkerBuilder::new()
                                    .id(self.workers_count)
                                    .force_correctness(self.force_correctness)
                                    .max_retries(self.max_retries)
                                    .build();
                                // tell the worker to start solidifying the milestone
                                let tip_tx = validator_worker.tx.as_ref().unwrap().clone();
                                self.tip_txs.insert(self.workers_count, tip_tx.clone());
                                let _ = self.tip_txs.get(&self.workers_count).unwrap().send(validator_worker::Event::Tip(
                                    branch.clone(),
                                    self.current_ms,
                                    tip_tx,
                                ));
                                tokio::spawn(validator_worker.run(self.tx.clone().unwrap()));
                            }
                            // mark it as visited
                            self.visited.put(branch, ());
                        }
                    }
                    // check if needs to move to next milestone or is time to get shutdown;
                    self.maybe_next();
                }
                Event::Gap(hash, worker_id, worker_tx) => {
                    // return worker_tx to ready_workers
                    self.ready_workers.push_back((worker_id, worker_tx));
                    // gap identified, in memory map
                    // check if the gap already identified in previues milestones
                    if let None = self.gap.get(&hash) {
                        // commit the gap to a
                        let mut gap_line = String::new();
                        write!(&mut gap_line, "{},{}\n", self.current_ms, hash).unwrap();
                        self.gaps_file.write_all(gap_line.as_bytes()).unwrap();
                        // add it to the gap map
                        self.gap.insert(hash, self.current_ms);
                    } else {
                        warn!("dependent gap");
                    }
                    self.maybe_next();
                }
                Event::Unreachable(hash) => return Err(Box::new(ValidatorError::Unreachable(hash))),
            }
        }
        Ok(())
    }
    fn maybe_next(&mut self) {
        if self.ready_workers.len() == self.workers_count {
            // milestone is solid and correct, proceed to next ms
            if let Some((milestone_hash, milestone_index)) = self.milestones.pop() {
                if let Some((worker_id, worker_tx)) = self.ready_workers.pop_front() {
                    self.current_ms = milestone_index;
                    // identify as visited
                    self.visited.put(milestone_hash.clone(), ());
                    // push it by worker_tx
                    let _ = self.tip_txs.get(&worker_id).unwrap().send(validator_worker::Event::Tip(
                        milestone_hash,
                        milestone_index,
                        worker_tx,
                    ));
                    self.txs_count_in_current_ms = 1;
                }
            } else {
                // no further milestones to solidfy
                self.tx.take();
                // drop all workers_txs
                self.ready_workers.clear();
                // shutdown all of them
                for (_worker_id, worker_tx) in self.tip_txs.drain() {
                    let _ = worker_tx.send(validator_worker::Event::Shutdown);
                }
                let gaps_count = self.gap.len();
                info!("total gaps_count: {}", gaps_count);
            }
        }
    }
}

#[derive(Debug)]
pub enum ValidatorError {
    Unreachable(String),
}

impl std::fmt::Display for ValidatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidatorError::Unreachable(h) => write!(f, "Unreachable hash: {}", h),
        }
    }
}
impl Error for ValidatorError {}
