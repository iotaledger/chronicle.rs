// Copyright 2022 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
//! This module defines the offline data analytics methods for archive data.

use crate::{
    error::AnalyticsError,
    helper::*,
    report::{
        ReportData,
        ReportRow,
    },
    MilestoneRangePath,
};
use anyhow::anyhow;

use tokio::{
    sync::mpsc::UnboundedReceiver,
    task::JoinHandle,
};

use chronicle_storage::access::{
    Address,
    Ed25519Address,
    Essence,
    LedgerInclusionState,
    MilestoneData,
    Output,
    Payload,
    SignatureUnlock,
    UnlockBlock,
};
use chrono::NaiveDate;
use crypto::hashes::{
    blake2b::Blake2b256,
    Digest,
};

use datafusion::{
    arrow::{
        array::{
            Float32Array,
            StringArray,
            UInt64Array,
        },
        datatypes::{
            DataType,
            Field,
            Schema,
        },
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    from_slice::FromSlice,
};
use indicatif::{
    MultiProgress,
    ProgressBar,
    ProgressStyle,
};
use rand::seq::SliceRandom;
use std::{
    collections::{
        btree_map,
        BTreeMap,
        BinaryHeap,
    },
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    fs::OpenOptions,
    io::{
        AsyncBufReadExt,
        BufReader,
    },
    sync::{
        mpsc::UnboundedSender,
        Mutex,
        MutexGuard,
    },
};

/// Builder for a Reporter.
pub struct ReporterBuilder {
    /// The directory contains the input logs.
    historical_log_directory: String,

    /// The number of running tasks.
    num_tasks: usize,
    /// The milestone range for analysis.
    range: std::ops::Range<u32>,
}

impl Default for ReporterBuilder {
    fn default() -> Self {
        Self {
            historical_log_directory: String::from(""),
            num_tasks: 4,
            range: 0..u32::MAX,
        }
    }
}

/// The reporter to generate data reports.
pub struct Reporter {
    /// The directory contains the input logs.
    historical_log_directory: String,

    /// The number of running tasks.
    num_tasks: usize,
    /// The milestone range for analysis.
    range: std::ops::Range<u32>,

    /// The milestone range and the corresponding log paths.
    paths: Vec<MilestoneRangePath>,
}

impl ReporterBuilder {
    /// Creates a new builder for a reporter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the historical_log_directory
    pub fn with_historical_log_directory(mut self, dir: &str) -> Self {
        self.historical_log_directory = String::from(dir);
        self
    }

    /// Set the number of tasks.
    pub fn with_num_tasks(mut self, num: usize) -> Self {
        self.num_tasks = num;
        self
    }

    /// The the range of milestones for processing.
    pub fn with_range(mut self, range: std::ops::Range<u32>) -> Self {
        self.range = range;
        self
    }

    /// Build a reporter.
    pub fn finish(self) -> Result<Reporter, AnalyticsError> {
        // Get the milestone range and the corresponding log paths
        let mut paths: Vec<MilestoneRangePath> = get_milestone_range_paths(&self.historical_log_directory)?;
        // Randomly order the paths so that we don't have all the tasks processing the same dates at the same time
        paths.shuffle(&mut rand::thread_rng());

        let reporter = Reporter {
            historical_log_directory: self.historical_log_directory,
            num_tasks: self.num_tasks,
            range: self.range,
            paths: paths,
        };
        Ok(reporter)
    }
}

/// Get the report from the milestone data.
/// Return the number of processed milestones and flag true if the milestone index already exceeds the target range.
pub fn update_report(
    mut report: MutexGuard<BTreeMap<chrono::NaiveDate, ReportData>>,
    data: chronicle_storage::access::MilestoneData,
    contained_range: std::ops::Range<u32>,
) -> (u64, bool) {
    let mut processed_milestone_count = 0;
    let mut stop_processing = false;
    if contained_range.contains(&data.milestone_index()) {
        let date = chrono::NaiveDateTime::from_timestamp(
            data.milestone()
                .ok_or_else(|| anyhow!("No milestone data for {}", data.milestone_index()))
                .unwrap()
                .essence()
                .timestamp() as i64,
            0,
        )
        .date();

        let report = report.entry(date).or_default();
        report.message_count += data.messages().len() as u64;
        for (metadata, payload) in data.messages().values().filter_map(|f| match f.0.payload() {
            Some(Payload::Transaction(t)) => Some((&f.1, &**t)),
            _ => None,
        }) {
            let Essence::Regular(regular_essence) = payload.essence();
            if metadata.ledger_inclusion_state == Some(LedgerInclusionState::Included) {
                report.included_transaction_count += 1;

                for output in regular_essence.outputs() {
                    match output {
                        // Accumulate the transferred token amount
                        Output::SignatureLockedSingle(output) => {
                            report.transferred_tokens += output.amount() as u128;
                            report.total_addresses.insert(output.address().clone());
                            report.recv_addresses.insert(output.address().clone());
                        }
                        Output::SignatureLockedDustAllowance(output) => {
                            report.transferred_tokens += output.amount() as u128
                        }
                        _ => (),
                    }
                }

                for unlock in payload.unlock_blocks().iter() {
                    if let UnlockBlock::Signature(SignatureUnlock::Ed25519(sig)) = unlock {
                        let address =
                            Address::Ed25519(Ed25519Address::new(Blake2b256::digest(sig.public_key()).into()));
                        report.total_addresses.insert(address);
                        report.send_addresses.insert(address);
                    }
                }
            } else if metadata.ledger_inclusion_state == Some(LedgerInclusionState::Conflicting) {
                report.conflicting_transaction_count += 1;
            }
            for output in regular_essence.outputs() {
                match output {
                    Output::SignatureLockedSingle(output) => {
                        *report.outputs.entry(output.address().clone()).or_default() += 1;
                    }
                    Output::SignatureLockedDustAllowance(output) => {
                        *report.outputs.entry(output.address().clone()).or_default() += 1;
                    }
                    _ => (),
                }
            }
            report.total_transaction_count += 1;
        }
        processed_milestone_count += 1;
    } else if data.milestone_index() > contained_range.end {
        stop_processing = true;
    }
    return (processed_milestone_count, stop_processing);
}

/// Get the tasks of generating reports.
pub fn get_tasks(
    pbs: Vec<indicatif::ProgressBar>,
    receivers: Vec<UnboundedReceiver<MilestoneRangePath>>,
    reports: &Vec<Arc<Mutex<BTreeMap<chrono::NaiveDate, ReportData>>>>,
    range: std::ops::Range<u32>,
) -> Vec<JoinHandle<()>> {
    let tasks = pbs
        .into_iter()
        .zip(receivers)
        .zip(reports.iter().cloned())
        .map(|((pb, mut receiver), report)| {
            tokio::spawn(async move {
                // let mut report = report.lock().await;
                let mut paths = Vec::new();
                while let Some((start, end, path)) = receiver.recv().await {
                    pb.inc_length((end - start) as u64);
                    paths.push((start, end, path));
                }
                for (start, end, path) in paths {
                    let contained_range = start.max(range.start)..end.min(range.end);
                    println!("contained_range = {:?}", contained_range);
                    if contained_range.is_empty() {
                        return;
                    }
                    let mut file = OpenOptions::new().read(true).open(path).await.unwrap();
                    let reader = BufReader::new(&mut file);
                    let mut lines = reader.lines();
                    while let Some(line) = lines.next_line().await.map_err(|e| anyhow!(e)).unwrap() {
                        let data = serde_json::from_str::<MilestoneData>(&line).unwrap();
                        let (pb_length, stop_processing) =
                            update_report(report.lock().await, data, contained_range.clone());
                        pb.inc(pb_length);
                        if stop_processing {
                            break;
                        }
                    }
                }
                pb.finish_with_message("done");
            })
        })
        .collect::<Vec<_>>();
    tasks
}

impl Reporter {
    /// Run the reporter and return the memory table
    pub async fn run(&mut self) -> anyhow::Result<MemTable> {
        // Need a progress bar for each task
        let sty = ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} (eta: {eta})")
            .progress_chars("##-");

        let mp = MultiProgress::new();
        let pbs = std::iter::repeat_with(|| {
            let pb = mp.add(ProgressBar::new(0));
            pb.set_style(sty.clone());
            pb
        })
        .take(self.num_tasks)
        .collect::<Vec<_>>();

        // Sending the MultiProgress to a blocking thread allows us to render
        let mp_join = tokio::task::spawn_blocking(move || mp.join().unwrap());

        // One report for each task, which will later be reduced to a single report
        let mut reports = std::iter::repeat_with(|| Arc::new(Mutex::new(BTreeMap::<NaiveDate, ReportData>::new())))
            .take(self.num_tasks)
            .collect::<Vec<_>>();

        // Create a channel for each task to send paths for processing
        let (mut senders, mut receivers) = (Vec::new(), Vec::new());
        for (sender, receiver) in
            std::iter::repeat_with(|| tokio::sync::mpsc::unbounded_channel::<(u32, u32, PathBuf)>())
                .take(self.num_tasks)
        {
            senders.push(sender);
            receivers.push(receiver);
        }
        let tasks = get_tasks(pbs, receivers, &reports, self.range.clone());

        // Make a min-heap so we can balance the work across the tasks
        let mut senders = senders.into_iter().map(|s| PrioritySender { sender: s, val: 0 }).fold(
            BinaryHeap::new(),
            |mut heap, sender| {
                heap.push(sender);
                heap
            },
        );
        // Send the work to the task with the least to do
        for path in self.paths.clone() {
            let mut p = senders.pop().unwrap();
            // Work is based on number of milestones
            p.val += (path.1 - path.0) as usize;
            p.sender.send(path)?;
            senders.push(p);
        }
        drop(senders);
        mp_join.await?;
        for task in tasks {
            task.await?;
        }
        let mut final_report = Arc::try_unwrap(reports.pop().unwrap()).unwrap().into_inner();
        for report in reports {
            let report = Arc::try_unwrap(report).unwrap().into_inner();
            for (date, data) in report {
                match final_report.entry(date) {
                    btree_map::Entry::Vacant(v) => {
                        v.insert(data);
                    }
                    btree_map::Entry::Occupied(o) => {
                        o.into_mut().merge(data);
                    }
                }
            }
        }
        let report_path = PathBuf::from(self.historical_log_directory.clone())
            .join(format!("report_{}to{}.csv", self.range.start, self.range.end));
        let mut writer = csv::Writer::from_path(&report_path)?;
        let pb = ProgressBar::new(0);
        pb.set_style(sty.clone());
        pb.enable_steady_tick(200);
        pb.set_length(final_report.len() as u64);

        let mut date_column: Vec<String> = Vec::new();
        let mut total_addresses_column: Vec<u64> = Vec::new();
        let mut recv_addresses_column: Vec<u64> = Vec::new();
        let mut send_addresses_column: Vec<u64> = Vec::new();
        let mut avg_outputs_column: Vec<f32> = Vec::new();
        let mut max_outputs_column: Vec<u64> = Vec::new();
        let mut message_count_column: Vec<u64> = Vec::new();
        let mut included_transaction_count_column: Vec<u64> = Vec::new();
        let mut conflicting_transaction_count_column: Vec<u64> = Vec::new();
        let mut total_transaction_count_column: Vec<u64> = Vec::new();
        let mut transferred_tokens_column: Vec<u64> = Vec::new();

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Utf8, false),
            Field::new("total_addresses", DataType::UInt64, false),
            Field::new("recv_addresses", DataType::UInt64, false),
            Field::new("send_addresses", DataType::UInt64, false),
            Field::new("avg_outputs", DataType::Float32, false),
            Field::new("max_outputs", DataType::UInt64, false),
            Field::new("message_count", DataType::UInt64, false),
            Field::new("included_transaction_count", DataType::UInt64, false),
            Field::new("conflicting_transaction_count", DataType::UInt64, false),
            Field::new("total_transaction_count", DataType::UInt64, false),
            Field::new("transferred_tokens", DataType::UInt64, false),
        ]));

        for (date, data) in final_report {
            pb.set_message(format!("Writing data for date {}", date));
            let row = ReportRow::from((date, data));
            writer.serialize(row.clone())?;

            // New added
            date_column.push(date.format("%Y-%m-%d").to_string());
            total_addresses_column.push(row.total_addresses.try_into().unwrap());
            recv_addresses_column.push(row.recv_addresses.try_into().unwrap());
            send_addresses_column.push(row.send_addresses.try_into().unwrap());
            avg_outputs_column.push(row.avg_outputs);
            max_outputs_column.push(row.max_outputs.try_into().unwrap());
            message_count_column.push(row.message_count);
            included_transaction_count_column.push(row.included_transaction_count);
            conflicting_transaction_count_column.push(row.conflicting_transaction_count);
            total_transaction_count_column.push(row.total_transaction_count);
            transferred_tokens_column.push(row.transferred_tokens.try_into().unwrap());

            pb.inc(1);
        }
        pb.finish_with_message(format!("Saved report at {}", report_path.to_string_lossy()));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_slice(&date_column)),
                Arc::new(UInt64Array::from_slice(&total_addresses_column)),
                Arc::new(UInt64Array::from_slice(&recv_addresses_column)),
                Arc::new(UInt64Array::from_slice(&send_addresses_column)),
                Arc::new(Float32Array::from_slice(&avg_outputs_column)),
                Arc::new(UInt64Array::from_slice(&max_outputs_column)),
                Arc::new(UInt64Array::from_slice(&message_count_column)),
                Arc::new(UInt64Array::from_slice(&included_transaction_count_column)),
                Arc::new(UInt64Array::from_slice(&conflicting_transaction_count_column)),
                Arc::new(UInt64Array::from_slice(&total_transaction_count_column)),
                Arc::new(UInt64Array::from_slice(&transferred_tokens_column)),
            ],
        )?;
        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;

        Ok(mem_table)
    }
}

#[derive(Debug)]
struct PrioritySender<T> {
    pub sender: UnboundedSender<T>,
    pub val: usize,
}

impl<T> Ord for PrioritySender<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.val.cmp(&self.val)
    }
}
impl<T> PartialOrd for PrioritySender<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.val.partial_cmp(&self.val)
    }
}

impl<T> Eq for PrioritySender<T> {}
impl<T> PartialEq for PrioritySender<T> {
    fn eq(&self, other: &Self) -> bool {
        self.val == other.val
    }
}
