// Copyright 2022 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use arrow::record_batch::RecordBatch;
use chronicle_analytics::offline::ReporterBuilder;
use datafusion::prelude::ExecutionContext;
use std::sync::Arc;

#[tokio::test]
pub async fn run_offline_reporter() {
    let log_dir = "./tests/fixtures";
    let mut reporter = ReporterBuilder::new()
        .with_historical_log_directory(log_dir)
        .with_num_tasks(4)
        .with_range(111106..111115)
        .finish()
        .unwrap();

    let mem_table = reporter.run().await.unwrap();

    let mut ctx = ExecutionContext::new();
    ctx.register_table("report_table", Arc::new(mem_table)).unwrap();

    // create a plan
    let df = ctx.sql("SELECT * FROM report_table").await.unwrap();

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await.unwrap();

    // format the results
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)
        .unwrap()
        .to_string();

    let expected = vec![
        "+------------+-----------------+----------------+----------------+-------------+-------------+---------------+----------------------------+-------------------------------+-------------------------+--------------------+", 
        "| date       | total_addresses | recv_addresses | send_addresses | avg_outputs | max_outputs | message_count | included_transaction_count | conflicting_transaction_count | total_transaction_count | transferred_tokens |", 
        "+------------+-----------------+----------------+----------------+-------------+-------------+---------------+----------------------------+-------------------------------+-------------------------+--------------------+", 
        "| 2021-05-11 | 0               | 0              | 0              | NaN         | 0           | 6030          | 0                          | 0                             | 0                       | 0                  |", 
        "+------------+-----------------+----------------+----------------+-------------+-------------+---------------+----------------------------+-------------------------------+-------------------------+--------------------+"
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
}
