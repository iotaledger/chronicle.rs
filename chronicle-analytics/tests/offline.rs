// Copyright 2022 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use chronicle_analytics::offline::ReporterBuilder;

#[tokio::test]
pub async fn run_offline_reporter() {
    let log_dir = "./tests/fixtures";
    let mut reporter = ReporterBuilder::new()
        .with_historical_log_directory(log_dir)
        .with_num_tasks(4)
        .with_range(111106..111115)
        .finish()
        .unwrap();
    reporter.run().await;

    // TODO: Check the generated results
}
