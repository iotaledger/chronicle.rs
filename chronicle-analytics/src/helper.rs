// Copyright 2022 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
//! This module defines helper functions for chronicle-analytics crate.

use crate::{
    error::AnalyticsError,
    MilestoneRangePath,
};

use std::path::PathBuf;

/// Get the milestone range and log paths from the input directory.
pub fn get_milestone_range_paths(dir: &str) -> Result<Vec<MilestoneRangePath>, AnalyticsError> {
    let split_filename = |v: Result<PathBuf, _>| match v {
        Ok(path) => {
            let file_name = path.file_stem().unwrap();
            let mut split = file_name.to_str().unwrap().split(".").next().unwrap().split("to");
            let (start, end) = (
                split.next().unwrap().parse::<u32>().unwrap(),
                split.next().map(|s| s.parse::<u32>().unwrap()).unwrap_or(u32::MAX),
            );
            println!("Start {}, End {}", start, end);
            Some((start, end, path))
        }
        Err(_) => None,
    };
    let paths = glob::glob(&format!("{}/*to*.log*", dir))
        .unwrap()
        .filter_map(split_filename)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        println!("No logs found in {}", dir);
        return Err(AnalyticsError::NoResults);
    } else {
        return Ok(paths);
    }
}
