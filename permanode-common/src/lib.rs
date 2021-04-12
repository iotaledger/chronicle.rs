#![warn(missing_docs)]
//! Common code for Chronicle

use config::Config;
use glob::glob;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::{
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
    path::Path,
};
use tokio::sync::RwLock;

/// Configuration for the Chronicle application
pub mod config;
pub mod metrics;
use lazy_static::lazy_static;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct SyncRange {
    pub from: u32,
    pub to: u32,
}
impl Default for SyncRange {
    fn default() -> Self {
        Self {
            from: 1,
            to: i32::MAX as u32,
        }
    }
}
#[derive(Clone, Copy)]
pub struct Synckey;

pub trait Wrapper: Deref {
    fn into_inner(self) -> Self::Target;
}

/// A historical record
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct HistoricalConfig {
    config: Config,
    /// The timestamp representing when this record was created
    pub created: u64,
}

impl HistoricalConfig {
    /// Create a new historical record with the current timestamp
    pub fn new(config: Config) -> Self {
        Self {
            config,
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

impl From<Config> for HistoricalConfig {
    fn from(record: Config) -> Self {
        Self::new(record)
    }
}

impl From<(Config, u64)> for HistoricalConfig {
    fn from((config, created): (Config, u64)) -> Self {
        Self { config, created }
    }
}

impl Deref for HistoricalConfig {
    type Target = Config;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl DerefMut for HistoricalConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl Wrapper for HistoricalConfig {
    fn into_inner(self) -> Self::Target {
        self.config
    }
}

impl Persist for HistoricalConfig {
    fn persist(&self) {
        let mut path = std::env::var("HISTORICAL_CONFIG_PATH").unwrap_or(config::HISTORICAL_CONFIG_PATH.to_owned());
        path = Path::new(&path)
            .join(format!("{}_config.ron", self.created))
            .to_str()
            .unwrap()
            .to_owned();
        if let Err(e) = self.save(path) {
            error!("{}", e);
        }
    }
}

impl PartialOrd for HistoricalConfig {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HistoricalConfig {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.created.cmp(&other.created)
    }
}

/// A historical record which maintains `max_records`
pub struct History<R> {
    records: BinaryHeap<R>,
    max_records: usize,
}

impl<R> History<R>
where
    R: DerefMut + Default + Ord + Persist + Wrapper,
{
    /// Create a new history with `max_records`
    pub fn new(max_records: usize) -> Self {
        Self {
            records: BinaryHeap::new(),
            max_records,
        }
    }

    /// Get the most recent record with its created timestamp
    pub fn last(&self) -> R
    where
        R: Clone,
    {
        self.records.peek().cloned().unwrap_or_default()
    }

    /// Get an immutable reference to the latest record without a timestamp
    pub fn latest(&self) -> R::Target
    where
        R::Target: Clone + Default,
    {
        self.records.peek().map(Deref::deref).cloned().unwrap_or_default()
    }

    /// Update the history with a new record
    pub fn update(&mut self, record: R::Target)
    where
        R: From<<R as Deref>::Target>,
        R::Target: Sized,
    {
        self.records.push(record.into());
        self.truncate();
    }

    /// Add to the history with a new record and a timestamp and return a reference to it.
    /// *This should only be used to deserialize a `History`.*
    pub fn add(&mut self, record: R::Target, created: u64)
    where
        R: From<(<R as Deref>::Target, u64)>,
        R::Target: Sized,
    {
        self.records.push((record, created).into());
        self.truncate();
    }

    /// Rollback to the previous version and return the removed record
    pub fn rollback(&mut self) -> Option<R::Target>
    where
        R::Target: Sized,
    {
        self.records.pop().map(|r| r.into_inner())
    }

    fn truncate(&mut self) {
        self.records = self.records.drain().take(self.max_records).collect();
    }

    /// Get an interator over the time-ordered history
    pub fn iter(&self) -> std::collections::binary_heap::Iter<R> {
        self.records.iter()
    }
}

impl History<HistoricalConfig> {
    /// Load the historical config from the file system
    pub fn load<M: Into<Option<usize>>>(max_records: M) -> Self {
        let mut history = max_records
            .into()
            .map(|max_records| Self::new(max_records))
            .unwrap_or_default();
        match Config::load(None) {
            Ok(latest) => {
                debug!("Latest Config found! {:?}", latest);
                let historical_config_path = latest.historical_config_path.clone();
                history.update(latest);
                glob(&format!(r"{}/\d+_config.ron", historical_config_path))
                    .into_iter()
                    .flat_map(|v| v.into_iter())
                    .filter_map(|path| {
                        debug!("historical path: {:?}", path);
                        path.map(|p| {
                            Config::load(p.to_str().map(|s| s.to_owned())).ok().and_then(|c| {
                                p.file_name()
                                    .and_then(|s| s.to_string_lossy().split("_").next().map(|s| s.to_owned()))
                                    .and_then(|s| s.parse::<u64>().ok())
                                    .map(|created| (c, created))
                            })
                        })
                        .ok()
                        .flatten()
                    })
                    .for_each(|(config, created)| {
                        history.add(config, created);
                    });
            }
            Err(e) => {
                panic!("{}", e)
            }
        }
        history
    }
}

impl<R> Default for History<R>
where
    R: DerefMut + Ord + Persist,
    R::Target: Persist,
{
    fn default() -> Self {
        Self {
            records: Default::default(),
            max_records: 20,
        }
    }
}

impl Persist for History<HistoricalConfig> {
    fn persist(&self) {
        debug!("Persisting history!");
        let mut iter = self.records.clone().into_sorted_vec().into_iter().rev();
        debug!(
            "Sorted records: {:?}",
            iter.clone().map(|r| r.created).collect::<Vec<_>>()
        );
        if let Some(latest) = iter.next() {
            debug!("Persisting latest config!");
            latest.deref().persist();
            for v in iter {
                debug!("Persisting historical config!");
                v.persist();
            }
        }
    }
}

/// Specifies that the implementor should be able to persist itself
pub trait Persist {
    /// Persist this value
    fn persist(&self);
}

/// A handle which will persist when dropped
pub struct PersistHandle {
    guard: tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig>>,
}

impl From<tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig>>> for PersistHandle {
    fn from(guard: tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig>>) -> Self {
        Self { guard }
    }
}

impl Deref for PersistHandle {
    type Target = tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for PersistHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl std::ops::Drop for PersistHandle {
    fn drop(&mut self) {
        self.persist()
    }
}

/// Get the latest config asynchronously
pub async fn get_config_async() -> Config {
    CONFIG.read().await.latest()
}
/// Get the latest config synchronously
pub fn get_config() -> Config {
    futures::executor::block_on(get_config_async())
}

/// Get a mutable reference to the config history asynchronously
pub async fn get_history_mut_async() -> PersistHandle {
    CONFIG.write().await.into()
}

/// Get a mutable reference to the config history synchronously
pub fn get_history_mut() -> PersistHandle {
    futures::executor::block_on(get_history_mut_async())
}

lazy_static! {
    /// Global config
    pub static ref CONFIG: RwLock<History<HistoricalConfig>> = RwLock::new(History::load(20));
}
