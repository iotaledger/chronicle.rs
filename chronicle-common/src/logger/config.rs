use log::LevelFilter;
use serde::Deserialize;

const DEFAULT_COLOR: bool = true;
const DEFAULT_NAME: &str = "stdout";
const DEFAULT_LEVEL: &str = "info";

#[derive(Default, Deserialize, Debug, Clone)]
pub struct LoggerOutputConfigBuilder {
    name: Option<String>,
    level: Option<String>,
}

impl LoggerOutputConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name.replace(name.to_string());
        self
    }

    pub fn level(mut self, level: &str) -> Self {
        self.level.replace(level.to_string());
        self
    }

    pub fn finish(self) -> LoggerOutputConfig {
        let level = match self.level.unwrap_or_else(|| DEFAULT_LEVEL.to_owned()).as_str() {
            "trace" => LevelFilter::Trace,
            "debug" => LevelFilter::Debug,
            "info" => LevelFilter::Info,
            "warn" => LevelFilter::Warn,
            "error" => LevelFilter::Error,
            _ => LevelFilter::Info,
        };

        LoggerOutputConfig {
            name: self.name.unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            level,
        }
    }
}

#[derive(Default, Deserialize, Debug, Clone)]
pub struct LoggerConfigBuilder {
    color: Option<bool>,
    outputs: Option<Vec<LoggerOutputConfigBuilder>>,
}

impl LoggerConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn color(mut self, color: bool) -> Self {
        self.color.replace(color);
        self
    }

    pub fn stdout_level(&mut self, level: String) {
        if let Some(outputs) = self.outputs.as_deref_mut() {
            if let Some(stdout) = outputs
                .iter_mut()
                .find(|output| "stdout" == output.name.as_ref().unwrap())
            {
                stdout.level.replace(level);
            }
        }
    }

    pub fn finish(self) -> LoggerConfig {
        let mut outputs = Vec::new();

        if let Some(content) = self.outputs {
            for output in content {
                outputs.push(output.finish());
            }
        }

        LoggerConfig {
            color: self.color.unwrap_or(DEFAULT_COLOR),
            outputs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoggerOutputConfig {
    pub(crate) name: String,
    pub(crate) level: LevelFilter,
}

#[derive(Debug, Clone)]
pub struct LoggerConfig {
    pub(crate) color: bool,
    pub(crate) outputs: Vec<LoggerOutputConfig>,
}

impl LoggerConfig {
    pub fn build() -> LoggerConfigBuilder {
        LoggerConfigBuilder::new()
    }
}
