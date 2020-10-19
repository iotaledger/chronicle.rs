// TODO compute token to enable shard_awareness.
// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This module provides publiction functions for logger.

mod config;

pub use config::{LoggerConfig, LoggerConfigBuilder, LoggerOutputConfig, LoggerOutputConfigBuilder};

use fern::colors::{Color, ColoredLevelConfig};

#[derive(Debug)]
#[non_exhaustive]
/// The logger error enum.
pub enum Error {
    /// Log file error.
    File,
    /// Log apply error.
    Apply,
}

/// Initialize a logger.
pub fn logger_init(config: LoggerConfig) -> Result<(), Error> {
    let timestamp_format = "[%Y-%m-%d][%H:%M:%S]";

    let mut logger = if config.color {
        let colors = ColoredLevelConfig::new()
            .trace(Color::BrightMagenta)
            .debug(Color::BrightBlue)
            .info(Color::BrightGreen)
            .warn(Color::BrightYellow)
            .error(Color::BrightRed);

        fern::Dispatch::new().format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format(timestamp_format),
                record.target(),
                colors.color(record.level()),
                message
            ))
        })
    } else {
        fern::Dispatch::new().format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format(timestamp_format),
                record.target(),
                record.level(),
                message
            ))
        })
    };

    for output in config.outputs {
        let mut dispatcher = fern::Dispatch::new().level(output.level);

        dispatcher = if output.name == "stdout" {
            dispatcher.chain(std::io::stdout())
        } else {
            dispatcher.chain(fern::log_file(output.name).map_err(|_| Error::File)?)
        };

        logger = logger.chain(dispatcher);
    }

    logger.apply().map_err(|_| Error::Apply)?;

    Ok(())
}
