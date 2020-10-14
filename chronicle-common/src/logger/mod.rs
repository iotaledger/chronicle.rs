mod config;

pub use config::{LoggerConfig, LoggerConfigBuilder, LoggerOutputConfig, LoggerOutputConfigBuilder};

use fern::colors::{Color, ColoredLevelConfig};

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    File,
    Apply,
}

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
