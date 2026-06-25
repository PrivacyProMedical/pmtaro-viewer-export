use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

static LOG_INITIALIZED: OnceLock<()> = OnceLock::new();

/// Initialize the log4rs logging system.
///
/// Log files are saved in the `logs` folder under the directory where the `.node` file is located,
/// with rolling at 2MB increments, keeping up to 100 historical log files.
pub fn init_logging(module_dir: &PathBuf) {
    LOG_INITIALIZED.get_or_init(|| {
        do_init_logging(module_dir);
    });
}

fn do_init_logging(module_dir: &PathBuf) {
    // Create the logs directory
    let logs_dir = module_dir.join("logs");
    if let Err(e) = fs::create_dir_all(&logs_dir) {
        eprintln!("[init-logger] Failed to create logs directory: {}, error: {}", logs_dir.display(), e);
        // If the logs directory cannot be created, fall back to the current working directory
        if let Ok(cwd) = std::env::current_dir() {
            let fallback_logs = cwd.join("logs");
            let _ = fs::create_dir_all(&fallback_logs);
            do_init_logging_inner(&fallback_logs);
        }
        return;
    }

    do_init_logging_inner(&logs_dir);
}

fn do_init_logging_inner(logs_dir: &PathBuf) {
    // Base path for log files
    let log_file = logs_dir.join("pmtaro-viewer-export.log");

    // Rolling file naming pattern: pmtaro-viewer-export.log.0, pmtaro-viewer-export.log.1, ..., pmtaro-viewer-export.log.99
    let roller_pattern = logs_dir
        .join("pmtaro-viewer-export.log.{}")
        .to_string_lossy()
        .to_string();
    let roller = match FixedWindowRoller::builder()
        .base(0)
        .build(&roller_pattern, 100)
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[init-logger] Failed to create FixedWindowRoller: {}", e);
            // Fallback: skip log4rs
            return;
        }
    };

    // Trigger rolling at 2MB
    let trigger = SizeTrigger::new(2 * 1024 * 1024);

    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    let appender = match RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S%.3f)} [{l}] {t} - {m}{n}",
        )))
        .build(log_file, Box::new(policy))
    {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[init-logger] Failed to create RollingFileAppender: {}", e);
            return;
        }
    };

    let config = match Config::builder()
        .appender(Appender::builder().build("file", Box::new(appender)))
        .logger(Logger::builder().build("pmtaro_export_plugin", log::LevelFilter::Info))
        .build(Root::builder().appender("file").build(log::LevelFilter::Info))
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[init-logger] Failed to create log4rs config: {}", e);
            return;
        }
    };

    if let Err(e) = log4rs::init_config(config) {
        eprintln!(
            "[init-logger] log4rs initialization failed (may already be initialized): {}",
            e
        );
    }
}
