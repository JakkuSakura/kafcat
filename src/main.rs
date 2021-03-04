#![deny(unsafe_code)]

#[macro_use]
extern crate log;
pub mod modes;
use modes::*;

use chrono::DateTime;
use chrono::Local;
use env_logger::fmt::Formatter;
use env_logger::Builder;
use env_logger::Target;
use kafcat::configs::AppConfig;
use kafcat::configs::WorkingMode;
use kafcat::error::KafcatError;
use kafcat::rdkafka_impl::RdKafka;
use log::LevelFilter;
use log::Record;
use std::io::Write;
use std::thread;

pub fn setup_logger(log_thread: bool, rust_log: LevelFilter) {
    let output_format = move |formatter: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        write!(formatter, "{} {}{} - {} - {}\n", time_str, thread_name, record.level(), record.target(), record.args())
    };

    let mut builder = Builder::new();
    builder.format(output_format).filter(None, rust_log);
    builder.target(Target::Stderr);
    builder.init();
}

#[tokio::main]
async fn main() -> Result<(), KafcatError> {
    let args = std::env::args().collect::<Vec<String>>();
    let config = AppConfig::from_args(args.iter().map(|x| x.as_str()).collect());
    setup_logger(true, config.log_level);

    info!("Starting {:?}", config.working_mode);

    let interface = RdKafka {};
    match config.working_mode {
        WorkingMode::Consumer => run_async_consume_topic(interface, config).await?,
        WorkingMode::Producer => run_async_produce_topic(interface, config).await?,
        WorkingMode::Metadata => {},
        WorkingMode::Query => {},
        WorkingMode::Copy => run_async_copy_topic(interface, config).await?,
        _ => {},
    }
    Ok(())
}
