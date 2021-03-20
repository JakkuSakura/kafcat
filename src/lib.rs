pub mod coder;
pub mod configs;
pub mod connection;
pub mod error;
pub mod input;
pub mod interface;
pub mod message;
pub mod output;
pub mod rdkafka_impl;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate log;

use crate::error::KafcatError;

type Result<T> = std::result::Result<T, KafcatError>;

use chrono::DateTime;
use chrono::Local;
use env_logger::fmt::Formatter;
use env_logger::Builder;
use env_logger::Target;
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
        writeln!(formatter, "{} {}{} - {} - {}", time_str, thread_name, record.level(), record.target(), record.args())
    };

    let mut builder = Builder::new();
    builder.format(output_format).filter(None, rust_log);
    builder.target(Target::Stderr);
    builder.init();
}
