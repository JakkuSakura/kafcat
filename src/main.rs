#![deny(unsafe_code)]

#[macro_use]
extern crate log;

use chrono::DateTime;
use chrono::Local;
use env_logger::fmt::Formatter;
use env_logger::Builder;
use env_logger::Target;
use kafcat::configs::AppConfig;
use kafcat::configs::WorkingMode;
use kafcat::error::KafcatError;
use kafcat::interface::CustomConsumer;
use kafcat::interface::CustomMessage;
use kafcat::interface::CustomProducer;
use kafcat::interface::KafkaInterface;
use kafcat::rdkafka_impl::RdKafka;
use kafcat::scheduled_stream::ScheduledStream;
use log::LevelFilter;
use log::Record;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use tokio::time::Duration;

fn process_message<Msg: CustomMessage>(msg: &Msg) -> Result<(), KafcatError> {
    println!("{:?}", msg.get_payload());
    Ok(())
}

fn get_delay(exit: bool) -> Duration {
    if exit {
        Duration::from_millis(3000)
    } else {
        Duration::from_secs(3600)
    }
}

async fn run_async_copy_topic<Interface: KafkaInterface>(_interface: Interface, config: AppConfig) -> Result<(), KafcatError> {
    let input_config = config.input_kafka.as_ref().expect("Must specify input kafka config");
    let consumer: Interface::Consumer = Interface::Consumer::from_config(input_config.clone());
    consumer.set_offset(input_config.offset).await?;

    let producer: Interface::Producer = Interface::Producer::from_config(config.output_kafka.clone().expect("Must specify output kafka config"));
    let producer = Arc::new(producer);
    let delay = get_delay(input_config.exit_on_done);
    ScheduledStream::new(delay, consumer, move |msg| {
        process_message(&msg)?;
        let p = Arc::clone(&producer);
        tokio::task::spawn(async move { p.write_one(msg).await });
        Ok(())
    })
    .await?;

    Ok(())
}

async fn run_async_consume_topic<Interface: KafkaInterface>(_interface: Interface, config: AppConfig) -> Result<(), KafcatError> {
    let input_config = config.input_kafka.as_ref().expect("Must specify input kafka config");
    let consumer: Interface::Consumer = Interface::Consumer::from_config(input_config.clone());
    consumer.set_offset(input_config.offset).await?;
    let delay = get_delay(input_config.exit_on_done);
    ScheduledStream::new(delay, consumer, |msg| process_message(&msg)).await?;
    Ok(())
}

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
        WorkingMode::Producer => {},
        WorkingMode::Metadata => {},
        WorkingMode::Query => {},
        WorkingMode::Copy => run_async_copy_topic(interface, config).await?,
        _ => {},
    }
    Ok(())
}
