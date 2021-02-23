#![deny(unsafe_code)]

#[macro_use]
extern crate log;

use chrono::DateTime;
use chrono::Local;
use env_logger::fmt::Formatter;
use env_logger::Builder;
use kafcat::configs::get_arg_matches;
use kafcat::configs::AppConfig;
use kafcat::configs::KafkaConfig;
use kafcat::configs::WorkingMode;
use kafcat::error::KafcatError;
use kafcat::kafka_interface::CustomConsumer;
use kafcat::kafka_interface::CustomMessage;
use kafcat::kafka_interface::CustomProducer;
use kafcat::kafka_interface::KafkaInterface;
use kafcat::rdkafka_impl::RdKafka;
use log::LevelFilter;
use log::Record;
use std::io::Write;
use std::thread;

async fn process_message<Msg: CustomMessage>(msg: &Msg) {
    println!("{:?}", msg.get_payload());
}

async fn run_async_copy_topic<Interface: KafkaInterface>(_interface: Interface, config: AppConfig) -> Result<(), KafcatError> {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let input_config = config.input_kafka.as_ref().expect("Must specify input kafka config");
    let consumer: Interface::Consumer = Interface::Consumer::from_config(input_config.clone());
    consumer.set_offset(input_config.topic.as_ref().unwrap(), input_config.partition, input_config.offset).await?;

    let producer: Interface::Producer = Interface::Producer::from_config(config.output_kafka.clone().expect("Must specify output kafka config"));
    consumer
        .for_each(|x| async {
            process_message(&x).await; // TODO
            producer.write_one(x).await?;
            Ok(())
        })
        .await
}

async fn run_async_consume_topic<Interface: KafkaInterface>(_interface: Interface, config: AppConfig) -> Result<(), KafcatError> {
    let input_config = config.input_kafka.as_ref().expect("Must specify input kafka config");
    let consumer: Interface::Consumer = Interface::Consumer::from_config(input_config.clone());
    consumer.set_offset(&input_config.topic.as_ref().unwrap(), input_config.partition, input_config.offset).await?;
    consumer
        .for_each(|x| async {
            process_message(&x).await;
            drop(x);
            Ok(())
        })
        .await
}

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
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
    builder.format(output_format).filter(None, LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}

#[tokio::main]
async fn main() -> Result<(), KafcatError> {
    let matches = get_arg_matches();
    setup_logger(true, matches.value_of("log-conf"));
    let config = AppConfig::from(matches);

    info!("Starting {:?}", config.mode);

    let interface = RdKafka {};
    match config.mode {
        WorkingMode::Consumer => run_async_consume_topic(interface, config).await?,
        WorkingMode::Producer => {},
        WorkingMode::Metadata => {},
        WorkingMode::Query => {},
        WorkingMode::Copy => run_async_copy_topic(interface, config).await?,
        _ => {},
    }
    Ok(())
}
