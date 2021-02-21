#![deny(unsafe_code)]

use std::thread;
use std::time::Duration;

use chrono::DateTime;
use chrono::Local;
use env_logger::fmt::Formatter;
use env_logger::Builder;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use kafcat::configs::get_arg_matches;
use kafcat::configs::AppConfig;
use kafcat::configs::KafkaConfig;
use kafcat::configs::KafkaOffset;
use kafcat::configs::WorkingMode;
use kafcat::error::KafcatError;
use kafcat::kafka_interface::CustomConsumer;
use kafcat::kafka_interface::CustomMessage;
use kafcat::kafka_interface::CustomProducer;
use kafcat::kafka_interface::KafkaInterface;
use kafcat::rdkafka_impl::RdKafka;
use kafcat::timeout_stream::TimeoutStreamExt;
use log::info;
use log::LevelFilter;
use log::Record;
use std::io::Write;
use std::sync::Arc;
use tokio::task::spawn_blocking;

async fn process_message<Msg: CustomMessage>(msg: &Msg) {
    println!("{:?}", msg.get_payload());
}

async fn run_async_copy_topic<Interface: KafkaInterface>(
    interface: Interface,
    consumer_config: KafkaConfig,
    producer_config: KafkaConfig,
    input_topic: &str,
    output_topic: &str,
) -> Result<(), KafcatError> {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let mut consumer: Interface::Consumer = Interface::Consumer::from_config(interface.get_config(), consumer_config.clone(), input_topic, interface.get_config().partition);
    consumer.set_offset(&input_topic, consumer_config.partition, consumer_config.offset).await?;

    let mut producer: Interface::Producer = Interface::Producer::from_config(interface.get_config(), consumer_config.clone(), output_topic);
    consumer
        .for_each(|x| async {
            process_message(&x).await; // TODO
            producer.write_one(x).await?;
            Ok(())
        })
        .await
}

async fn run_async_consume_topic<Interface: KafkaInterface>(interface: Interface, config: &AppConfig, consumer_config: KafkaConfig, topic: &str) -> Result<(), KafcatError> {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let mut consumer: Interface::Consumer = Interface::Consumer::from_config(interface.get_config(), consumer_config.clone(), topic, interface.get_config().partition);
    consumer.set_offset(&topic, consumer_config.partition, consumer_config.offset).await?;
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
    let config = Box::leak(Box::new(AppConfig::from(matches))) as &AppConfig;

    info!("Starting {:?}", config.mode);

    let interface = RdKafka::from_config(config.clone());
    match config.mode {
        WorkingMode::Consumer => run_async_consume_topic(interface, config, config.into(), config.topic.as_ref().or(config.input_topic.as_ref()).expect("Must use topic")).await?,
        WorkingMode::Producer => {},
        WorkingMode::Metadata => {},
        WorkingMode::Query => {},
        WorkingMode::Copy => {
            run_async_copy_topic(
                interface,
                config.into(),
                config.into(),
                config.input_topic.as_ref().expect("Must use input_topic"),
                config.input_topic.as_ref().expect("Must use output_topic"),
            )
            .await?
        },
        _ => {},
    }
    Ok(())
}
