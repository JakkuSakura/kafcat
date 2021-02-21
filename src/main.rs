#![allow(non_upper_case_globals)]
#![deny(unsafe_code)]

use std::thread;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use log::info;
use log::LevelFilter;
use log::Record;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::Message;
use std::io::Write;

use chrono::DateTime;
use chrono::Local;
use env_logger::fmt::Formatter;
use env_logger::Builder;

use kafcat::configs::get_arg_matches;
use kafcat::configs::Config;
use kafcat::configs::KafkaConfig;
use kafcat::configs::WorkingMode;
use lazy_static::lazy_static;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use std::sync::Arc;

async fn record_owned_message_receipt(msg: &OwnedMessage) {
    println!("{:?}", msg);
}

async fn run_async_copy_topic(consumer: KafkaConfig, producer: KafkaConfig, input_topic: &str, output_topic: &str) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = consumer.into();

    consumer.subscribe(&[&input_topic]).expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = producer.into();

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
        async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                let mut record = FutureRecord::to(&output_topic);
                if let Some(key) = owned_message.key() {
                    record = record.key(key);
                }
                if let Some(payload) = owned_message.payload() {
                    record = record.payload(payload)
                }

                let produce_future = producer.send(record, Duration::from_secs(0));
                match produce_future.await {
                    Ok(delivery) => println!("Sent: {:?}", delivery),
                    Err((e, _)) => println!("Error: {:?}", e),
                }
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

async fn run_async_consume_topic(consumer: KafkaConfig, topic: &str) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = consumer.into();

    consumer.subscribe(&[&topic]).expect("Can't subscribe to specified topic");

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| async move {
        let owned_message = borrowed_message.detach();
        record_owned_message_receipt(&owned_message).await;

        Ok(())
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
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

lazy_static! {
    static ref config: Arc<Config> = {
        let matches = get_arg_matches();
        setup_logger(true, matches.value_of("log-conf"));
        Arc::new(Config::from(matches))
    };
}

#[tokio::main]
async fn main() {
    match config.mode {
        WorkingMode::Consumer => {
            (0..config.num_workers)
                .map(|_| {
                    tokio::spawn(run_async_consume_topic(
                        config.as_ref().into(),
                        config.topic.as_ref().or(config.input_topic.as_ref()).expect("Must use topic"),
                    ))
                })
                .collect::<FuturesUnordered<_>>()
                .for_each(|_| async { () })
                .await
        },
        WorkingMode::Producer => {},
        WorkingMode::Metadata => {},
        WorkingMode::Query => {},
        WorkingMode::Copy => {
            (0..config.num_workers)
                .map(|_| {
                    tokio::spawn(run_async_copy_topic(
                        config.as_ref().into(),
                        config.as_ref().into(),
                        config.input_topic.as_ref().expect("Must use input_topic"),
                        config.input_topic.as_ref().expect("Must use output_topic"),
                    ))
                })
                .collect::<FuturesUnordered<_>>()
                .for_each(|_| async { () })
                .await
        },
    }
}
