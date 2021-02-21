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
use kafcat::configs::Config;
use kafcat::configs::KafkaConfig;
use kafcat::configs::KafkaOffset;
use kafcat::configs::WorkingMode;
use kafcat::timeout_stream::TimeoutStreamExt;
use log::info;
use log::LevelFilter;
use log::Record;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use std::io::Write;
use tokio::task::spawn_blocking;

fn process_message(msg: &OwnedMessage) {
    println!("{:?}", msg);
}

async fn get_topic_partition_list(consumer: StreamConsumer, topic: String, partition: Option<i32>, offset: KafkaOffset) -> KafkaResult<(StreamConsumer, TopicPartitionList)> {
    info!("offset {:?}", offset);
    let mut tpl = TopicPartitionList::new();
    let partition = partition.unwrap_or(0);
    let r: KafkaResult<_> = {
        let topic = topic.clone();
        spawn_blocking(move || {
            let r = match offset {
                KafkaOffset::Beginning => Offset::Beginning,
                KafkaOffset::End => Offset::End,
                KafkaOffset::Stored => Offset::Stored,
                KafkaOffset::Offset(o) if o >= 0 => Offset::Offset(o as _),
                KafkaOffset::Offset(o) => Offset::OffsetTail((-o - 1) as _),
                KafkaOffset::OffsetInterval(b, _) => Offset::Offset(b as _),
                KafkaOffset::TimeInterval(b, _e) => {
                    let mut tpl_b = TopicPartitionList::new();
                    tpl_b.add_partition_offset(&topic, partition, Offset::Offset(b as _))?;
                    tpl_b = consumer.offsets_for_times(tpl_b, Duration::from_secs(1))?;
                    tpl_b.find_partition(&topic, partition).unwrap().offset()
                },
            };
            Ok((consumer, r))
        })
        .await
        .unwrap()
    };
    let (consumer, offset) = r?;
    tpl.add_partition_offset(&topic, partition, offset).unwrap();
    Ok((consumer, tpl))
}

async fn run_async_copy_topic(consumer_config: KafkaConfig, producer_config: KafkaConfig, input_topic: &str, output_topic: &str) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = consumer_config.clone().into();
    let (consumer, tpl) = get_topic_partition_list(consumer, input_topic.to_owned(), consumer_config.partition, consumer_config.offset)
        .await
        .unwrap();
    consumer.assign(&tpl).unwrap();

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = producer_config.into();

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

async fn run_async_consume_topic(config: &Config, consumer_conf: KafkaConfig, topic: &str) {
    let consumer: StreamConsumer = consumer_conf.clone().into();
    let (consumer, tpl) = get_topic_partition_list(consumer, topic.to_owned(), consumer_conf.partition, consumer_conf.offset).await.unwrap();
    consumer.assign(&tpl).unwrap();

    consumer
        .stream()
        .timeout(if config.exit { Duration::from_secs(3) } else { Duration::from_secs(3600) })
        .try_for_each(|borrowed_message| {
            let owned_message = borrowed_message.detach();
            async move {
                process_message(&owned_message);
                Ok(())
            }
        })
        .await
        .unwrap();
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
async fn main() {
    let matches = get_arg_matches();
    setup_logger(true, matches.value_of("log-conf"));
    let config = Box::leak(Box::new(Config::from(matches))) as &Config;

    info!("Starting {:?}", config.mode);

    match config.mode {
        WorkingMode::Consumer => {
            (0..config.num_workers)
                .map(|_| {
                    tokio::spawn(run_async_consume_topic(
                        config,
                        config.into(),
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
                        config.into(),
                        config.into(),
                        config.input_topic.as_ref().expect("Must use input_topic"),
                        config.input_topic.as_ref().expect("Must use output_topic"),
                    ))
                })
                .collect::<FuturesUnordered<_>>()
                .for_each(|_| async { () })
                .await
        },
        _ => {},
    }
}
