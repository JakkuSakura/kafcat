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
use kafcat::kafka_interface::CustomConsumer;
use kafcat::kafka_interface::CustomMessage;
use kafcat::kafka_interface::KafkaInterface;
use kafcat::rdkafka_impl::RdKafka;
use kafcat::timeout_stream::TimeoutStreamExt;
use log::info;
use log::LevelFilter;
use log::Record;
use std::io::Write;
use std::sync::Arc;
use tokio::task::spawn_blocking;

fn process_message<Msg: CustomMessage>(msg: Msg) {
    println!("{:?}", msg.get_payload());
}

async fn run_async_copy_topic<Interface: KafkaInterface>(interface: Interface, consumer_config: KafkaConfig, producer_config: KafkaConfig, input_topic: &str, output_topic: &str) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer = Interface::Consumer::from_config(interface.get_config(), consumer_config, input_topic, interface.get_config().partition);
    unimplemented!();
    // // Create the `FutureProducer` to produce asynchronously.
    // let producer: FutureProducer = producer_config.into();
    //
    // // Create the outer pipeline on the message stream.
    // let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
    //     let producer = producer.clone();
    //     let output_topic = output_topic.to_string();
    //     async move {
    //         let owned_message = borrowed_message.detach();
    //         tokio::spawn(async move {
    //             let mut record = FutureRecord::to(&output_topic);
    //             if let Some(key) = owned_message.key() {
    //                 record = record.key(key);
    //             }
    //             if let Some(payload) = owned_message.payload() {
    //                 record = record.payload(payload)
    //             }
    //
    //             let produce_future = producer.send(record, Duration::from_secs(0));
    //             match produce_future.await {
    //                 Ok(delivery) => println!("Sent: {:?}", delivery),
    //                 Err((e, _)) => println!("Error: {:?}", e),
    //             }
    //         });
    //         Ok(())
    //     }
    // });
    //
    // info!("Starting event loop");
    // stream_processor.await.expect("stream processing failed");
    // info!("Stream processing terminated");
}

async fn run_async_consume_topic<Interface: KafkaInterface>(interface: Interface, config: &AppConfig, consumer_config: KafkaConfig, topic: &str) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let mut consumer: Arc<Interface::Consumer> = Arc::new(Interface::Consumer::from_config(
        interface.get_config(),
        consumer_config.clone(),
        topic,
        interface.get_config().partition,
    ));
    consumer = consumer.set_offset(&topic, consumer_config.partition, consumer_config.offset).await.unwrap();
    consumer
        .for_each(|x| async {
            process_message(x);
            Ok(())
        })
        .await;
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
    let config = Box::leak(Box::new(AppConfig::from(matches))) as &AppConfig;

    info!("Starting {:?}", config.mode);

    let interface = RdKafka::from_config(config.clone());
    match config.mode {
        WorkingMode::Consumer => {
            run_async_consume_topic(interface, config, config.into(), config.topic.as_ref().or(config.input_topic.as_ref()).expect("Must use topic")).await;
        },
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
            .await;
        },
        _ => {},
    }
}
