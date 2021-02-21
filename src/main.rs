use std::{thread, time::Duration};

use clap::{App, Arg, ArgGroup, crate_version};
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use log::{info, LevelFilter, Record};
use rdkafka::{
    config::ClientConfig,
    consumer::{stream_consumer::StreamConsumer, Consumer},
    message::{BorrowedMessage, OwnedMessage},
    producer::{FutureProducer, FutureRecord},
    Message,
};
use std::io::Write;

use chrono::{DateTime, Local};
use env_logger::{fmt::Formatter, Builder};

async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    // Simulate some work that must be done in the same order as messages are
    // received; i.e., before truly parallel processing can begin.
    info!("Message received: {}", msg.offset());
}

async fn record_owned_message_receipt(_msg: &OwnedMessage) {
    // Like `record_borrowed_message_receipt`, but takes an `OwnedMessage`
    // instead, as in a real-world use case  an `OwnedMessage` might be more
    // convenient than a `BorrowedMessage`.
}

// Emulates an expensive, synchronous computation.
fn expensive_computation<'a>(msg: OwnedMessage) -> String {
    info!("Starting expensive computation on message {}", msg.offset());
    info!("Expensive computation completed on message {}", msg.offset());
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => format!("Payload len for {} is {}", payload, payload.len()),
        Some(Err(_)) => "Message payload is not a string".to_owned(),
        None => "No payload".to_owned(),
    }
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
async fn run_async_processor(brokers: String, group_id: String, input_topic: String, output_topic: String) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[&input_topic]).expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
        async move {
            // Process each message
            record_borrowed_message_receipt(&borrowed_message).await;
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            record_owned_message_receipt(&owned_message).await;
            tokio::spawn(async move {
                // The body of this block will be executed on the main thread pool,
                // but we perform `expensive_computation` on a separate thread pool
                // for CPU-intensive tasks via `tokio::task::spawn_blocking`.
                let computation_result = tokio::task::spawn_blocking(|| expensive_computation(owned_message))
                    .await
                    .expect("failed to wait for expensive computation");
                let produce_future = producer.send(FutureRecord::to(&output_topic).key("some key").payload(&computation_result), Duration::from_secs(0));
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
    let matches = App::new("kafcat")
        .version(crate_version!())
        .author("Jiangkun Qiu <qiujiangkun@foxmail.com>")
        .about("cat but kafka")
        .help_heading("MODE")
        .arg(Arg::new("consumer").short('C').about("use Consumer mode").group("mode"))
        .arg(Arg::new("producer").short('P').about("use Producer mode").group("mode"))
        .arg(Arg::new("metadata").short('L').about("use Metadata mode").group("mode"))
        .arg(Arg::new("query").short('Q').about("use Query mode").group("mode"))
        .group(
            ArgGroup::new("mode")
                .required(true)
                .args(&["consumer", "producer", "metadata", "query"])

            ,
        )
        .help_heading("OPTIONS")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .about("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("group-id")
                .short('G')
                .long("group-id")
                .about("Consumer group id. (Kafka >=0.9 balanced consumer groups)")
                .takes_value(true)
                .default_value("kafcat")
        )
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .about("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(Arg::new("topic").short('t').long("topic").about("Topic").takes_value(true).required(true))
        .arg(Arg::new("input-topic").long("input-topic").about("Input topic").takes_value(true).required(false))
        .arg(Arg::new("output-topic").long("output-topic").about("Output topic").takes_value(true).required(false))
        .arg(Arg::new("num-workers").long("num-workers").about("Number of workers").takes_value(true).default_value("1"))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();
    let num_workers = matches.value_of("num-workers").unwrap().parse().expect("Cannot parse num-workers");

    (0..num_workers)
        .map(|_| tokio::spawn(run_async_processor(brokers.to_owned(), group_id.to_owned(), input_topic.to_owned(), output_topic.to_owned())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
