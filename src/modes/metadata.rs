use kafcat::configs::AppConfig;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    metadata::Metadata,
    util::Timeout,
    ClientConfig,
};
use tabwriter::TabWriter;

pub fn get_metadata(config: AppConfig) {
    let input_config = config
        .metadata_kafka
        .expect("Must specify input kafka config");
    let stream = ClientConfig::new()
        .set("group.id", &input_config.group_id)
        .set("bootstrap.servers", &input_config.brokers)
        .set_log_level(RDKafkaLogLevel::Debug)
        .create::<StreamConsumer>()
        .expect("Consumer creation failed");
    let metadata = stream.fetch_metadata(None, Timeout::Never).unwrap();
    print_topics(&metadata);
}

fn print_topics(metadata: &Metadata) {
    // NOTE: Can be optimized by precalculating the required memory, I think.
    let mut table_string = TabWriter::new(vec![]);
    let topics = metadata.topics();
    // TODO: Collect into rows, skipping Options
    let raw_string = topics
        .iter()
        .enumerate()
        .map(|(idx, metadata_topic)| {
            format!(
                "{}.\t{}\t{}\t{}",
                idx + 1,
                metadata_topic.name(),
                metadata_topic.partitions().len(),
                metadata_topic.error().map(|_| "true").unwrap_or("false")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::io::Write::write_all(&mut table_string, b"No.\tName\tNo. of Partitions\tError\n").unwrap();
    std::io::Write::write_all(&mut table_string, raw_string.as_bytes()).unwrap();
    std::io::Write::flush(&mut table_string).unwrap();
    let written = String::from_utf8(table_string.into_inner().unwrap()).unwrap();
    println!("{} Topics\n{}", topics.len(), written);
}
