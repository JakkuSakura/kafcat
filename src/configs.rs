use clap::{crate_version, App, Arg, ArgGroup, ArgMatches};
use rdkafka::{
    client::DefaultClientContext,
    consumer::{DefaultConsumerContext, StreamConsumer},
    producer::FutureProducer,
    util::DefaultRuntime,
    ClientConfig,
};

pub fn get_arg_matches() -> ArgMatches {
    App::new("kafcat")
        .version(crate_version!())
        .author("Jiangkun Qiu <qiujiangkun@foxmail.com>")
        .about("cat but kafka")
        .help_heading("MODE")
        .arg(Arg::new("consumer").short('C').about("use Consumer mode").group("mode"))
        .arg(Arg::new("producer").short('P').about("use Producer mode").group("mode"))
        .arg(Arg::new("metadata").short('L').about("use Metadata mode").group("mode"))
        .arg(Arg::new("query").short('Q').about("use Query mode").group("mode"))
        .arg(Arg::new("copy").long("cp").about("use Copy mode").group("mode"))
        .group(ArgGroup::new("mode").required(true).args(&["consumer", "producer", "metadata", "query"]))
        .help_heading("OPTIONS")
        .arg(Arg::new("brokers").short('b').long("brokers").about("Broker list in kafka format").default_value("localhost:9092"))
        .arg(
            Arg::new("group-id")
                .short('G')
                .long("group-id")
                .about("Consumer group id. (Kafka >=0.9 balanced consumer groups)")
                .default_value("kafcat"),
        )
        .arg(Arg::new("log-conf").long("log-conf").about("Configure the logging format (example: 'rdkafka=trace')").takes_value(true))
        .arg(Arg::new("topic").short('t').long("topic").about("Topic").takes_value(true))
        .arg(Arg::new("input-topic").long("input-topic").about("Input topic").takes_value(true))
        .arg(Arg::new("output-topic").long("output-topic").about("Output topic").takes_value(true))
        .arg(Arg::new("offset").short('o').takes_value(true).default_value(r#"beginning"#).long_about(
            r#"Offset to start consuming from:
                     beginning | end | stored |
                     <value>  (absolute offset) |
                     -<value> (relative offset from end)
                     s@<value> (timestamp in ms to start at)
                     e@<value> (timestamp in ms to stop at (not included))"#,
        ))
        .arg(Arg::new("exit").short('e').about("Exit successfully when last message received").default_value("true"))
        .group(
            ArgGroup::new("topics")
                .args(&["input-topic", "output-topic"])
                .requires_all(&["input-topic", "output-topic", "copy"])
                .conflicts_with("topic"),
        )
        .arg(Arg::new("num-workers").long("num-workers").about("Number of workers").takes_value(true).default_value("1"))
        .get_matches()
}
#[derive(Debug, Clone)]
pub enum WorkingMode {
    Consumer,
    Producer,
    Metadata,
    Query,
    Copy,
}

#[derive(Debug, Clone)]
pub enum Offset {
    Beginning,
    End,
    Resume,
    Offset(usize),
}

#[derive(Debug, Clone)]
pub struct Config {
    matches:          ArgMatches,
    pub group_id:     String,
    pub exit:         bool,
    pub offset:       Offset,
    pub mode:         WorkingMode,
    pub brokers:      String,
    pub num_workers:  i32,
    pub topic:        Option<String>,
    pub input_topic:  Option<String>,
    pub output_topic: Option<String>,
}

impl From<ArgMatches> for Config {
    fn from(matches: ArgMatches) -> Self {
        let brokers = matches.value_of_t_or_exit("brokers");
        let group_id = matches.value_of("group-id").unwrap().into();
        let num_workers = matches.value_of_t_or_exit("num-workers");

        let mut mode = None;
        for direction in &["consumer", "producer", "metadata", "query", "copy"] {
            if matches.is_present(direction) {
                mode = Some(direction);
            }
        }
        let mode = match *mode.unwrap() {
            "consumer" => WorkingMode::Consumer,
            "producer" => WorkingMode::Producer,
            "metadata" => WorkingMode::Metadata,
            "query" => WorkingMode::Query,
            "copy" => WorkingMode::Copy,
            _ => unreachable!(),
        };

        let topic = matches.value_of("topic").map(|x| x.into());
        let input_topic = matches.value_of("input-topic").map(|x| x.into());
        let output_topic = matches.value_of("output-topic").map(|x| x.into());

        Config {
            matches,
            group_id,
            exit: false,
            offset: Offset::Resume,
            mode,
            brokers,
            num_workers,
            topic,
            input_topic,
            output_topic,
        }
    }
}
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    brokers:  String,
    group_id: String,
}

impl From<&Config> for KafkaConfig {
    fn from(x: &Config) -> Self {
        KafkaConfig {
            brokers:  x.brokers.clone(),
            group_id: x.group_id.clone(),
        }
    }
}
impl Into<StreamConsumer> for KafkaConfig {
    fn into(self) -> StreamConsumer {
        ClientConfig::new()
            .set("group.id", &self.group_id)
            .set("bootstrap.servers", &self.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Consumer creation failed")
    }
}
impl Into<FutureProducer> for KafkaConfig {
    fn into(self) -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error")
    }
}
