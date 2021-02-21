use clap::crate_version;
use clap::App;
use clap::Arg;
use clap::ArgGroup;
use clap::ArgMatches;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use strum::Display;
use strum::EnumString;

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
        .arg(Arg::new("partition").short('p').long("partition").about("Partition").takes_value(true))
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
        .arg(Arg::new("exit").short('e').long("exit").about("Exit successfully when last message received"))
        .group(
            ArgGroup::new("topics")
                .args(&["input-topic", "output-topic"])
                .requires_all(&["input-topic", "output-topic", "copy"])
                .conflicts_with("topic"),
        )
        .arg(Arg::new("num-workers").long("num-workers").about("Number of workers").takes_value(true).default_value("1"))
        .get_matches()
}

#[derive(Debug, Copy, Clone, EnumString, Display)]
#[strum(serialize_all = "lowercase")]
pub enum WorkingMode {
    Unspecified,
    Consumer,
    Producer,
    Metadata,
    Query,
    Copy,
}
impl Default for WorkingMode {
    fn default() -> Self { Self::Unspecified }
}
#[derive(Debug, Clone, Copy)]
pub enum KafkaOffset {
    Beginning,
    End,
    Stored,
    Offset(isize),
    OffsetInterval(i64, i64),
    TimeInterval(i64, i64),
}
impl Default for KafkaOffset {
    fn default() -> Self { Self::Beginning }
}

use regex::Regex;
use std::str::FromStr;

impl FromStr for KafkaOffset {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, String> {
        Ok(match value {
            "beginning" => KafkaOffset::Beginning,
            "end" => KafkaOffset::End,
            "stored" => KafkaOffset::Stored,
            _ => {
                if let Ok(val) = value.parse() {
                    KafkaOffset::Offset(val)
                } else if let Some(x) = Regex::new(r"s@(\d+)e@(\d+)$").unwrap().captures(value) {
                    let b = x.get(1).unwrap().as_str();
                    let e = x.get(2).unwrap().as_str();
                    KafkaOffset::TimeInterval(b.parse().unwrap(), e.parse().unwrap())
                } else {
                    return Err(format!("Cannot parse {} as offset", value));
                }
            },
        })
    }
}
#[derive(Debug, Clone, Default)]
pub struct Config {
    matches:          ArgMatches,
    pub group_id:     String,
    pub exit:         bool,
    pub offset:       KafkaOffset,
    pub mode:         WorkingMode,
    pub brokers:      String,
    pub num_workers:  i32,
    pub topic:        Option<String>,
    pub partition:    Option<i32>,
    pub input_topic:  Option<String>,
    pub output_topic: Option<String>,
}

impl From<ArgMatches> for Config {
    fn from(matches: ArgMatches) -> Self {
        let brokers = matches.value_of_t_or_exit("brokers");
        let group_id = matches.value_of("group-id").unwrap().into();
        let num_workers = matches.value_of_t_or_exit("num-workers");

        let mut working_mode = None;
        for mode in &[WorkingMode::Producer, WorkingMode::Consumer, WorkingMode::Metadata, WorkingMode::Query, WorkingMode::Copy] {
            if matches.is_present(&mode.to_string()) {
                working_mode = Some(mode);
            }
        }

        let topic = matches.value_of("topic").map(|x| x.into());
        let input_topic = matches.value_of("input-topic").map(|x| x.into());
        let output_topic = matches.value_of("output-topic").map(|x| x.into());
        let offset = matches.value_of("offset").map(|x| x.parse().expect("Cannot parse offset")).unwrap_or(KafkaOffset::Beginning);
        let partition = matches.value_of("partition").map(|x| x.parse().expect("Cannot parse partition"));
        let exit = matches.occurrences_of("exit") > 0;
        Config {
            matches,
            group_id,
            exit,
            offset,
            mode: *working_mode.unwrap(),
            brokers,
            num_workers,
            topic,
            partition,
            input_topic,
            output_topic,
        }
    }
}
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers:   String,
    pub group_id:  String,
    pub offset:    KafkaOffset,
    pub partition: Option<i32>,
}

impl From<&Config> for KafkaConfig {
    fn from(x: &Config) -> Self {
        KafkaConfig {
            brokers:   x.brokers.clone(),
            group_id:  x.group_id.clone(),
            offset:    x.offset,
            partition: x.partition,
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
