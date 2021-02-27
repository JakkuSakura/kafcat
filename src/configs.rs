use clap::crate_version;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use strum::Display;
use strum::EnumString;

pub fn group_id() -> Arg<'static> {
    Arg::new("group-id")
        .short('G')
        .long("group-id")
        .about("Consumer group id. (Kafka >=0.9 balanced consumer groups)")
        .default_value("kafcat")
}
pub fn offset() -> Arg<'static> {
    Arg::new("offset").short('o').takes_value(true).default_value(r#"beginning"#).long_about(
        r#"Offset to start consuming from:
                     beginning | end | stored |
                     <value>  (absolute offset) |
                     -<value> (relative offset from end)
                     s@<value> (timestamp in ms to start at)
                     e@<value> (timestamp in ms to stop at (not included))"#,
    )
}
pub fn topic() -> Arg<'static> { Arg::new("topic").short('t').long("topic").about("Topic").takes_value(true) }
pub fn brokers() -> Arg<'static> { Arg::new("brokers").short('b').long("brokers").about("Broker list in kafka format").default_value("localhost:9092") }
pub fn partition() -> Arg<'static> { Arg::new("partition").short('p').long("partition").about("Partition").takes_value(true) }
pub fn exit() -> Arg<'static> { Arg::new("exit").short('e').long("exit").about("Exit successfully when last message received") }
pub fn consume_subcommand() -> App<'static> {
    App::new("consume")
        .short_flag('C')
        .args(vec![brokers(), group_id(), topic().required(true), partition(), offset(), exit()])
}
pub fn produce_subcommand() -> App<'static> { App::new("produce").short_flag('P').args(vec![brokers(), group_id(), topic().required(true), partition()]) }
pub fn copy_subcommand() -> App<'static> {
    // this is not meant to be used directly only for help message
    App::new("copy")
        .setting(AppSettings::AllowLeadingHyphen)
        .alias("--cp")
        .arg(Arg::new("from").multiple(true).required(true))
        .arg(Arg::new("to").multiple(true).last(true).required(true))
        .about("Copy mode accepts two parts of arguments <from> and <to>, the two parts are separated by [--]. <from> is the exact as Consumer mode, and <to> is the exact as Producer mode.")
}

pub fn get_arg_matcher() -> App<'static> {
    App::new("kafcat")
        .version(crate_version!())
        .author("Jiangkun Qiu <qiujiangkun@foxmail.com>")
        .about("cat but kafka")
        .subcommands(vec![consume_subcommand(), produce_subcommand(), copy_subcommand()])
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::new("log")
                .long("log")
                .about("Configure the logging format: Off, Error, Warn, Info, Debug, Trace")
                .takes_value(true),
        )
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
impl WorkingMode {
    pub fn should_have_input_kafka(self) -> bool {
        match self {
            WorkingMode::Unspecified => false,
            WorkingMode::Consumer => true,
            WorkingMode::Producer => false,
            WorkingMode::Metadata => true,
            WorkingMode::Query => true,
            WorkingMode::Copy => true,
        }
    }

    pub fn should_have_output_kafka(self) -> bool {
        match self {
            WorkingMode::Unspecified => false,
            WorkingMode::Consumer => false,
            WorkingMode::Producer => true,
            WorkingMode::Metadata => false,
            WorkingMode::Query => false,
            WorkingMode::Copy => true,
        }
    }
}

impl Default for WorkingMode {
    fn default() -> Self { Self::Unspecified }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
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

use log::LevelFilter;
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

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub working_mode:   WorkingMode,
    pub consumer_kafka: Option<KafkaConsumerConfig>,
    pub producer_kafka: Option<KafkaProducerConfig>,
    pub log_level:      LevelFilter,
}
impl AppConfig {
    pub fn from_args(args: Vec<&str>) -> Self {
        let matches = get_arg_matcher().get_matches_from(args);

        let kafcat_log_env = std::env::var("KAFCAT_LOG").ok();
        let log_level = matches
            .value_of("log")
            .or(kafcat_log_env.as_ref().map(|x| x.as_str()))
            .map(|x| LevelFilter::from_str(x).expect("Cannot parse log level"))
            .unwrap_or(LevelFilter::Info);

        let mut this = AppConfig {
            working_mode: WorkingMode::Unspecified,
            consumer_kafka: None,
            producer_kafka: None,
            log_level,
        };
        match matches.subcommand() {
            Some(("consume", matches)) => {
                this.working_mode = WorkingMode::Consumer;
                this.consumer_kafka = Some(KafkaConsumerConfig::from_matches(matches));
            },
            Some(("produce", matches)) => {
                this.working_mode = WorkingMode::Producer;
                this.producer_kafka = Some(KafkaProducerConfig::from_matches(matches));
            },
            Some(("copy", matches)) => {
                this.working_mode = WorkingMode::Copy;
                let from = vec!["kafka"].into_iter().chain(matches.values_of("from").expect("Must specify from"));
                let to = vec!["kafka"].into_iter().chain(matches.values_of("to").expect("Must specify to"));

                let consumer = consume_subcommand().get_matches_from(from);
                let producer = produce_subcommand().get_matches_from(to);
                this.consumer_kafka = Some(KafkaConsumerConfig::from_matches(&consumer));
                this.producer_kafka = Some(KafkaProducerConfig::from_matches(&producer));
            },
            _ => unreachable!(),
        }

        this
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KafkaConsumerConfig {
    pub brokers:      String,
    pub group_id:     String,
    pub offset:       KafkaOffset,
    pub partition:    Option<i32>,
    pub topic:        String,
    pub exit_on_done: bool,
}

impl KafkaConsumerConfig {
    pub fn from_matches(matches: &ArgMatches) -> KafkaConsumerConfig {
        let brokers = matches.value_of("brokers").expect("Must specify brokers").to_owned();
        let group_id = matches.value_of("group-id").unwrap_or("kafcat").to_owned();
        let offset = matches.value_of("offset").map(|x| x.parse().expect("Cannot parse offset")).unwrap_or(KafkaOffset::Beginning);
        let partition = matches.value_of("partition").map(|x| x.parse().expect("Cannot parse partition"));
        let exit = matches.occurrences_of("exit") > 0;
        let topic = matches.value_of("topic").expect("Must specify topic").to_owned();
        KafkaConsumerConfig {
            brokers,
            group_id,
            offset,
            partition,
            topic,
            exit_on_done: exit,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KafkaProducerConfig {
    pub brokers:   String,
    pub group_id:  String,
    pub partition: Option<i32>,
    pub topic:     String,
}
impl KafkaProducerConfig {
    pub fn from_matches(matches: &ArgMatches) -> KafkaProducerConfig {
        let brokers = matches.value_of("brokers").expect("Must specify brokers").to_owned();
        let group_id = matches.value_of("group-id").unwrap_or("kafcat").to_owned();
        let partition = matches.value_of("partition").map(|x| x.parse().expect("Cannot parse partition"));
        let topic = matches.value_of("topic").expect("Must specify topic").to_owned();

        KafkaProducerConfig {
            brokers: brokers.to_owned(),
            group_id: group_id.clone(),
            partition,
            topic,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::configs::AppConfig;
    use crate::configs::KafkaConsumerConfig;
    use crate::configs::KafkaOffset;
    use crate::configs::KafkaProducerConfig;

    #[test]
    fn consumer_config() {
        let config = AppConfig::from_args(vec!["kafcat", "-C", "-b", "localhost", "-t", "topic", "-e"]);
        assert_eq!(config.consumer_kafka.unwrap(), KafkaConsumerConfig {
            brokers:      "localhost".to_string(),
            group_id:     "kafcat".to_string(),
            offset:       KafkaOffset::Beginning,
            partition:    None,
            topic:        "topic".to_string(),
            exit_on_done: true,
        })
    }
    #[test]
    fn producer_config() {
        let config = AppConfig::from_args(vec!["kafcat", "-P", "-b", "localhost", "-t", "topic"]);
        assert_eq!(config.producer_kafka.unwrap(), KafkaProducerConfig {
            brokers:   "localhost".to_string(),
            group_id:  "kafcat".to_string(),
            partition: None,
            topic:     "topic".to_string(),
        })
    }
    #[test]
    fn copy_config() {
        let config = AppConfig::from_args(vec!["kafcat", "copy", "-b", "localhost1", "-t", "topic1", "-e", "--", "-b", "localhost2", "-t", "topic2"]);
        assert_eq!(config.consumer_kafka.unwrap(), KafkaConsumerConfig {
            brokers:      "localhost1".to_string(),
            group_id:     "kafcat".to_string(),
            offset:       KafkaOffset::Beginning,
            partition:    None,
            topic:        "topic1".to_string(),
            exit_on_done: true,
        });
        assert_eq!(config.producer_kafka.unwrap(), KafkaProducerConfig {
            brokers:   "localhost2".to_string(),
            group_id:  "kafcat".to_string(),
            partition: None,
            topic:     "topic2".to_string(),
        });
    }
}
