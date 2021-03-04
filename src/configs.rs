use clap::crate_version;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use log::LevelFilter;
use regex::Regex;
use std::str::FromStr;
use strum::Display;
use strum::EnumString;

const GROUP_ID_DEFAULT: &str = "kafcat";
const BROKERS_DEFAULT: &str = "localhost:9092";
const MSG_DELIMITER_DEFAULT: &str = "\n";
const KEY_DELIMITER_DEFAULT: &str = "\n";
const OFFSET_DEFAULT: &str = "Beginning";
const FORMAT_DEFAULT: &str = "text";

pub fn group_id() -> Arg<'static> {
    Arg::new("group-id")
        .short('G')
        .long("group-id")
        .about("Consumer group id. (Kafka >=0.9 balanced consumer groups)")
        .default_value(GROUP_ID_DEFAULT)
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
pub fn brokers() -> Arg<'static> { Arg::new("brokers").short('b').long("brokers").about("Broker list in kafka format").default_value(BROKERS_DEFAULT) }
pub fn partition() -> Arg<'static> { Arg::new("partition").short('p').long("partition").about("Partition").takes_value(true) }
pub fn exit() -> Arg<'static> { Arg::new("exit").short('e').long("exit").about("Exit successfully when last message received") }
pub fn format() -> Arg<'static> { Arg::new("format").short('s').long("format").about("Serialize/Deserialize format").default_value(FORMAT_DEFAULT) }

pub fn msg_delimiter() -> Arg<'static> {
    Arg::new("msg-delimiter")
        .short('D')
        .about("Delimiter to split input into messages(currently only supports '\\n')")
        .default_value(MSG_DELIMITER_DEFAULT)
}
pub fn key_delimiter() -> Arg<'static> {
    Arg::new("key-delimiter")
        .short('K')
        .about("Delimiter to split input key and message")
        .default_value(KEY_DELIMITER_DEFAULT)
}
pub fn consume_subcommand() -> App<'static> {
    App::new("consume").short_flag('C').args(vec![
        brokers(),
        group_id(),
        topic().required(true),
        partition(),
        offset(),
        exit(),
        msg_delimiter(),
        key_delimiter(),
        format(),
    ])
}
pub fn produce_subcommand() -> App<'static> {
    App::new("produce")
        .short_flag('P')
        .args(vec![brokers(), group_id(), topic().required(true), partition(), msg_delimiter(), key_delimiter(), format()])
}
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum SerdeFormat {
    Text,
    Json,
    Regex(String),
}

impl FromStr for SerdeFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, String> {
        Ok(match value {
            "text" => SerdeFormat::Text,
            "json" => SerdeFormat::Json,
            _ => SerdeFormat::Regex(value.to_owned()),
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
    pub msg_delim:    String,
    pub key_delim:    String,
    pub format:       SerdeFormat,
}

impl KafkaConsumerConfig {
    pub fn from_matches(matches: &ArgMatches) -> KafkaConsumerConfig {
        let brokers = matches.value_of("brokers").expect("Must specify brokers").to_owned();
        let group_id = matches.value_of("group-id").unwrap_or("kafcat").to_owned();
        let offset = matches.value_of("offset").map(|x| x.parse().expect("Cannot parse offset")).unwrap_or(KafkaOffset::Beginning);
        let partition = matches.value_of("partition").map(|x| x.parse().expect("Cannot parse partition"));
        let exit = matches.occurrences_of("exit") > 0;
        let topic = matches.value_of("topic").expect("Must specify topic").to_owned();
        let format = matches.value_of("format").expect("Must specify format");
        let msg_delim = matches.value_of("msg-delimiter").unwrap().to_owned();
        let key_delim = matches.value_of("key-delimiter").unwrap().to_owned();
        KafkaConsumerConfig {
            brokers,
            group_id,
            offset,
            partition,
            topic,
            format: SerdeFormat::from_str(format).unwrap(),
            exit_on_done: exit,
            msg_delim,
            key_delim,
        }
    }
}
impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        KafkaConsumerConfig {
            brokers:      BROKERS_DEFAULT.to_string(),
            group_id:     GROUP_ID_DEFAULT.to_string(),
            offset:       KafkaOffset::from_str(OFFSET_DEFAULT).unwrap(),
            partition:    None,
            topic:        "".to_string(),
            format:       SerdeFormat::from_str(FORMAT_DEFAULT).unwrap(),
            exit_on_done: false,
            msg_delim:    MSG_DELIMITER_DEFAULT.to_string(),
            key_delim:    KEY_DELIMITER_DEFAULT.to_string(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KafkaProducerConfig {
    pub brokers:   String,
    pub group_id:  String,
    pub partition: Option<i32>,
    pub topic:     String,
    pub msg_delim: String,
    pub key_delim: String,
    pub format:    SerdeFormat,
}
impl KafkaProducerConfig {
    pub fn from_matches(matches: &ArgMatches) -> KafkaProducerConfig {
        let brokers = matches.value_of("brokers").expect("Must specify brokers").to_owned();
        let group_id = matches.value_of("group-id").unwrap_or("kafcat").to_owned();
        let partition = matches.value_of("partition").map(|x| x.parse().expect("Cannot parse partition"));
        let topic = matches.value_of("topic").expect("Must specify topic").to_owned();
        let msg_delim = matches.value_of("msg-delimiter").unwrap().to_owned();
        let key_delim = matches.value_of("key-delimiter").unwrap().to_owned();
        let format = matches.value_of("format").expect("Must specify format");
        KafkaProducerConfig {
            brokers: brokers.to_owned(),
            group_id: group_id.clone(),
            partition,
            topic,
            msg_delim,
            key_delim,
            format: SerdeFormat::from_str(format).unwrap(),
        }
    }
}
impl Default for KafkaProducerConfig {
    fn default() -> Self {
        KafkaProducerConfig {
            brokers:   BROKERS_DEFAULT.to_string(),
            group_id:  GROUP_ID_DEFAULT.to_string(),
            partition: None,
            topic:     "".to_string(),
            msg_delim: MSG_DELIMITER_DEFAULT.to_string(),
            key_delim: KEY_DELIMITER_DEFAULT.to_string(),
            format:    SerdeFormat::from_str(FORMAT_DEFAULT).unwrap(),
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
            brokers: "localhost".to_string(),
            group_id: "kafcat".to_string(),
            offset: KafkaOffset::Beginning,
            partition: None,
            topic: "topic".to_string(),
            exit_on_done: true,
            ..Default::default()
        })
    }
    #[test]
    fn producer_config() {
        let config = AppConfig::from_args(vec!["kafcat", "-P", "-b", "localhost", "-t", "topic"]);
        assert_eq!(config.producer_kafka.unwrap(), KafkaProducerConfig {
            brokers: "localhost".to_string(),
            group_id: "kafcat".to_string(),
            partition: None,
            topic: "topic".to_string(),
            ..Default::default()
        })
    }
    #[test]
    fn copy_config() {
        let config = AppConfig::from_args(vec!["kafcat", "copy", "-b", "localhost1", "-t", "topic1", "-e", "--", "-b", "localhost2", "-t", "topic2"]);
        assert_eq!(config.consumer_kafka.unwrap(), KafkaConsumerConfig {
            brokers: "localhost1".to_string(),
            group_id: "kafcat".to_string(),
            offset: KafkaOffset::Beginning,
            partition: None,
            topic: "topic1".to_string(),
            exit_on_done: true,
            ..Default::default()
        });
        assert_eq!(config.producer_kafka.unwrap(), KafkaProducerConfig {
            brokers: "localhost2".to_string(),
            group_id: "kafcat".to_string(),
            partition: None,
            topic: "topic2".to_string(),
            ..Default::default()
        });
    }
}
