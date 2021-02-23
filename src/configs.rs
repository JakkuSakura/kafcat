use clap::crate_version;
use clap::App;
use clap::Arg;
use clap::ArgGroup;
use clap::ArgMatches;
use strum::Display;
use strum::EnumString;

pub fn get_arg_matches() -> ArgMatches {
    let mut app = App::new("kafcat").version(crate_version!()).author("Jiangkun Qiu <qiujiangkun@foxmail.com>").about("cat but kafka");
    #[cfg(features = "help_heading")]
    {
        app = app.help_heading("MODE");
    }
    app = app
        .arg(Arg::new("consumer").short('C').about("use Consumer mode").group("mode"))
        .arg(Arg::new("producer").short('P').about("use Producer mode").group("mode"))
        .arg(Arg::new("metadata").short('L').about("use Metadata mode").group("mode"))
        .arg(Arg::new("query").short('Q').about("use Query mode").group("mode"))
        .arg(Arg::new("copy").long("cp").about("use Copy mode").group("mode"))
        .group(ArgGroup::new("mode").required(true).args(&["consumer", "producer", "metadata", "query"]));
    #[cfg(features = "help_heading")]
    {
        app = app.help_heading("OPTIONS");
    }
    app.arg(Arg::new("brokers").short('b').long("brokers").about("Broker list in kafka format").default_value("localhost:9092"))
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
        // .arg(Arg::new("num-workers").long("num-workers").about("Number of workers").takes_value(true).default_value("1"))
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
pub struct AppConfig {
    matches:          ArgMatches,
    pub group_id:     String,
    pub exit:         bool,
    pub offset:       KafkaOffset,
    pub mode:         WorkingMode,
    pub input_kafka:  Option<KafkaConfig>,
    pub output_kafka: Option<KafkaConfig>,
}
impl From<ArgMatches> for AppConfig {
    fn from(matches: ArgMatches) -> Self {
        let brokers = matches.value_of("brokers").expect("Must specify brokers").to_owned();
        let group_id = matches.value_of("group-id").unwrap_or("kafcat").to_owned();
        // let num_workers = matches.value_of_t_or_exit("num-workers");

        let mut working_mode = None;
        for mode in &[WorkingMode::Producer, WorkingMode::Consumer, WorkingMode::Metadata, WorkingMode::Query, WorkingMode::Copy] {
            if matches.is_present(&mode.to_string()) {
                working_mode = Some(*mode);
            }
        }
        let working_mode = working_mode.expect("Must specify one of the working mode");

        let offset = matches.value_of("offset").map(|x| x.parse().expect("Cannot parse offset")).unwrap_or(KafkaOffset::Beginning);
        let partition = matches.value_of("partition").map(|x| x.parse().expect("Cannot parse partition"));
        let exit = matches.occurrences_of("exit") > 0;

        let topic = matches.value_of("topic").map(|x| x.into());
        let input_topic: Option<String> = matches.value_of("input-topic").map(|x| x.into());
        let output_topic: Option<String> = matches.value_of("output-topic").map(|x| x.into());

        let input_kafka = if working_mode.should_have_output_kafka() {
            Some(KafkaConfig {
                brokers: brokers.to_owned(),
                group_id: group_id.clone(),
                offset,
                partition,
                topic: topic.clone().or(input_topic),
                exit_on_done: exit,
            })
        } else {
            None
        };

        let output_kafka = if working_mode.should_have_output_kafka() {
            Some(KafkaConfig {
                brokers: brokers.to_owned(),
                group_id: group_id.clone(),
                offset,
                partition,
                topic: topic.clone().or(output_topic),
                exit_on_done: exit,
            })
        } else {
            None
        };

        AppConfig {
            matches,
            group_id,
            exit,
            offset,
            mode: working_mode,
            input_kafka,
            output_kafka,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers:      String,
    pub group_id:     String,
    pub offset:       KafkaOffset,
    pub partition:    Option<i32>,
    pub topic:        Option<String>,
    pub exit_on_done: bool,
}
