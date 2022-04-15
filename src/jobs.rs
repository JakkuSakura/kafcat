use std::collections::{HashMap, HashSet};

use kafka::client::KafkaClient;
use serde::Deserialize;

use crate::{
    configs::{KafkaAuthConfig, KafkaProducerConfig, SerdeFormat},
    interface::KafkaProducer,
    message::KafkaMessage,
    rdkafka_impl::{RdKafkaAdmin, RdkafkaProducer},
};

/// Defines the producer job to dump the specified file to kafka.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ProducerJob {
    topic_name: String,

    /// The file path.
    file: String,

    /// The total partitions of the topic.
    #[serde(default = "default_num_partitions")]
    partitions: i32,

    format: SerdeFormat,
}

fn default_num_partitions() -> i32 {
    1
}

impl ProducerJob {
    async fn run(&self, auth: &KafkaAuthConfig) {
        let cfg = KafkaProducerConfig {
            auth: auth.clone(),
            format: self.format.clone(),
            topic: self.topic_name.clone(),
            ..Default::default()
        };
        let producer = RdkafkaProducer::from_config(cfg.clone()).await;

        let absolute_path = std::fs::canonicalize(&self.file).unwrap();
        println!("{}", absolute_path.to_string_lossy());

        let content = std::fs::read_to_string(&absolute_path)
            .unwrap_or_else(|e| panic!("Cannot read data file {}: {}", self.file, e));
        for msg in content.split(cfg.msg_delim.as_str()) {
            producer
                .write_one(KafkaMessage {
                    key: "".as_bytes().to_vec(),
                    payload: msg.as_bytes().to_vec(),
                    timestamp: std::time::Instant::now().elapsed().as_millis() as i64,
                    headers: HashMap::new(),
                })
                .await
                .unwrap_or_else(|e| panic!("Failed to write message to Kafka: {}", e));
        }
    }
}

/// The jobs config. The jobs will be executed sequentially.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct JobsConfig {
    producer_jobs: Vec<ProducerJob>,
}

impl JobsConfig {
    /// Read YAML config from file path.
    pub fn from_config_file(path: &str) -> Self {
        let file = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Cannot read config file {}: {}", path, e));
        serde_yaml::from_str(&file)
            .unwrap_or_else(|e| panic!("Cannot parse config file {}: {}", path, e))
    }

    pub async fn run(&self, auth: &KafkaAuthConfig) {
        let mut client = KafkaClient::new(auth.brokers.clone());
        client
            .load_metadata_all()
            .unwrap_or_else(|e| panic!("Failed to load kafka metadata: {}", e));

        // The pure-rust kafka client doesn't support `CreateTopic` API.
        let admin_client = RdKafkaAdmin::create(auth);

        let topics = client
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect::<HashSet<String>>();

        let non_existed_topics = self
            .producer_jobs
            .iter()
            .filter(|j| topics.get(&j.topic_name).is_none())
            .collect::<Vec<&ProducerJob>>();

        for job in non_existed_topics {
            admin_client
                .create_topic(&job.topic_name, job.partitions)
                .await;
        }

        for job in &self.producer_jobs {
            job.run(auth).await;
        }
    }
}
