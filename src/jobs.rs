use std::collections::HashMap;

use serde::Deserialize;

use crate::{
    configs::{KafkaAuthConfig, KafkaProducerConfig, SerdeFormat},
    interface::KafkaProducer,
    message::KafkaMessage,
    rdkafka_impl::RdkafkaProducer,
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
        info!("Processing {}", absolute_path.to_string_lossy());

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
        for job in &self.producer_jobs {
            job.run(auth).await;
        }
    }
}
