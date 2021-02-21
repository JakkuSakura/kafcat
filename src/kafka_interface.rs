use crate::configs::Config;
use crate::configs::KafkaConfig;
use crate::configs::KafkaOffset;
use crate::error::KafcatError;
use async_trait::async_trait;

#[async_trait]
pub trait KafkaInterface {
    type Message: CustomMessage;
    type Consumer: CustomConsumer<Message = Self::Message>;
    type Producer: CustomProducer<Message = Self::Message>;
    fn set_config(&mut self, config: Config);
    async fn get_producer(&mut self, consumer: KafkaConfig, topic: &str) -> Self::Producer;
    async fn get_consumer(&mut self, consumer: KafkaConfig, topic: &str) -> Self::Consumer;
}

#[async_trait]
pub trait CustomConsumer {
    type Message;
    fn from_config(config: &Config, kafka_config: KafkaConfig, topic: &str, partition: Option<i32>) -> Self
    where
        Self: Sized;

    async fn set_offset(&mut self, topic: String, partition: Option<i32>, offset: KafkaOffset) -> Result<(), KafcatError>;
    async fn get_one(&mut self) -> Self::Message;
}

#[async_trait]
pub trait CustomProducer {
    type Message;
    fn from_config(config: &Config, kafka_config: KafkaConfig, topic: &str) -> Self
    where
        Self: Sized;
    async fn write_one(&mut self, msg: Self::Message) -> Result<(), KafcatError>;
}

pub trait CustomMessage {
    fn get_key(&self) -> &[u8];
    fn get_payload(&self) -> Vec<u8>;
    fn get_timestamp(&self) -> i64;
    fn set_key(&self, key: Vec<u8>);
    fn set_payload(&self, payload: Vec<u8>);
    fn set_timestamp(&self, key: i64);
}
