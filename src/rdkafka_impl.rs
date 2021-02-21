use crate::configs::Config;
use crate::configs::KafkaConfig;
use crate::configs::KafkaOffset;
use crate::error::KafcatError;
use crate::kafka_interface::CustomConsumer;
use crate::kafka_interface::CustomMessage;
use crate::kafka_interface::CustomProducer;
use crate::kafka_interface::KafkaInterface;
use async_trait::async_trait;

pub struct RdKafka {
    config: Config,
}
#[async_trait]
impl KafkaInterface for RdKafka {
    type Consumer = Consumer;
    type Message = Message;
    type Producer = Producer;

    fn set_config(&mut self, config: Config) { self.config = config; }

    async fn get_producer(&mut self, consumer: KafkaConfig, topic: &str) -> Self::Producer { unimplemented!() }

    async fn get_consumer(&mut self, consumer: KafkaConfig, topic: &str) -> Self::Consumer { unimplemented!() }
}

pub struct Consumer {}

#[async_trait]
impl CustomConsumer for Consumer {
    type Message = Message;

    fn from_config(config: &Config, kafka_config: KafkaConfig, topic: &str, partition: Option<i32>) -> Self
    where
        Self: Sized,
    {
        unimplemented!()
    }

    async fn set_offset(&mut self, topic: String, partition: Option<i32>, offset: KafkaOffset) -> Result<(), KafcatError> { unimplemented!() }

    async fn get_one(&mut self) -> Self::Message { unimplemented!() }
}
pub struct Producer {}

#[async_trait]
impl CustomProducer for Producer {
    type Message = Message;

    fn from_config(config: &Config, kafka_config: KafkaConfig, topic: &str) -> Self
    where
        Self: Sized,
    {
        unimplemented!()
    }

    async fn write_one(&mut self, msg: Self::Message) -> Result<(), KafcatError> { unimplemented!() }
}

pub struct Message {}

impl CustomMessage for Message {
    fn get_key(&self) -> &[u8] { unimplemented!() }

    fn get_payload(&self) -> Vec<u8> { unimplemented!() }

    fn get_timestamp(&self) -> i64 { unimplemented!() }

    fn set_key(&self, key: Vec<u8>) { unimplemented!() }

    fn set_payload(&self, payload: Vec<u8>) { unimplemented!() }

    fn set_timestamp(&self, key: i64) { unimplemented!() }
}
