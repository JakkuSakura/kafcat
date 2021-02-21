use crate::configs::AppConfig;
use crate::configs::KafkaConfig;
use crate::configs::KafkaOffset;
use crate::error::KafcatError;
use async_trait::async_trait;
use futures::Stream;
use futures::TryFuture;
use rdkafka::error::KafkaError;
use std::sync::Arc;

#[async_trait]
pub trait KafkaInterface {
    type Message: CustomMessage;
    type Consumer: CustomConsumer<Message = Self::Message>;
    type Producer: CustomProducer<Message = Self::Message>;
    fn from_config(config: AppConfig) -> Self
    where
        Self: Sized;
    fn get_config(&self) -> Arc<AppConfig>;
}

#[async_trait]
pub trait CustomConsumer {
    type Message;
    fn from_config(config: Arc<AppConfig>, kafka_config: KafkaConfig, topic: &str, partition: Option<i32>) -> Self
    where
        Self: Sized;

    async fn set_offset(self: Arc<Self>, topic: &str, partition: Option<i32>, offset: KafkaOffset) -> Result<Arc<Self>, KafcatError>;

    async fn for_each<Fut, F>(self: Arc<Self>, mut func: F) -> Result<(), KafcatError>
    where
        F: FnMut(Self::Message) -> Fut + Send,
        Fut: TryFuture<Ok = (), Error = KafcatError> + Send;
}

#[async_trait]
pub trait CustomProducer {
    type Message;
    fn from_config(config: Arc<AppConfig>, kafka_config: KafkaConfig, topic: &str) -> Self
    where
        Self: Sized;
    async fn write_one(self: Arc<Self>, msg: Self::Message) -> Result<(), KafcatError>;
}

pub trait CustomMessage: Send {
    fn get_key(&self) -> &[u8];
    fn get_payload(&self) -> &[u8];
    fn get_timestamp(&self) -> i64;
    fn set_key(&mut self, key: Vec<u8>);
    fn set_payload(&mut self, payload: Vec<u8>);
    fn set_timestamp(&mut self, timestamp: i64);
}
