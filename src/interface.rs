use crate::configs::KafkaConsumerConfig;
use crate::configs::KafkaOffset;
use crate::configs::KafkaProducerConfig;
use crate::error::KafcatError;
use async_trait::async_trait;
use futures::TryFuture;

pub trait KafkaInterface {
    type Message: CustomMessage;
    type Consumer: CustomConsumer<Message = Self::Message>;
    type Producer: CustomProducer<Message = Self::Message>;
}

#[async_trait]
pub trait CustomConsumer: Send + Sync {
    type Message;
    fn from_config(kafka_config: KafkaConsumerConfig) -> Self
    where
        Self: Sized;

    async fn set_offset(&self, offset: KafkaOffset) -> Result<(), KafcatError>;

    async fn for_each<Fut, F>(&self, mut func: F) -> Result<(), KafcatError>
    where
        F: FnMut(Self::Message) -> Fut + Send,
        Fut: TryFuture<Ok = (), Error = KafcatError> + Send;
}

#[async_trait]
pub trait CustomProducer: Send + Sync {
    type Message;
    fn from_config(kafka_config: KafkaProducerConfig) -> Self
    where
        Self: Sized;
    async fn write_one(&self, msg: Self::Message) -> Result<(), KafcatError>;
}

pub trait CustomMessage: Send + Sync {
    fn get_key(&self) -> &[u8];
    fn get_payload(&self) -> &[u8];
    fn get_timestamp(&self) -> i64;
    fn set_key(&mut self, key: Vec<u8>);
    fn set_payload(&mut self, payload: Vec<u8>);
    fn set_timestamp(&mut self, timestamp: i64);
}
