use crate::configs::KafkaConsumerConfig;
use crate::configs::KafkaOffset;
use crate::configs::KafkaProducerConfig;
use crate::message::KafkaMessage;
use crate::Result;
use async_trait::async_trait;

pub trait KafkaInterface {
    type Consumer: KafkaConsumer + 'static;
    type Producer: KafkaProducer + 'static;
}

#[async_trait]
pub trait KafkaConsumer: Send + Sync {
    async fn from_config(config: KafkaConsumerConfig) -> Self
    where
        Self: Sized;

    async fn set_offset_and_subscribe(&self, offset: KafkaOffset) -> Result<()>;
    async fn get_offset(&self) -> Result<i64>;
    async fn get_watermarks(&self) -> Result<(i64, i64)>;
    async fn recv(&self) -> Result<KafkaMessage>;
}

#[async_trait]
pub trait KafkaProducer: Send + Sync {
    async fn from_config(config: KafkaProducerConfig) -> Self
    where
        Self: Sized;
    async fn write_one(&self, msg: KafkaMessage) -> Result<()>;
}
