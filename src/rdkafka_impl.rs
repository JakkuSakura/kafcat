use crate::configs::KafkaConsumerConfig;
use crate::configs::KafkaOffset;
use crate::configs::KafkaProducerConfig;
use crate::interface::CustomConsumer;
use crate::interface::CustomProducer;
use crate::interface::KafkaInterface;
use crate::message::KafkaMessage;
use crate::Result;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::task::JoinHandle;

pub struct RdKafka {}
impl KafkaInterface for RdKafka {
    type Consumer = RdkafkaConsumer;
    type Producer = RdkafkaProducer;
}

pub struct RdkafkaConsumer {
    stream: Arc<Mutex<StreamConsumer>>,
    config: KafkaConsumerConfig,
}
impl RdkafkaConsumer {
    pub fn new(stream: StreamConsumer, config: KafkaConsumerConfig) -> Self {
        RdkafkaConsumer {
            stream: Arc::new(Mutex::new(stream)),
            config,
        }
    }
}

#[async_trait]
impl CustomConsumer for RdkafkaConsumer {
    async fn from_config(kafka_config: KafkaConsumerConfig) -> Self
    where
        Self: Sized,
    {
        let stream: StreamConsumer = ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Consumer creation failed");

        RdkafkaConsumer {
            stream: Arc::new(Mutex::new(stream)),
            config: kafka_config,
        }
    }

    async fn set_offset_and_subscribe(&self, offset: KafkaOffset) -> Result<()> {
        info!("set offset {:?}", offset);
        let mut tpl = TopicPartitionList::new();
        let partition = self.config.partition.unwrap_or(0);
        let topic = self.config.topic.clone();
        let offset = match offset {
            KafkaOffset::Beginning => Offset::Beginning,
            KafkaOffset::End => Offset::End,
            KafkaOffset::Stored => Offset::Stored,
            KafkaOffset::Offset(o) if o >= 0 => Offset::Offset(o as _),
            KafkaOffset::Offset(o) => Offset::OffsetTail((-o - 1) as _),
            KafkaOffset::OffsetInterval(b, _) => Offset::Offset(b as _),
            KafkaOffset::TimeInterval(b, _e) => {
                let consumer = Arc::clone(&self.stream);
                let consumer = consumer.lock_owned().await;
                let r: JoinHandle<KafkaResult<_>> = spawn_blocking(move || {
                    let mut tpl_b = TopicPartitionList::new();
                    tpl_b.add_partition_offset(&topic, partition, Offset::Offset(b as _))?;
                    tpl_b = consumer.offsets_for_times(tpl_b, Duration::from_secs(1))?;
                    Ok(tpl_b.find_partition(&topic, partition).unwrap().offset())
                });
                r.await.unwrap().map_err(|x| anyhow::Error::from(x))?
            },
        };

        tpl.add_partition_offset(&self.config.topic, partition, offset).unwrap();
        let lock = self.stream.lock();
        lock.await.assign(&tpl)?;
        Ok(())
    }

    async fn get_offset(&self) -> Result<i64> { unimplemented!() }

    async fn get_watermarks(&self) -> Result<(i64, i64)> {
        let stream = Arc::clone(&self.stream).lock_owned().await;
        let config = self.config.clone();
        let watermarks = spawn_blocking(move || {
            stream
                .fetch_watermarks(&config.topic, config.partition.unwrap_or(0), Duration::from_secs(3))
                .map_err(|x| anyhow::Error::new(x))
        })
        .await
        .unwrap()?;
        Ok(watermarks)
    }

    async fn recv(&self) -> Result<KafkaMessage> {
        let locker = Arc::clone(&self.stream).lock_owned().await;

        match locker.recv().await {
            Ok(x) => {
                let msg = x.detach();
                Ok(KafkaMessage {
                    key: msg.key().map(Vec::from).unwrap_or(vec![]),
                    payload: msg.payload().map(Vec::from).unwrap_or(vec![]),
                    timestamp: msg.timestamp().to_millis().unwrap(),
                    ..KafkaMessage::default() // TODO headers
                })
            },
            Err(err) => Err(anyhow::Error::from(err).into()),
        }
    }
}
pub struct RdkafkaProducer {
    producer: FutureProducer,
    config:   KafkaProducerConfig,
}

#[async_trait]
impl CustomProducer for RdkafkaProducer {
    async fn from_config(kafka_config: KafkaProducerConfig) -> Self
    where
        Self: Sized,
    {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        RdkafkaProducer { producer, config: kafka_config }
    }

    async fn write_one(&self, msg: KafkaMessage) -> Result<()> {
        let mut record = FutureRecord::to(&self.config.topic);
        let key = msg.key;
        if key.len() > 0 {
            record = record.key(&key);
        }
        let payload = msg.payload;
        if payload.len() > 0 {
            record = record.payload(&payload)
        }
        self.producer.send(record, Duration::from_secs(0)).await.map_err(|(err, _msg)| anyhow::Error::from(err))?;
        Ok(())
    }
}
