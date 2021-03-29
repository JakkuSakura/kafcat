use crate::configs::KafkaConsumerConfig;
use crate::configs::KafkaOffset;
use crate::configs::KafkaProducerConfig;
use crate::interface::KafkaConsumer;
use crate::interface::KafkaInterface;
use crate::interface::KafkaProducer;
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
use std::time::Duration;
use tokio::task::block_in_place;

pub struct RdKafka {}
impl KafkaInterface for RdKafka {
    type Consumer = RdkafkaConsumer;
    type Producer = RdkafkaProducer;
}

pub struct RdkafkaConsumer {
    stream: StreamConsumer,
    config: KafkaConsumerConfig,
}
impl RdkafkaConsumer {
    pub fn new(stream: StreamConsumer, config: KafkaConsumerConfig) -> Self {
        RdkafkaConsumer { stream, config }
    }
}

#[async_trait]
impl KafkaConsumer for RdkafkaConsumer {
    async fn from_config(config: KafkaConsumerConfig) -> Self
    where
        Self: Sized,
    {
        let stream: StreamConsumer = ClientConfig::new()
            .set("group.id", &config.group_id)
            .set("bootstrap.servers", &config.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Consumer creation failed");

        RdkafkaConsumer { stream, config }
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
                let consumer = &self.stream;
                let r: KafkaResult<_> = block_in_place(|| {
                    let mut tpl_b = TopicPartitionList::new();
                    tpl_b.add_partition_offset(&topic, partition, Offset::Offset(b as _))?;
                    tpl_b = consumer.offsets_for_times(tpl_b, Duration::from_secs(1))?;
                    Ok(tpl_b.find_partition(&topic, partition).unwrap().offset())
                });
                r?
            }
        };

        tpl.add_partition_offset(&self.config.topic, partition, offset)
            .unwrap();
        self.stream.assign(&tpl)?;
        Ok(())
    }

    async fn get_offset(&self) -> Result<i64> {
        unimplemented!()
    }

    async fn get_watermarks(&self) -> Result<(i64, i64)> {
        let stream = &self.stream;
        let config = self.config.clone();
        let watermarks = block_in_place(|| {
            stream.fetch_watermarks(
                &config.topic,
                config.partition.unwrap_or(0),
                Duration::from_secs(3),
            )
        })?;
        Ok(watermarks)
    }

    async fn recv(&self) -> Result<KafkaMessage> {
        let locker = &self.stream;

        match locker.recv().await {
            Ok(x) => {
                let msg = x.detach();
                Ok(KafkaMessage {
                    key: msg.key().map(Vec::from).unwrap_or_default(),
                    payload: msg.payload().map(Vec::from).unwrap_or_default(),
                    timestamp: msg.timestamp().to_millis().unwrap(),
                    ..KafkaMessage::default() // TODO headers
                })
            }
            Err(err) => Err(anyhow::Error::from(err).into()),
        }
    }
}
pub struct RdkafkaProducer {
    producer: FutureProducer,
    config: KafkaProducerConfig,
}

#[async_trait]
impl KafkaProducer for RdkafkaProducer {
    async fn from_config(kafka_config: KafkaProducerConfig) -> Self
    where
        Self: Sized,
    {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        RdkafkaProducer {
            producer,
            config: kafka_config,
        }
    }

    async fn write_one(&self, msg: KafkaMessage) -> Result<()> {
        let mut record = FutureRecord::to(&self.config.topic);
        let key = msg.key;
        if !key.is_empty() {
            record = record.key(&key);
        }
        let payload = msg.payload;
        if !payload.is_empty() {
            record = record.payload(&payload)
        }
        self.producer
            .send(record, Duration::from_secs(0))
            .await
            .map_err(|(err, _msg)| anyhow::Error::from(err))?;
        Ok(())
    }
}
