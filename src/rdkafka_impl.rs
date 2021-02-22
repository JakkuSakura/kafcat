use crate::configs::AppConfig;
use crate::configs::KafkaConfig;
use crate::configs::KafkaOffset;
use crate::error::KafcatError;
use crate::kafka_interface::CustomConsumer;
use crate::kafka_interface::CustomMessage;
use crate::kafka_interface::CustomProducer;
use crate::kafka_interface::KafkaInterface;
use crate::timeout_stream::TimeoutStreamExt;
use futures::TryFuture;
use futures::TryStreamExt;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::task::JoinHandle;

pub struct RdKafka {
    config: Arc<AppConfig>,
}
#[async_trait]
impl KafkaInterface for RdKafka {
    type Consumer = RdkafkaConsumer;
    type Message = RdkafkaMessage;
    type Producer = RdkafkaProducer;

    fn from_config(config: AppConfig) -> Self
    where
        Self: Sized,
    {
        Self { config: Arc::new(config) }
    }

    fn get_config(&self) -> Arc<AppConfig> { Arc::clone(&self.config) }
}

pub struct RdkafkaConsumer {
    config: Arc<AppConfig>,
    stream: Arc<Mutex<StreamConsumer>>,
}

#[async_trait]
impl CustomConsumer for RdkafkaConsumer {
    type Message = RdkafkaMessage;

    fn from_config(config: Arc<AppConfig>, kafka_config: KafkaConfig, _topic: &str, _partition: Option<i32>) -> Self
    where
        Self: Sized,
    {
        let stream: StreamConsumer = ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Consumer creation failed");

        RdkafkaConsumer {
            config,
            stream: Arc::new(Mutex::new(stream)),
        }
    }

    async fn set_offset(&self, topic: &str, partition: Option<i32>, offset: KafkaOffset) -> Result<(), KafcatError> {
        info!("offset {:?}", offset);
        let mut tpl = TopicPartitionList::new();
        let partition = partition.unwrap_or(0);
        let topic_ = topic.to_owned();
        let offset = match offset {
            KafkaOffset::Beginning => Offset::Beginning,
            KafkaOffset::End => Offset::End,
            KafkaOffset::Stored => Offset::Stored,
            KafkaOffset::Offset(o) if o >= 0 => Offset::Offset(o as _),
            KafkaOffset::Offset(o) => Offset::OffsetTail((-o - 1) as _),
            KafkaOffset::OffsetInterval(b, _) => Offset::Offset(b as _),
            KafkaOffset::TimeInterval(b, _e) => {
                let consumer = Arc::clone(&self.stream);
                async move {
                    let consumer = consumer.lock_owned().await;
                    let r: JoinHandle<KafkaResult<_>> = spawn_blocking(move || {
                        let mut tpl_b = TopicPartitionList::new();
                        tpl_b.add_partition_offset(&topic_, partition, Offset::Offset(b as _))?;
                        tpl_b = consumer.offsets_for_times(tpl_b, Duration::from_secs(1))?;
                        Ok(tpl_b.find_partition(&topic_, partition).unwrap().offset())
                    });
                    r.await
                }
                .await
                .expect("")
                .map_err(|x| anyhow::Error::from(x))?
            },
        };

        tpl.add_partition_offset(&topic, partition, offset).unwrap();
        self.stream.lock().await.assign(&tpl).map_err(|x| anyhow::Error::from(x))?;
        Ok(())
    }

    async fn for_each<Fut, F>(&self, mut func: F) -> Result<(), KafcatError>
    where
        F: FnMut(Self::Message) -> Fut + Send,
        Fut: TryFuture<Ok = (), Error = KafcatError> + Send,
    {
        let config = Arc::clone(&self.config);
        let stream = Arc::clone(&self.stream).lock_owned().await;
        let handler = stream.stream().timeout(if config.exit { Duration::from_secs(3) } else { Duration::from_secs(3600) });
        let handler = handler.map_err(|x| anyhow::Error::from(x).into()).try_for_each(|x| {
            let msg = x.detach();
            let msg = RdkafkaMessage {
                key:       msg.key().map(Vec::from).unwrap_or(vec![]),
                payload:   msg.payload().map(Vec::from).unwrap_or(vec![]),
                timestamp: msg.timestamp().to_millis().unwrap(),
            };
            (func)(msg)
        });
        handler.await
    }
}

pub struct RdkafkaProducer {
    _config:  Arc<AppConfig>,
    producer: FutureProducer,
    topic:    String,
}

#[async_trait]
impl CustomProducer for RdkafkaProducer {
    type Message = RdkafkaMessage;

    fn from_config(config: Arc<AppConfig>, kafka_config: KafkaConfig, topic: &str) -> Self
    where
        Self: Sized,
    {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        RdkafkaProducer {
            _config: config,
            producer,
            topic: topic.to_owned(),
        }
    }

    async fn write_one(&self, msg: Self::Message) -> Result<(), KafcatError> {
        let mut record = FutureRecord::to(&self.topic);
        let key = msg.get_key();
        if key.len() > 0 {
            record = record.key(key);
        }
        let payload = msg.get_payload();
        if payload.len() > 0 {
            record = record.payload(payload)
        }
        self.producer.send(record, Duration::from_secs(0)).await.map_err(|(err, _msg)| anyhow::Error::from(err))?;
        Ok(())
    }
}

pub struct RdkafkaMessage {
    key:       Vec<u8>,
    payload:   Vec<u8>,
    timestamp: i64,
}

impl CustomMessage for RdkafkaMessage {
    fn get_key(&self) -> &[u8] { &self.key }

    fn get_payload(&self) -> &[u8] { &self.payload }

    fn get_timestamp(&self) -> i64 { self.timestamp }

    fn set_key(&mut self, key: Vec<u8>) { self.key = key; }

    fn set_payload(&mut self, payload: Vec<u8>) { self.payload = payload; }

    fn set_timestamp(&mut self, timestamp: i64) { self.timestamp = timestamp; }
}