use crate::modes::get_delay;
use kafcat::configs::AppConfig;
use kafcat::error::KafcatError;
use kafcat::interface::KafkaConsumer;
use kafcat::interface::KafkaInterface;
use kafcat::interface::KafkaProducer;
use tokio::time::timeout_at;
use tokio::time::Instant;

pub async fn run_async_copy_topic<Interface: KafkaInterface>(
    _interface: Interface,
    config: AppConfig,
) -> Result<(), KafcatError> {
    let input_config = config
        .consumer_kafka
        .as_ref()
        .expect("Must specify input kafka config");
    let consumer: Interface::Consumer =
        Interface::Consumer::from_config(input_config.clone()).await;
    consumer
        .set_offset_and_subscribe(input_config.offset)
        .await?;

    let producer: Interface::Producer = Interface::Producer::from_config(
        config
            .producer_kafka
            .clone()
            .expect("Must specify output kafka config"),
    )
    .await;
    let timeout = get_delay(input_config.exit_on_done);
    loop {
        match timeout_at(Instant::now() + timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                producer.write_one(msg).await?;
            }
            Ok(Err(err)) => return Err(err),
            Err(_) => break,
        }
    }
    Ok(())
}
