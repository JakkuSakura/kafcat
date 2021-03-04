use crate::modes::get_delay;
use futures::TryStreamExt;
use kafcat::configs::AppConfig;
use kafcat::error::KafcatError;
use kafcat::interface::CustomConsumer;
use kafcat::interface::CustomMessage;
use kafcat::interface::CustomProducer;
use kafcat::interface::KafkaInterface;
use kafcat::scheduled_stream::scheduled_stream;

async fn process_message<Msg: CustomMessage>(msg: &Msg) -> Result<(), KafcatError> {
    println!("{:?}", msg.get_payload());
    Ok(())
}

pub async fn run_async_copy_topic<Interface: KafkaInterface>(_interface: Interface, config: AppConfig) -> Result<(), KafcatError> {
    let input_config = config.consumer_kafka.as_ref().expect("Must specify input kafka config");
    let consumer: Interface::Consumer = Interface::Consumer::from_config(input_config.clone());
    consumer.set_offset(input_config.offset).await?;

    let producer: Interface::Producer = Interface::Producer::from_config(config.producer_kafka.clone().expect("Must specify output kafka config"));
    let delay = get_delay(input_config.exit_on_done);
    scheduled_stream(delay, consumer)
        .try_for_each(|msg| async {
            process_message(&msg).await?;
            producer.write_one(msg).await?;
            Ok(())
        })
        .await?;

    Ok(())
}
