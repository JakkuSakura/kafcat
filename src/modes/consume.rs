use crate::modes::get_delay;
use kafcat::configs::AppConfig;
use kafcat::configs::SerdeFormat;
use kafcat::error::KafcatError;
use kafcat::interface::KafkaConsumer;
use kafcat::interface::KafkaInterface;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::time::timeout_at;
use tokio::time::Instant;

pub async fn run_async_consume_topic<Interface: KafkaInterface>(
    _interface: Interface,
    config: AppConfig,
) -> Result<(), KafcatError> {
    let input_config = config
        .consumer_kafka
        .expect("Must specify input kafka config");
    let consumer: Interface::Consumer =
        Interface::Consumer::from_config(input_config.clone()).await;
    consumer
        .set_offset_and_subscribe(input_config.offset)
        .await?;

    let timeout = get_delay(input_config.exit_on_done);
    let mut stdout = BufWriter::new(tokio::io::stdout());
    let mut bytes_read = 0;
    let mut messages_count = 0;
    loop {
        match timeout_at(Instant::now() + timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                log::trace!("Received message:\n{:#?}", msg);
                match input_config.format {
                    SerdeFormat::Text => {
                        bytes_read += stdout.write(&msg.key).await?;
                        bytes_read += stdout.write(input_config.key_delim.as_bytes()).await?;
                        bytes_read += stdout.write(&msg.payload).await?;
                        bytes_read += stdout.write(input_config.msg_delim.as_bytes()).await?;
                    }
                    SerdeFormat::Json => {
                        let x = serde_json::to_string(&msg)?;
                        bytes_read += stdout.write(x.as_bytes()).await?;
                        bytes_read += stdout.write(input_config.msg_delim.as_bytes()).await?;
                    }
                    SerdeFormat::Regex(r) => {
                        unimplemented!("Does not support {} yet", r);
                    }
                };
                messages_count += 1;
                let should_flush = {
                    match (input_config.msg_count_flush, input_config.msg_bytes_flush) {
                        (None, None) => false,
                        (None, Some(bytes_treshold)) => bytes_read >= bytes_treshold,
                        (Some(count_treshold), None) => messages_count >= count_treshold,
                        (Some(count_treshold), Some(bytes_treshold)) => {
                            bytes_read >= bytes_treshold || messages_count >= count_treshold
                        }
                    }
                };
                if should_flush {
                    stdout.flush().await?;
                    bytes_read = 0;
                    messages_count = 0;
                }
            }
            Ok(Err(err)) => Err(err)?,
            Err(_err) => break,
        }
    }
    stdout.flush().await?;
    Ok(())
}
