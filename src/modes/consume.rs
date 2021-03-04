use crate::modes::get_delay;
use kafcat::configs::AppConfig;
use kafcat::configs::SerdeFormat;
use kafcat::error::KafcatError;
use kafcat::interface::CustomConsumer;
use kafcat::interface::CustomMessage;
use kafcat::interface::KafkaInterface;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::time::timeout_at;
use tokio::time::Instant;

pub async fn run_async_consume_topic<Interface: KafkaInterface>(_interface: Interface, config: AppConfig) -> Result<(), KafcatError> {
    let input_config = config.consumer_kafka.expect("Must specify input kafka config");
    let consumer: Interface::Consumer = Interface::Consumer::from_config(input_config.clone());
    consumer.set_offset(input_config.offset).await?;
    let timeout = get_delay(input_config.exit_on_done);

    let mut stdout = BufWriter::new(tokio::io::stdout());
    loop {
        match timeout_at(Instant::now() + timeout, consumer.recv()).await {
            Ok(Ok(msg)) => match input_config.format {
                SerdeFormat::Text => {
                    println!("Got one");
                    stdout.write(msg.get_key()).await?;
                    stdout.write(input_config.key_delim.as_bytes()).await?;
                    stdout.write(msg.get_payload()).await?;
                    stdout.write(input_config.msg_delim.as_bytes()).await?;
                },
                SerdeFormat::Json => {
                    let x = serde_json::to_string(&msg)?;
                    stdout.write(x.as_bytes()).await?;
                    stdout.write(input_config.msg_delim.as_bytes()).await?;
                },
                SerdeFormat::Regex(r) => {
                    unimplemented!("Does not supports {} yet", r);
                },
            },
            Ok(Err(err)) => Err(err)?,
            Err(_err) => break,
        }
    }
    stdout.flush().await?;
    Ok(())
}
