use kafcat::configs::AppConfig;
use kafcat::configs::SerdeFormat;
use kafcat::error::KafcatError;
use kafcat::interface::CustomMessage;
use kafcat::interface::CustomProducer;
use kafcat::interface::KafkaInterface;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;

pub async fn run_async_produce_topic<Interface: KafkaInterface>(_interface: Interface, config: AppConfig) -> Result<(), KafcatError> {
    let producer_config = config.producer_kafka.expect("Must specify output kafka config");
    let producer: Interface::Producer = Interface::Producer::from_config(producer_config.clone());
    let reader = BufReader::new(tokio::io::stdin());
    let mut lines = reader.lines();
    let key_delim = producer_config.key_delim;

    match producer_config.format {
        SerdeFormat::Text => {
            while let Some(line) = lines.next_line().await? {
                if let Some(index) = line.find(&key_delim) {
                    let key = &line[..index];
                    let payload = &line[index + key_delim.len()..];
                    let mut msg = Interface::Message::new();
                    msg.set_key(key.to_owned().into_bytes());
                    msg.set_payload(payload.to_owned().into_bytes());
                    msg.set_timestamp(chrono::Utc::now().timestamp());
                    producer.write_one(msg).await?;
                } else {
                    error!("Did not find key delimiter in line {}", line);
                }
            }
        },
        SerdeFormat::Json => {
            while let Some(line) = lines.next_line().await? {
                match serde_json::from_str(&line) {
                    Ok(msg) => producer.write_one(msg).await?,
                    Err(err) => {
                        error!("Error parsing json: {} {}", err, line)
                    },
                }
            }
        },
        SerdeFormat::Regex(r) => {
            unimplemented!("Does not support {} yet", r)
        },
    };

    Ok(())
}
