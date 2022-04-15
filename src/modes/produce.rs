use kafcat::configs::AppConfig;
use kafcat::configs::SerdeFormat;
use kafcat::error::KafcatError;
use kafcat::interface::KafkaInterface;
use kafcat::interface::KafkaProducer;
use kafcat::message::KafkaMessage;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::io::Lines;

enum Messages {
    File(Lines<BufReader<tokio::fs::File>>),
    Stdin(Lines<BufReader<tokio::io::Stdin>>),
}

impl Messages {
    async fn new(data_file: &Option<String>) -> Messages {
        match data_file {
            Some(data_file) => {
                info!("Reading from file: {}", data_file);
                let file = tokio::fs::File::open(data_file)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to load data file {}: {}", data_file, e));
                Messages::File(BufReader::new(file).lines())
            }
            None => {
                info!("Reading from stdin");
                Messages::Stdin(BufReader::new(tokio::io::stdin()).lines())
            }
        }
    }

    async fn next(&mut self) -> std::io::Result<Option<String>> {
        match self {
            Messages::File(lines) => lines.next_line().await,
            Messages::Stdin(lines) => lines.next_line().await,
        }
    }
}

pub async fn run_async_produce_topic<Interface: KafkaInterface>(
    _interface: Interface,
    config: AppConfig,
) -> Result<(), KafcatError> {
    let producer_config = config
        .producer_kafka
        .expect("Must specify output kafka config");
    let producer: Interface::Producer =
        Interface::Producer::from_config(producer_config.clone()).await;

    let mut messages = Messages::new(&producer_config.data_file).await;
    let key_delim = producer_config.key_delim;

    match producer_config.format {
        SerdeFormat::None => {
            while let Some(line) = messages.next().await? {
                let msg = KafkaMessage {
                    key: "".as_bytes().to_vec(),
                    payload: line.as_bytes().to_vec(),
                    timestamp: chrono::Utc::now().timestamp(),
                    headers: Default::default(),
                };
                producer.write_one(msg).await?;
            }
        }
        SerdeFormat::Text => {
            while let Some(line) = messages.next().await? {
                if let Some(index) = line.find(&key_delim) {
                    let key = &line[..index];
                    let payload = &line[index + key_delim.len()..];
                    let msg = KafkaMessage {
                        key: key.to_owned().into_bytes(),
                        payload: payload.to_owned().into_bytes(),
                        timestamp: chrono::Utc::now().timestamp(),
                        headers: Default::default(),
                    };
                    producer.write_one(msg).await?;
                } else {
                    error!("Did not find key delimiter in line {}", line);
                }
            }
        }
        SerdeFormat::Json => {
            while let Some(line) = messages.next().await? {
                match serde_json::from_str(&line) {
                    Ok(msg) => producer.write_one(msg).await?,
                    Err(err) => {
                        error!("Error parsing json: {} {}", err, line)
                    }
                }
            }
        }
        SerdeFormat::Regex(r) => {
            unimplemented!("Does not support {} yet", r)
        }
    };

    Ok(())
}
