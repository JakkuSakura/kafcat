use kafcat::configs::AppConfig;
use kafcat::error::KafcatError;
use kafcat::interface::KafkaInterface;

pub async fn run_async_produce_topic<Interface: KafkaInterface>(_interface: Interface, _config: AppConfig) -> Result<(), KafcatError> { unimplemented!() }
