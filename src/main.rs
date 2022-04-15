#![deny(unsafe_code)]

#[macro_use]
extern crate log;
pub mod modes;
use modes::*;

use kafcat::configs::{AppConfig, WorkingMode};
use kafcat::error::KafcatError;
use kafcat::rdkafka_impl::RdKafka;
use kafcat::setup_logger;

#[tokio::main]
async fn main() -> Result<(), KafcatError> {
    let args = std::env::args().collect::<Vec<String>>();
    let config = AppConfig::from_args(args.iter().map(|x| x.as_str()).collect());
    setup_logger(true, config.log_level);

    info!("Starting {:?}", config.working_mode);

    let interface = RdKafka {};
    match config.working_mode {
        WorkingMode::Consumer => run_async_consume_topic(interface, config).await?,
        WorkingMode::Producer => run_async_produce_topic(interface, config).await?,
        WorkingMode::Metadata => {}
        WorkingMode::Query => {}
        WorkingMode::Copy => run_async_copy_topic(interface, config).await?,
        WorkingMode::Execute => {
            config.executor_config.unwrap().run().await;
        }
        _ => {}
    }
    Ok(())
}
