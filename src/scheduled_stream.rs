use crate::error::KafcatError;
use crate::interface::CustomConsumer;
use crate::Result;
use async_stream::stream;
use futures::Stream;
use tokio::time::{timeout_at, Duration, Instant};

pub fn scheduled_stream<T: CustomConsumer>(timeout: Duration, consumer: T) -> impl Stream<Item = Result<T::Message>> {
    stream! {
       loop {
            yield match timeout_at(Instant::now() + timeout, consumer.recv()).await {
               Ok(Ok(x)) => Ok(x),
               Ok(Err(err)) => Err(err),
               Err(_err) => Err(KafcatError::Timeout),
           }
       }
    }
}
