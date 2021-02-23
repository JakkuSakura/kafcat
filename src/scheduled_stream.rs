use crate::error::KafcatError;
use crate::interface::CustomConsumer;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use tokio::time::sleep;

type Result<T> = std::result::Result<T, KafcatError>;

pub struct ScheduledStream<T: CustomConsumer + 'static> {
    consumer: T,
    callback: Box<dyn FnMut(T::Message) -> Result<()>>,
    delay:    Duration,
}

impl<T: CustomConsumer + 'static> ScheduledStream<T> {
    pub fn new<F>(delay: Duration, consumer: T, callback: F) -> Self
    where
        F: FnMut(T::Message) -> Result<()> + 'static,
    {
        ScheduledStream {
            consumer,
            callback: Box::new(callback),
            delay,
        }
    }
}
impl<T: CustomConsumer> Unpin for ScheduledStream<T> {}

impl<T: CustomConsumer> Future for ScheduledStream<T> {
    type Output = std::result::Result<(), KafcatError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut get_one = Box::pin(this.consumer.recv());
        let delay = sleep(this.delay);
        let callback = &mut this.callback;
        loop {
            if delay.is_elapsed() {
                return Poll::Ready(Err(KafcatError::Timeout));
            }

            match get_one.as_mut().poll(cx) {
                Poll::Ready(Ok(x)) => {
                    return match (*callback.as_mut())(x) {
                        Err(err) => Poll::Ready(Err(err)),
                        Ok(()) => Poll::Pending,
                    };
                },
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(err));
                },
                Poll::Pending => {},
            }
        }
    }
}
