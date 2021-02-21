use futures::Stream;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::Sleep;

pin_project! {
    /// Future returned by [`timeout`](timeout) and [`timeout_at`](timeout_at).
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    pub struct TimeoutStream<T> {
        #[pin]
        value: T,
        #[pin]
        delay: Sleep,
    }
}
pub trait TimeoutStreamExt {
    fn timeout(self, delay: Duration) -> TimeoutStream<Self>
    where
        Self: Sized;
}
impl<T: Stream> TimeoutStreamExt for T {
    fn timeout(self, delay: Duration) -> TimeoutStream<Self> { TimeoutStream::new_with_delay(self, delay) }
}

impl<T> TimeoutStream<T> {
    pub(crate) fn new_with_delay(value: T, delay: Duration) -> TimeoutStream<T> { TimeoutStream { value, delay: sleep(delay) } }

    /// Gets a reference to the underlying value in this timeout.
    pub fn get_ref(&self) -> &T { &self.value }

    /// Gets a mutable reference to the underlying value in this timeout.
    pub fn get_mut(&mut self) -> &mut T { &mut self.value }

    /// Consumes this timeout, returning the underlying value.
    pub fn into_inner(self) -> T { self.value }
}

/// Error returned by `TimeoutStream`.
#[derive(Debug, PartialEq)]
pub struct Elapsed;

impl<T> Stream for TimeoutStream<T>
where
    T: Stream,
{
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();

        // First, try polling the future
        if let Poll::Ready(v) = me.value.poll_next(cx) {
            return Poll::Ready(v);
        }

        // Now check the timer
        match me.delay.poll(cx) {
            Poll::Ready(()) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
