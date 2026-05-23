//! Shared streaming response wrapper.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use tracing::debug;

use crate::Status;

/// Generic RPC response stream wrapper.
///
/// Wraps a pinned stream of `Result<T, Status>` messages. Use [`message()`](Self::message)
/// to receive the next response, or use the [`Stream`] impl for async iteration.
///
/// # Drop behavior
///
/// Dropping a `Streaming` emits a `debug`-level trace log. It does **not** guarantee
/// that the remote peer is immediately notified — the underlying QUIC stream may
/// linger until the connection driver processes the reset.
///
/// # Examples
///
/// ```ignore
/// while let Some(resp) = stream.message().await? {
///     println!("received: {:?}", resp);
/// }
/// // stream ended (EOF)
/// ```
pub struct Streaming<T> {
    inner: Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>,
    label: &'static str,
}

impl<T> std::fmt::Debug for Streaming<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streaming")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl<T> Streaming<T> {
    /// Create a new stream wrapper.
    #[must_use]
    #[inline]
    pub fn new(
        inner: Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>,
        label: &'static str,
    ) -> Self {
        Self { inner, label }
    }

    /// Receive the next message from the stream.
    pub async fn message(&mut self) -> Result<Option<T>, Status> {
        self.inner.next().await.transpose()
    }
}

impl<T> Drop for Streaming<T> {
    fn drop(&mut self) {
        debug!(label = self.label, "Streaming dropped");
    }
}

impl<T> Stream for Streaming<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}
