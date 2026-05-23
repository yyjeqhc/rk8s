use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, channel::mpsc::Sender};
use tracing::debug;
pub use xlineapi::{
    LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse, LeaseRevokeResponse,
    LeaseStatus, LeaseTimeToLiveResponse,
};

use crate::error::{Result, XlineClientError};
use crate::transport::Streaming;

/// Lease keep-alive request sender.
///
/// Sends keep-alive requests for a specific lease. Typically paired with
/// [`LeaseStreaming`] to receive TTL responses. Dropping the `LeaseKeeper`
/// closes the request channel — the handler task will drain remaining
/// responses and then exit.
///
/// # Examples
///
/// ```ignore
/// let (mut keeper, mut stream) = client.keep_alive(lease_id).await?;
/// loop {
///     keeper.keep_alive()?;
///     match stream.message().await? {
///         Some(resp) => println!("ttl: {}", resp.ttl),
///         None => break, // stream closed
///     }
///     tokio::time::sleep(Duration::from_secs(5)).await;
/// }
/// ```
#[derive(Debug)]
pub struct LeaseKeeper {
    /// lease id
    id: i64,
    /// sender to send keep alive request
    sender: Sender<xlineapi::LeaseKeepAliveRequest>,
}

impl LeaseKeeper {
    /// Creates a new `LeaseKeeper`.
    #[inline]
    #[must_use]
    pub fn new(id: i64, sender: Sender<xlineapi::LeaseKeepAliveRequest>) -> Self {
        Self { id, sender }
    }

    /// The lease id which user want to keep alive.
    #[inline]
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Sends a keep alive request and receive response
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner channel is closed
    #[inline]
    pub fn keep_alive(&mut self) -> Result<()> {
        self.sender
            .try_send(xlineapi::LeaseKeepAliveRequest { id: self.id })
            .map_err(|e| XlineClientError::LeaseError(e.to_string()))
    }

    /// Closes the request channel, causing the handler task to exit and the
    /// QUIC stream to close. This also affects [`LeaseStreaming`] since both
    /// share the same channel. After calling `close()`, subsequent
    /// `keep_alive()` calls will fail.
    ///
    /// This is equivalent to dropping both `LeaseKeeper` and `LeaseStreaming`.
    /// Call this when you want to explicitly signal cleanup without relying
    /// on Drop.
    #[inline]
    pub fn close(&mut self) {
        self.sender.close_channel();
        debug!(lease_id = self.id, "LeaseKeeper channel closed");
    }

    /// Returns whether the request channel is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        debug!(lease_id = self.id, "LeaseKeeper dropped");
    }
}

/// Lease keep-alive response stream.
///
/// Receives [`LeaseKeepAliveResponse`] messages from the server. Holds a clone
/// of the request sender as a **lifecycle pin**: even if the [`LeaseKeeper`] is
/// dropped first, the request channel stays open and the handler task continues
/// to receive responses. Dropping `LeaseStreaming` releases both sides, allowing
/// the handler task and QUIC stream to close.
///
/// Use [`message()`](Self::message) to receive the next response, or the
/// [`Stream`] impl for async iteration.
///
/// # Drop behavior
///
/// Dropping `LeaseStreaming` (after or instead of `LeaseKeeper`) closes the
/// request channel and allows the handler task to exit. The inner [`Streaming`]
/// emits a `debug`-level trace log on drop.
#[derive(Debug)]
pub struct LeaseStreaming {
    inner: Streaming<LeaseKeepAliveResponse>,
    _sender: Sender<xlineapi::LeaseKeepAliveRequest>,
}

impl LeaseStreaming {
    /// Creates a new `LeaseStreaming`.
    #[inline]
    #[must_use]
    pub(crate) fn new(
        inner: Streaming<LeaseKeepAliveResponse>,
        sender: Sender<xlineapi::LeaseKeepAliveRequest>,
    ) -> Self {
        Self {
            inner,
            _sender: sender,
        }
    }

    /// Receive the next keep-alive response from the stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream encounters a transport error.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<LeaseKeepAliveResponse>> {
        self.inner.message().await.map_err(Into::into)
    }

    /// Closes the request channel, causing the handler task to exit and the
    /// QUIC stream to close. This also affects [`LeaseKeeper`] since both
    /// share the same channel. After calling `close()`, subsequent
    /// `message()` calls will return `None`.
    ///
    /// This is equivalent to dropping both `LeaseStreaming` and `LeaseKeeper`.
    /// Call this when you want to explicitly signal cleanup without relying
    /// on Drop.
    #[inline]
    pub fn close(&mut self) {
        self._sender.close_channel();
        debug!("LeaseStreaming channel closed");
    }

    /// Returns whether the request channel is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self._sender.is_closed()
    }
}

impl Stream for LeaseStreaming {
    type Item = Result<LeaseKeepAliveResponse>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.get_mut().inner).poll_next(cx) {
            Poll::Ready(Some(Ok(resp))) => Poll::Ready(Some(Ok(resp))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
