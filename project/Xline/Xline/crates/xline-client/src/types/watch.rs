use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use super::range_end::RangeOption;
use crate::error::{Result, XlineClientError};
use crate::transport::Streaming;
use futures::{Stream, channel::mpsc::Sender};
use tracing::debug;
pub use xlineapi::{Event, EventType, KeyValue, WatchResponse};
use xlineapi::{RequestUnion, WatchCancelRequest, WatchProgressRequest};

/// Watch request sender.
///
/// Sends watch create, cancel, and progress requests to the server.
/// Typically paired with [`WatchStreaming`] to receive events. Dropping
/// the `Watcher` closes the request channel — but if `WatchStreaming`
/// is still alive, the handler task stays open (the `_sender` lifecycle
/// pin in `WatchStreaming` prevents premature teardown).
///
/// # Examples
///
/// ```ignore
/// let (mut watcher, mut stream) = client.watch("key", None).await?;
/// // ... receive events from stream ...
/// watcher.cancel()?;
/// ```
#[derive(Debug)]
pub struct Watcher {
    /// Id of the watcher
    watch_id: i64,
    /// The channel sender
    sender: Sender<xlineapi::WatchRequest>,
}

impl Watcher {
    /// Creates a new `Watcher`.
    #[inline]
    #[must_use]
    pub fn new(watch_id: i64, sender: Sender<xlineapi::WatchRequest>) -> Self {
        Self { watch_id, sender }
    }

    /// The ID of the watcher.
    #[inline]
    #[must_use]
    pub const fn watch_id(&self) -> i64 {
        self.watch_id
    }

    /// Watches for events happening or that have happened.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn watch(&mut self, request: WatchOptions) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(request.into())),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }

    /// Cancels this watcher.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn cancel(&mut self) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(WatchCancelRequest {
                watch_id: self.watch_id,
            })),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }

    /// Cancels watch by specified `watch_id`.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn cancel_by_id(&mut self, watch_id: i64) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(WatchCancelRequest { watch_id })),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }

    /// Requests a watch stream progress status be sent in the watch response stream as soon as
    /// possible.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn request_progress(&mut self) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::ProgressRequest(WatchProgressRequest {})),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }

    /// Closes the request channel, causing the handler task to exit and the
    /// QUIC stream to close. This also affects [`WatchStreaming`] since both
    /// share the same channel. After calling `close()`, subsequent `watch()`,
    /// `cancel()`, and `request_progress()` calls will fail.
    ///
    /// This is equivalent to dropping both `Watcher` and `WatchStreaming`.
    /// Call this when you want to explicitly signal cleanup without relying
    /// on Drop.
    #[inline]
    pub fn close(&mut self) {
        self.sender.close_channel();
        debug!(watch_id = self.watch_id, "Watcher channel closed");
    }

    /// Returns whether the request channel is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        debug!(watch_id = self.watch_id, "Watcher dropped");
    }
}

/// Watch Request
#[derive(Clone, Debug, PartialEq, Default)]
pub struct WatchOptions {
    /// Inner watch create request
    inner: xlineapi::WatchCreateRequest,
    /// Watch range end options
    range_end_options: RangeOption,
}

impl WatchOptions {
    /// `key` is the key to register for watching.
    #[inline]
    #[must_use]
    pub fn with_key<K: Into<Vec<u8>>>(mut self, key: K) -> Self {
        self.inner.key = key.into();
        self
    }

    /// If set, Xline will watch all keys with the matching prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        self.range_end_options = RangeOption::Prefix;
        self
    }

    /// If set, Xline will watch all keys that are equal to or greater than the given key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        self.range_end_options = RangeOption::FromKey;
        self
    }

    /// `range_end` is the end of the range [key, `range_end`) to watch. If `range_end` is not given,
    /// only the key argument is watched. If `range_end` is equal to '\0', all keys greater than
    /// or equal to the key argument are watched.
    /// If the `range_end` is one bit larger than the given key,
    /// then all keys with the prefix (the given key) will be watched.
    #[inline]
    #[must_use]
    pub fn with_range_end<R: Into<Vec<u8>>>(mut self, range_end: R) -> Self {
        self.range_end_options = RangeOption::RangeEnd(range_end.into());
        self
    }

    /// Sets the start revision to watch from (inclusive). No `start_revision` is "now".
    #[inline]
    #[must_use]
    pub const fn with_start_revision(mut self, revision: i64) -> Self {
        self.inner.start_revision = revision;
        self
    }

    /// `progress_notify` is set so that the Xline server will periodically send a `WatchResponse` with no events to the new watcher if there are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent known revision. The xline server may decide how often it will send notifications based on current load.
    #[inline]
    #[must_use]
    pub const fn with_progress_notify(mut self) -> Self {
        self.inner.progress_notify = true;
        self
    }

    /// `filters` filter the events on server side before it sends back to the watcher.
    #[inline]
    #[must_use]
    pub fn with_filters<F: Into<Vec<WatchFilterType>>>(mut self, filters: F) -> Self {
        self.inner.filters = filters.into().into_iter().map(Into::into).collect();
        self
    }

    /// If `prev_kv` is set, created watcher gets the previous KV before the event happens.
    /// If the previous KV is already compacted, nothing will be returned.
    #[inline]
    #[must_use]
    pub const fn with_prev_kv(mut self) -> Self {
        self.inner.prev_kv = true;
        self
    }

    /// If `watch_id` is provided and non-zero, it will be assigned to this watcher.
    /// this can be used ensure that ordering is correct when creating multiple
    /// watchers on the same stream. Creating a watcher with an ID already in
    /// use on the stream will cause an error to be returned.
    #[inline]
    #[must_use]
    pub const fn with_watch_id(mut self, watch_id: i64) -> Self {
        self.inner.watch_id = watch_id;
        self
    }

    /// fragment enables splitting large revisions into multiple watch responses.
    #[inline]
    #[must_use]
    pub const fn with_fragment(mut self) -> Self {
        self.inner.fragment = true;
        self
    }
}

impl From<WatchOptions> for xlineapi::WatchCreateRequest {
    #[inline]
    fn from(mut request: WatchOptions) -> Self {
        request.inner.range_end = request
            .range_end_options
            .get_range_end(&mut request.inner.key);
        request.inner
    }
}

/// Watch filter type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum WatchFilterType {
    /// Filter out put event.
    NoPut = 0,
    /// Filter out delete event.
    NoDelete = 1,
}

impl From<WatchFilterType> for i32 {
    #[inline]
    fn from(value: WatchFilterType) -> Self {
        match value {
            WatchFilterType::NoPut => 0,
            WatchFilterType::NoDelete => 1,
        }
    }
}

/// Watch event stream.
///
/// Receives [`WatchResponse`] events from the server. Holds a clone of the
/// request sender as a **lifecycle pin**: even if the [`Watcher`] is dropped
/// first, the request channel stays open and the handler task continues to
/// deliver events. Dropping `WatchStreaming` releases both sides, allowing
/// the handler task and QUIC stream to close.
///
/// Use [`message()`](Self::message) to receive the next event, or the
/// [`Stream`] impl for async iteration.
///
/// # Drop behavior
///
/// Dropping `WatchStreaming` (after or instead of `Watcher`) closes the
/// request channel and allows the handler task to exit. The inner [`Streaming`]
/// emits a `debug`-level trace log on drop.
#[derive(Debug)]
pub struct WatchStreaming {
    /// Inner QUIC stream
    inner: Streaming<WatchResponse>,
    /// Lifecycle pin: keeps the request channel open so the handler task stays
    /// alive even if the `Watcher` is dropped first. Without this, dropping
    /// `Watcher` would close the request channel, causing the handler task to
    /// exit and the QUIC stream to close — even if the user still wants to
    /// receive events from `WatchStreaming`.
    _sender: Sender<xlineapi::WatchRequest>,
}

impl WatchStreaming {
    /// Create a new watch streaming
    #[inline]
    #[must_use]
    pub(crate) fn new(
        inner: Streaming<WatchResponse>,
        sender: Sender<xlineapi::WatchRequest>,
    ) -> Self {
        Self {
            inner,
            _sender: sender,
        }
    }

    /// Receive the next watch response from the stream.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<WatchResponse>> {
        self.inner.message().await.map_err(Into::into)
    }

    /// Closes the request channel, causing the handler task to exit and the
    /// QUIC stream to close. This also affects [`Watcher`] since both share
    /// the same channel. After calling `close()`, subsequent `message()` calls
    /// will return `None`.
    ///
    /// This is equivalent to dropping both `WatchStreaming` and `Watcher`.
    /// Call this when you want to explicitly signal cleanup without relying
    /// on Drop.
    #[inline]
    pub fn close(&mut self) {
        self._sender.close_channel();
        debug!("WatchStreaming channel closed");
    }

    /// Returns whether the request channel is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self._sender.is_closed()
    }
}

impl Stream for WatchStreaming {
    type Item = Result<WatchResponse>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.get_mut().inner).poll_next(cx) {
            Poll::Ready(Some(Ok(resp))) => Poll::Ready(Some(Ok(resp))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for WatchStreaming {
    fn drop(&mut self) {
        debug!("WatchStreaming dropped");
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;
    use xlineapi::command::KeyRange;

    use crate::transport::Streaming;

    use super::*;

    fn make_pair() -> (
        Watcher,
        WatchStreaming,
        mpsc::Receiver<xlineapi::WatchRequest>,
    ) {
        let (tx, rx) = mpsc::channel::<xlineapi::WatchRequest>(128);
        let watcher = Watcher::new(1, tx.clone());
        let streaming = WatchStreaming::new(
            Streaming::new(Box::pin(futures::stream::empty()), "test"),
            tx,
        );
        (watcher, streaming, rx)
    }

    #[test]
    fn test_watch_request_build_from_watch_options() {
        let options = WatchOptions::default().with_prev_kv().with_key("key");
        let request = xlineapi::WatchCreateRequest::from(options.clone());
        assert!(request.prev_kv);
        assert!(request.range_end.is_empty());

        let options2 = options.clone().with_prefix();
        let request = xlineapi::WatchCreateRequest::from(options2.clone());
        assert_eq!(request.range_end, KeyRange::get_prefix("key"));
    }

    #[test]
    fn watcher_close_marks_both_closed() {
        let (mut watcher, streaming, _rx) = make_pair();
        assert!(!watcher.is_closed());
        assert!(!streaming.is_closed());

        watcher.close();

        assert!(watcher.is_closed());
        assert!(streaming.is_closed());
    }

    #[test]
    fn watch_streaming_close_marks_both_closed() {
        let (watcher, mut streaming, _rx) = make_pair();
        assert!(!watcher.is_closed());
        assert!(!streaming.is_closed());

        streaming.close();

        assert!(watcher.is_closed());
        assert!(streaming.is_closed());
    }

    #[test]
    fn watcher_close_idempotent() {
        let (mut watcher, _streaming, _rx) = make_pair();
        watcher.close();
        watcher.close();
        watcher.close();
        assert!(watcher.is_closed());
    }

    #[test]
    fn watch_streaming_close_idempotent() {
        let (_watcher, mut streaming, _rx) = make_pair();
        streaming.close();
        streaming.close();
        streaming.close();
        assert!(streaming.is_closed());
    }

    #[test]
    fn drop_watcher_alone_does_not_close_channel() {
        let (tx, rx) = mpsc::channel::<xlineapi::WatchRequest>(128);
        let watcher = Watcher::new(1, tx.clone());
        let streaming = WatchStreaming::new(
            Streaming::new(Box::pin(futures::stream::empty()), "test"),
            tx,
        );

        drop(watcher);
        let _rx = rx;

        assert!(!streaming.is_closed());
    }

    #[test]
    fn drop_streaming_then_watcher_closes_channel() {
        let (tx, rx) = mpsc::channel::<xlineapi::WatchRequest>(128);
        let watcher = Watcher::new(1, tx.clone());
        let streaming = WatchStreaming::new(
            Streaming::new(Box::pin(futures::stream::empty()), "test"),
            tx,
        );
        let _rx = rx;

        drop(streaming);
        assert!(!watcher.is_closed());

        drop(watcher);
    }

    #[test]
    fn watcher_send_after_close_errors() {
        let (mut watcher, _streaming, _rx) = make_pair();
        watcher.close();

        let err = watcher
            .watch(WatchOptions::default().with_key("k"))
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("closed")
                || msg.contains("disconnected")
                || msg.contains("full")
                || msg.contains("gone"),
            "unexpected error: {msg}"
        );
    }
}
