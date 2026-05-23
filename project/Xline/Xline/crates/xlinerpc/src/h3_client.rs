use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::endpoint::{DnsFallback, EndpointError, resolve_endpoint_with_fallback};
use bytes::{Buf, Bytes};
use dquic::prelude::{Connection as GmConnection, QuicClient};
use dquic::qresolve::Source;
use futures::{Stream, StreamExt};
use h3::client::RequestStream;
use h3_shim::BidiStream;
use prost::Message;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, warn};

use crate::{Status, Streaming, grpc};

/// Fallback idle timeout used for stream reads when global timeout is disabled.
const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Minimum timeout used for transport operations to reduce false positives.
const MIN_EFFECTIVE_TIMEOUT: Duration = Duration::from_secs(3);

#[inline]
fn it_debug_enabled() -> bool {
    std::env::var("XLINE_IT_DEBUG")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(false)
}

#[allow(clippy::print_stderr)]
#[inline]
fn it_debug(msg: impl AsRef<str>) {
    if it_debug_enabled() {
        eprintln!("[xlinerpc-h3-debug] {}", msg.as_ref());
    }
}

/// Type alias for h3 request stream with BidiStream
pub type H3RequestStream = RequestStream<BidiStream<Bytes>, Bytes>;

/// Factory for creating new QUIC/H3 connections.
///
/// This is NOT a connection pool — each RPC creates a fresh QUIC connection,
/// H3 session, and driver task. Connection reuse is architecturally feasible
/// (h3 `SendRequest` is `Clone`, QUIC supports multiplexed streams) but would
/// require a session cache with lifecycle management, health eviction, and
/// shutdown hooks. See `docs/quic-refactor-audit.md` §Connection Reuse for the
/// full analysis and future design.
#[derive(Clone)]
struct H3ClientFactory {
    client: Arc<QuicClient>,
}

impl H3ClientFactory {
    #[inline]
    fn new(client: Arc<QuicClient>) -> Self {
        Self { client }
    }

    #[inline]
    fn client(&self) -> Arc<QuicClient> {
        Arc::clone(&self.client)
    }
}

#[derive(Clone)]
pub struct H3Channel {
    pool: Arc<H3ClientFactory>,
    addrs: Arc<RwLock<Vec<String>>>,
    index: Arc<AtomicUsize>,
    timeout: Duration,
}

impl std::fmt::Debug for H3Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3Channel")
            .field("index", &self.index)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl H3Channel {
    #[inline]
    pub fn new(client: Arc<QuicClient>, addrs: Vec<String>, timeout: Duration) -> Self {
        Self {
            pool: Arc::new(H3ClientFactory::new(client)),
            addrs: Arc::new(RwLock::new(addrs)),
            index: Arc::new(AtomicUsize::new(0)),
            timeout,
        }
    }

    async fn get_endpoint(&self) -> Result<String, Status> {
        let addrs = self.addrs.read().await;
        if addrs.is_empty() {
            return Err(Status::unavailable("no available endpoints"));
        }

        let len = addrs.len();
        let idx = self.index.fetch_add(1, Ordering::Relaxed) % len;
        let addr = addrs[idx]
            .strip_prefix("quic://")
            .or_else(|| addrs[idx].strip_prefix("https://"))
            .or_else(|| addrs[idx].strip_prefix("http://"))
            .unwrap_or(&addrs[idx]);
        Ok(addr.to_string())
    }

    fn build_request(
        path: &str,
        method_id_hex: Option<&str>,
        metadata: &[(String, String)],
        endpoint: &str,
    ) -> Result<http::Request<()>, Status> {
        let uri = format!("https://{endpoint}{path}");

        let mut builder = http::Request::builder().method(http::Method::POST).uri(uri);

        if let Some(method_id_hex) = method_id_hex {
            let method_id_header = http::header::HeaderName::from_bytes(b"x-method-id")
                .map_err(|e| Status::internal(format!("invalid x-method-id header: {e}")))?;
            builder = builder.header(method_id_header, method_id_hex);
        }

        let content_type = http::header::HeaderName::from_bytes(b"content-type")
            .map_err(|e| Status::internal(format!("invalid content-type header: {e}")))?;
        builder = builder.header(content_type, "application/grpc+proto");

        for (key, value) in metadata {
            let header_name = http::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| Status::internal(format!("invalid header name {key}: {e}")))?;
            let header_value = http::header::HeaderValue::from_str(value)
                .map_err(|e| Status::internal(format!("invalid header value for {key}: {e}")))?;
            builder = builder.header(header_name, header_value);
        }

        builder
            .body(())
            .map_err(|e| Status::internal(format!("build request error: {e}")))
    }

    async fn create_connection(&self, endpoint_str: &str) -> Result<GmConnection, Status> {
        let client = self.pool.client();
        debug!(endpoint = endpoint_str, "H3 create_connection start");
        it_debug(format!("quic connect start endpoint={}", endpoint_str));

        let resolved = resolve_endpoint_with_fallback(endpoint_str, DnsFallback::Disabled)
            .await
            .map_err(|e| match &e {
                EndpointError::DnsError { host, .. } => Status::unavailable(format!(
                    "{e}\n\
                         Hint: Check /etc/hosts or DNS for '{host}'. \
                         If testing locally, add '127.0.0.1 {host}' to /etc/hosts."
                )),
                EndpointError::ParseError { .. } => Status::invalid_argument(format!(
                    "{e}\n\
                         Hint: Endpoint must be 'scheme://host:port', e.g., 'https://server0:2379'."
                )),
                _ => Status::internal(e.to_string()),
            })?;
        let server_name = resolved.server_name;
        let socket_addr = resolved.socket_addr;

        debug!(endpoint = endpoint_str, %server_name, %socket_addr, "H3 endpoint resolved");
        it_debug(format!(
            "quic connect: endpoint={}, server_name={}, addr={}",
            endpoint_str, server_name, socket_addr
        ));

        let conn = match client
            .connected_to_with_source(&server_name, [(Source::System, socket_addr.into())])
            .await
        {
            Ok(conn) => conn,
            Err(e) => {
                warn!(endpoint = endpoint_str, %server_name, %socket_addr, error = %e, "H3 connect failed");
                it_debug(format!(
                    "connected_to_with_source failed for server_name={}: {}",
                    server_name, e
                ));
                return Err(Status::unavailable(format!(
                    "QUIC connect error: endpoint='{endpoint_str}', server_name='{server_name}', addr={socket_addr}: {e}\n\
                     Hint: Verify the server is running and reachable. If using TLS, check that the CA certificate is correct \
                     and the server's cert includes '{server_name}' in its Subject Alternative Name."
                )));
            }
        };
        debug!(endpoint = endpoint_str, %server_name, "H3 QUIC connected");
        it_debug(format!(
            "quic connect established endpoint={}",
            endpoint_str
        ));

        with_timeout(effective_timeout(self.timeout), "quic handshake", async {
            match conn.handshaked().await {
                Ok(_) => {
                    debug!(endpoint = endpoint_str, %server_name, "H3 handshake ok");
                    it_debug(format!(
                        "quic handshake ok endpoint={} server_name={}",
                        endpoint_str, server_name
                    ));
                    Ok(())
                }
                Err(e) => {
                    warn!(endpoint = endpoint_str, %server_name, error = %e, "H3 handshake failed");
                    it_debug(format!(
                        "quic handshake FAILED endpoint={} server_name={}: {}",
                        endpoint_str, server_name, e
                    ));
                    Err(Status::unavailable(format!(
                        "QUIC handshake error: endpoint='{endpoint_str}', server_name='{server_name}', addr={socket_addr}: {e}\n\
                         Hint: TLS handshake failure often means the CA certificate doesn't match the server's cert, \
                         or the server's cert SAN doesn't include '{server_name}'."
                    )))
                }
            }
        })
        .await?;
        debug!(endpoint = endpoint_str, %server_name, "H3 connection ready");
        it_debug(format!("quic handshake ok endpoint={}", endpoint_str));
        Ok(conn)
    }

    /// Connect with one retry on `Status::unavailable`. Retries use the next
    /// endpoint from the round-robin pool, so a transient single-server failure
    /// does not fail the entire RPC.
    async fn connect_with_retry(&self) -> Result<(String, GmConnection), Status> {
        const RETRY_DELAY: Duration = Duration::from_millis(50);

        let endpoint_str = self.get_endpoint().await?;
        match self.create_connection(&endpoint_str).await {
            Ok(conn) => Ok((endpoint_str, conn)),
            Err(e) if e.code() == crate::Code::Unavailable => {
                debug!(first_endpoint = endpoint_str, error = %e, "H3 connect failed, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
                let next_endpoint = self.get_endpoint().await?;
                debug!(retry_endpoint = next_endpoint, "H3 retry connect");
                let conn = self.create_connection(&next_endpoint).await?;
                Ok((next_endpoint, conn))
            }
            Err(e) => Err(e),
        }
    }

    // Each RPC creates a new QUIC connection, H3 session, and driver task.
    // This is intentional: h3 SendRequest is Clone (connection reuse is feasible)
    // but the current design prioritizes simplicity over performance.
    // See H3ClientFactory doc for the connection reuse analysis.
    pub async fn unary<Req, Resp>(
        &self,
        path: &str,
        method_id_hex: Option<&str>,
        metadata: &[(String, String)],
        req: Req,
    ) -> Result<Resp, Status>
    where
        Req: Message,
        Resp: Message + Default,
    {
        let (endpoint_str, conn) = self.connect_with_retry().await?;
        it_debug(format!(
            "h3 init start endpoint={} path={}",
            endpoint_str, path
        ));

        let (mut h3_driver, mut send_req) =
            with_timeout(effective_timeout(self.timeout), "h3 init", async {
                h3::client::new(h3_shim::QuicConnection::new(Arc::new(conn)))
                    .await
                    .map_err(|e| Status::unavailable(format!("h3 init error: {e}")))
            })
            .await?;
        it_debug(format!(
            "h3 init ok endpoint={} path={}",
            endpoint_str, path
        ));

        let request = Self::build_request(path, method_id_hex, metadata, &endpoint_str)?;
        let mut stream = with_timeout(effective_timeout(self.timeout), "send request", async {
            send_req
                .send_request(request)
                .await
                .map_err(h3_stream_error_to_status)
        })
        .await?;

        let _driver = tokio::spawn(async move {
            let _ = h3_driver.wait_idle().await;
        });

        let grpc_body = grpc::frame_encode(&req)?;
        with_timeout(self.timeout, "send request body", async {
            stream
                .send_data(grpc_body)
                .await
                .map_err(h3_stream_error_to_status)
        })
        .await?;
        with_timeout(self.timeout, "finish request stream", async {
            stream.finish().await.map_err(h3_stream_error_to_status)
        })
        .await?;

        let resp = with_timeout(
            effective_timeout(self.timeout),
            "receive response headers",
            async {
                stream
                    .recv_response()
                    .await
                    .map_err(h3_stream_error_to_status)
            },
        )
        .await?;
        http_status_to_result(resp.status())?;

        if let Some(err) = grpc::error_from_headers(resp.headers()) {
            return Err(err);
        }

        let mut buf = Vec::new();
        loop {
            match with_timeout(self.timeout, "receive response data", async {
                stream.recv_data().await.map_err(h3_stream_error_to_status)
            })
            .await
            {
                Ok(Some(data)) => buf.extend_from_slice(data.chunk()),
                Ok(None) => break,
                Err(e) => {
                    if let Ok(Some(trailers)) = stream.recv_trailers().await {
                        if let Some(err) = grpc::error_from_headers(&trailers) {
                            return Err(err);
                        }
                    }
                    return Err(e);
                }
            }
        }

        if let Ok(Some(trailers)) = stream.recv_trailers().await {
            if let Some(err) = grpc::error_from_headers(&trailers) {
                return Err(err);
            }
        }

        stream.stop_sending(h3::error::Code::H3_NO_ERROR);

        if buf.is_empty() {
            return Err(Status::internal("empty response body"));
        }

        let (msg, _) = grpc::frame_decode::<Resp>(&buf)?;
        Ok(msg)
    }

    pub async fn server_streaming<Req, Resp>(
        &self,
        path: &str,
        method_id_hex: Option<&str>,
        metadata: &[(String, String)],
        req: Req,
    ) -> Result<Streaming<Resp>, Status>
    where
        Req: Message,
        Resp: Message + Default + Send + Unpin + 'static,
    {
        let (endpoint_str, conn) = self.connect_with_retry().await?;

        let (mut h3_driver, mut send_req) =
            with_timeout(effective_timeout(self.timeout), "h3 init", async {
                h3::client::new(h3_shim::QuicConnection::new(Arc::new(conn)))
                    .await
                    .map_err(|e| Status::unavailable(format!("h3 init error: {e}")))
            })
            .await?;

        let request = Self::build_request(path, method_id_hex, metadata, &endpoint_str)?;
        let mut stream = with_timeout(effective_timeout(self.timeout), "send request", async {
            send_req
                .send_request(request)
                .await
                .map_err(h3_stream_error_to_status)
        })
        .await?;

        let grpc_body = grpc::frame_encode(&req)?;
        with_timeout(self.timeout, "send request body", async {
            stream
                .send_data(grpc_body)
                .await
                .map_err(h3_stream_error_to_status)
        })
        .await?;
        with_timeout(self.timeout, "finish request stream", async {
            stream.finish().await.map_err(h3_stream_error_to_status)
        })
        .await?;

        let resp = with_timeout(
            effective_timeout(self.timeout),
            "receive response headers",
            async {
                stream
                    .recv_response()
                    .await
                    .map_err(h3_stream_error_to_status)
            },
        )
        .await?;
        http_status_to_result(resp.status())?;

        if let Some(err) = grpc::error_from_headers(resp.headers()) {
            return Err(err);
        }

        let _driver = tokio::spawn(async move {
            let _ = h3_driver.wait_idle().await;
        });

        let timeout = self.timeout;
        let read_timeout = stream_idle_timeout(timeout);
        let path_owned = path.to_string();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Resp, Status>>(128);
        // Keep `send_req` alive for the entire lifetime of the streaming response.
        // See the comment in client_streaming for details.
        let _reader = tokio::spawn(async move {
            let _send_req = send_req;
            let mut buf = Vec::new();
            debug!("h3 server-streaming reader started path={}", path_owned);

            loop {
                match with_timeout(
                    effective_timeout(read_timeout),
                    "receive stream data",
                    async { stream.recv_data().await.map_err(h3_stream_error_to_status) },
                )
                .await
                {
                    Ok(Some(data)) => {
                        buf.extend_from_slice(data.chunk());
                        while buf.len() >= grpc::HEADER_SIZE {
                            let (_, total) = grpc::calculate_frame_size(&buf);
                            if buf.len() < total {
                                break;
                            }
                            let frame = buf[..total].to_vec();
                            let _ = buf.drain(..total);
                            match grpc::frame_decode::<Resp>(&frame) {
                                Ok((msg, _)) => {
                                    if tx.send(Ok(msg)).await.is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(
                            "h3 server-streaming read failed path={} err={}",
                            path_owned, e
                        );
                        stream.stop_sending(h3::error::Code::H3_NO_ERROR);
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }

            stream.stop_sending(h3::error::Code::H3_NO_ERROR);

            if !buf.is_empty() {
                warn!(
                    "h3 server-streaming ended with partial frame path={}",
                    path_owned
                );
                let _ = tx
                    .send(Err(Status::internal("incomplete gRPC frame at stream end")))
                    .await;
                return;
            }

            if let Ok(Some(trailers)) = stream.recv_trailers().await
                && let Some(err) = grpc::error_from_headers(&trailers)
            {
                warn!(
                    "h3 server-streaming trailers reported error path={} err={}",
                    path_owned, err
                );
                let _ = tx.send(Err(err)).await;
            }
            debug!("h3 server-streaming reader finished path={}", path_owned);
        });

        Ok(Streaming::new(
            Box::pin(ReceiverStream::new(rx)),
            "server_streaming",
        ))
    }

    pub async fn client_streaming<Req, Resp, St>(
        &self,
        path: &str,
        method_id_hex: Option<&str>,
        metadata: &[(String, String)],
        input: St,
    ) -> Result<Streaming<Resp>, Status>
    where
        Req: Message + Send + 'static,
        Resp: Message + Default + Send + Unpin + 'static,
        St: Stream<Item = Req> + Send + 'static,
    {
        let (endpoint_str, conn) = self.connect_with_retry().await?;

        let (mut h3_driver, mut send_req) =
            with_timeout(effective_timeout(self.timeout), "h3 init", async {
                h3::client::new(h3_shim::QuicConnection::new(Arc::new(conn)))
                    .await
                    .map_err(|e| Status::unavailable(format!("h3 init error: {e}")))
            })
            .await?;

        let request = Self::build_request(path, method_id_hex, metadata, &endpoint_str)?;
        let mut stream = with_timeout(effective_timeout(self.timeout), "send request", async {
            send_req
                .send_request(request)
                .await
                .map_err(h3_stream_error_to_status)
        })
        .await?;

        // Receive response headers first (server sends them immediately for gRPC).
        let resp = with_timeout(
            effective_timeout(self.timeout),
            "receive response headers",
            async {
                stream
                    .recv_response()
                    .await
                    .map_err(h3_stream_error_to_status)
            },
        )
        .await?;
        http_status_to_result(resp.status())?;

        if let Some(err) = grpc::error_from_headers(resp.headers()) {
            return Err(err);
        }

        let _driver = tokio::spawn(async move {
            let _ = h3_driver.wait_idle().await;
        });

        let path_owned = path.to_string();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Resp, Status>>(128);

        // Keep `send_req` alive — see the comment in server_streaming for details.
        //
        // Use a single task with `tokio::select!` to handle both sending and
        // receiving.  The previous `Arc<Mutex<RequestStream>>` design caused a
        // deadlock: `recv_data()` would hold the lock while waiting for the
        // server's response, preventing `send_data()` from acquiring the lock
        // to send the initial request — so the server never received the
        // request and never sent any data back.
        let _handler = tokio::spawn(async move {
            let _send_req = send_req;
            let input = input;
            futures::pin_mut!(input);
            let mut buf = Vec::new();
            let mut stream_done = false;
            let mut input_done = false;

            debug!("h3 client-streaming handler started path={}", path_owned);

            loop {
                if input_done && stream_done {
                    break;
                }

                tokio::select! {
                    // Send side: forward messages from the input stream
                    msg = input.next(), if !input_done => {
                        match msg {
                            Some(msg) => {
                                let grpc_body = match grpc::frame_encode(&msg) {
                                    Ok(body) => body,
                                    Err(e) => {
                                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                                        return;
                                    }
                                };
                                if let Err(e) = stream.send_data(grpc_body).await {
                                    let _ = tx.send(Err(h3_stream_error_to_status(e))).await;
                                    return;
                                }
                            }
                            None => {
                                input_done = true;
                                // Do NOT call stream.finish() — for bidirectional
                                // streaming (e.g. watch) the request side must stay
                                // open so the client can send cancel/progress later.
                            }
                        }
                    }
                    // Receive side: read response data from the server
                    result = stream.recv_data(), if !stream_done => {
                        match result {
                            Ok(Some(data)) => {
                                buf.extend_from_slice(data.chunk());
                                while buf.len() >= grpc::HEADER_SIZE {
                                    let (_, total) = grpc::calculate_frame_size(&buf);
                                    if buf.len() < total {
                                        break;
                                    }
                                    let frame = buf[..total].to_vec();
                                    let _ = buf.drain(..total);
                                    match grpc::frame_decode::<Resp>(&frame) {
                                        Ok((msg, _)) => {
                                            if tx.send(Ok(msg)).await.is_err() {
                                                return;
                                            }
                                        }
                                        Err(e) => {
                                            let _ = tx.send(Err(e)).await;
                                            return;
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                stream_done = true;
                            }
                            Err(e) => {
                                warn!(
                                    "h3 client-streaming read failed path={} err={}",
                                    path_owned, e
                                );
                                stream.stop_sending(h3::error::Code::H3_NO_ERROR);
                                let _ = tx.send(Err(h3_stream_error_to_status(e))).await;
                                return;
                            }
                        }
                    }
                }
            }

            stream.stop_sending(h3::error::Code::H3_NO_ERROR);

            if !buf.is_empty() {
                warn!(
                    "h3 client-streaming ended with partial frame path={}",
                    path_owned
                );
                let _ = tx
                    .send(Err(Status::internal("incomplete gRPC frame at stream end")))
                    .await;
                return;
            }

            if let Ok(Some(trailers)) = stream.recv_trailers().await
                && let Some(err) = grpc::error_from_headers(&trailers)
            {
                warn!(
                    "h3 client-streaming trailers reported error path={} err={}",
                    path_owned, err
                );
                let _ = tx.send(Err(err)).await;
            }
            debug!("h3 client-streaming handler finished path={}", path_owned);
        });

        Ok(Streaming::new(
            Box::pin(ReceiverStream::new(rx)),
            "client_streaming",
        ))
    }
}

async fn with_timeout<T>(
    timeout: Duration,
    operation: &str,
    fut: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    if timeout.is_zero() {
        return fut.await;
    }

    tokio::time::timeout(timeout, fut).await.map_err(|_| {
        Status::deadline_exceeded(format!("{operation} timed out after {timeout:?}"))
    })?
}

#[inline]
fn stream_idle_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        DEFAULT_STREAM_IDLE_TIMEOUT
    } else {
        timeout
    }
}

#[inline]
fn effective_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        Duration::ZERO
    } else {
        timeout.max(MIN_EFFECTIVE_TIMEOUT)
    }
}

/// Convert h3 stream error to RPC status.
fn h3_stream_error_to_status(e: h3::error::StreamError) -> Status {
    Status::internal(format!("h3 stream error: {e}"))
}

/// Convert HTTP status code to RPC status result.
fn http_status_to_result(status: http::StatusCode) -> Result<(), Status> {
    use crate::Code;

    if status.is_success() {
        return Ok(());
    }

    let code = match status {
        http::StatusCode::BAD_REQUEST => Code::InvalidArgument,
        http::StatusCode::UNAUTHORIZED => Code::Unauthenticated,
        http::StatusCode::FORBIDDEN => Code::PermissionDenied,
        http::StatusCode::NOT_FOUND => Code::NotFound,
        http::StatusCode::TOO_MANY_REQUESTS => Code::ResourceExhausted,
        http::StatusCode::REQUEST_TIMEOUT => Code::DeadlineExceeded,
        http::StatusCode::INTERNAL_SERVER_ERROR => Code::Internal,
        http::StatusCode::SERVICE_UNAVAILABLE => Code::Unavailable,
        http::StatusCode::GATEWAY_TIMEOUT => Code::DeadlineExceeded,
        _ if status.is_client_error() => Code::InvalidArgument,
        _ if status.is_server_error() => Code::Internal,
        _ => Code::Unknown,
    };

    Err(Status::new(code, format!("HTTP {}", status)))
}
