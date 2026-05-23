//! H3 server implementation for Xline
//!
//! This module provides the HTTP/3 server functionality specific to Xline,
//! including port-based routing between client requests and CURP peer communication.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use curp::rpc::QuicGrpcServer;
use dquic::prelude::EndpointAddr;
use h3::quic::{BidiStream, SendStream};
use h3::server::RequestStream;
use http::{Request, Response};
use tower::{Service, ServiceExt};
use tracing::{debug, error, info, trace, warn};
use xlineapi::command::{Command, CurpClient};

use super::port_router::{ConnectionTarget, PortRouter};
use super::quic_runtime::SharedQuicRuntime;
use super::server_registry::ServerRegistry;
use crate::server::command::CommandExecutor;
use crate::state::State;
use utils::config::TlsConfig;

/// Router builder for Xline H3 server
#[derive(Clone)]
pub(crate) struct RouterBuilder {
    router: xlinerpc::server::StateRouter<()>,
    pub(crate) tls_config: TlsConfig,
}

impl RouterBuilder {
    /// Create a new builder
    pub(crate) fn new() -> Self {
        Self {
            router: xlinerpc::server::StateRouter::new().fallback(unimplemented),
            tls_config: TlsConfig::default(),
        }
    }

    /// Add a nested router
    pub(crate) fn add_subrouter(
        mut self,
        name: &str,
        router: xlinerpc::server::StateRouter<()>,
    ) -> Self {
        self.router = self.router.nest(name, router);
        self
    }

    /// Set TLS config
    pub(crate) fn set_tls_config(mut self, config: &TlsConfig) -> Self {
        self.tls_config = config.clone();
        self
    }

    /// Get the inner router
    pub(crate) fn into_inner(self) -> xlinerpc::server::StateRouter<()> {
        self.router
    }
}

impl Default for RouterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct XlineH3Server {
    routers: HashMap<String, (RouterBuilder, Vec<String>)>,
    client_ports: HashSet<u16>,
    peer_ports: HashSet<u16>,
}

impl XlineH3Server {
    /// Create a new Xline H3 server
    pub(crate) fn new() -> Self {
        Self {
            routers: HashMap::new(),
            client_ports: HashSet::new(),
            peer_ports: HashSet::new(),
        }
    }

    /// Set client ports
    pub(crate) fn with_client_ports(mut self, ports: HashSet<u16>) -> Self {
        self.client_ports = ports;
        self
    }

    /// Set peer ports
    pub(crate) fn with_peer_ports(mut self, ports: HashSet<u16>) -> Self {
        self.peer_ports = ports;
        self
    }

    /// Add a server router
    pub(crate) fn add_server(
        mut self,
        name: &str,
        router: RouterBuilder,
        peer_urls: impl IntoIterator<Item = String>,
    ) -> Self {
        if self.routers.contains_key(name) {
            panic!("duplicate server name: {name}");
        }
        let _ = self
            .routers
            .insert(name.to_string(), (router, peer_urls.into_iter().collect()));
        self
    }

    pub(crate) async fn serve(
        self,
        grpc_server: QuicGrpcServer<
            Command,
            CommandExecutor,
            State<Arc<CurpClient>>,
            crate::server::quic_service::XlineQuicService,
        >,
    ) -> Result<(), anyhow::Error> {
        debug!(
            client_ports = ?self.client_ports,
            peer_ports = ?self.peer_ports,
            routers_count = self.routers.len(),
            "serve start"
        );

        let (listeners, handle) = SharedQuicRuntime::get_or_init()?;
        let is_first = handle.is_first;
        let registry = ServerRegistry::new(&listeners, handle);

        let grpc_server = Arc::new(grpc_server);
        for (server_name, (router_builder, peer_urls)) in &self.routers {
            registry
                .register_server(
                    server_name,
                    router_builder.clone(),
                    peer_urls.clone(),
                    &self.client_ports,
                    &self.peer_ports,
                    Arc::clone(&grpc_server),
                )
                .await?;
        }

        if is_first {
            let port_router = PortRouter::new(registry.into_servers());
            info!("starting global accept loop (first server)");
            Self::accept_loop(&listeners, &port_router).await
        } else {
            info!("not first server - registration done, accept loop already running");
            Ok(())
        }
    }

    async fn accept_loop(
        listeners: &dquic::prelude::QuicListeners,
        port_router: &PortRouter,
    ) -> Result<(), anyhow::Error> {
        loop {
            trace!("waiting for incoming connection...");
            let (new_conn, server_name, pathway, _link) = listeners
                .accept()
                .await
                .map_err(|e| anyhow::anyhow!("quic listener accept failed: {e}"))?;

            debug!(server_name, pathway = ?pathway.local(), "connection accepted");

            let EndpointAddr::Socket(socket_addr) = pathway.local() else {
                warn!(pathway = ?pathway.local(), "could not get local port from pathway");
                continue;
            };

            let local_port = socket_addr.addr().port();
            match port_router.route(&server_name, local_port).await {
                ConnectionTarget::PeerCurp => {
                    debug!(server_name, local_port, "dispatching to peer grpc server");
                    let routing = port_router
                        .get_routing_info(&server_name)
                        .await
                        .ok_or_else(|| anyhow::anyhow!("server {} not found", server_name))?;
                    let _handle = routing.grpc_server.spawn_connection(new_conn);
                }
                ConnectionTarget::ClientH3 => {
                    debug!(server_name, local_port, "dispatching to client h3 router");
                    let h3_conn = match h3::server::Connection::new(h3_shim::QuicConnection::new(
                        Arc::new(new_conn),
                    ))
                    .await
                    {
                        Ok(h3_conn) => h3_conn,
                        Err(error) => {
                            error!(local_port, error = %error, "failed to establish h3 connection");
                            continue;
                        }
                    };
                    let router =
                        port_router
                            .get_h3_router(&server_name)
                            .await
                            .ok_or_else(|| {
                                anyhow::anyhow!("server {} not found for h3", server_name)
                            })?;
                    let _ = tokio::spawn(Self::handle_connection(router, h3_conn));
                }
                ConnectionTarget::Unknown => {
                    error!(
                        server_name,
                        local_port, "received connection on unknown local port"
                    );
                }
            }
        }
    }

    async fn handle_connection<T>(
        router: xlinerpc::server::StateRouter<()>,
        mut connection: h3::server::Connection<T, Bytes>,
    ) where
        T: h3::quic::Connection<Bytes> + 'static,
        <T as h3::quic::OpenStreams<Bytes>>::BidiStream: BidiStream<Bytes> + Send + 'static,
        <<T as h3::quic::OpenStreams<Bytes>>::BidiStream as BidiStream<Bytes>>::RecvStream: Send,
        <<T as h3::quic::OpenStreams<Bytes>>::BidiStream as BidiStream<Bytes>>::SendStream: Send,
    {
        let svc = router.into_service();
        loop {
            match connection.accept().await {
                Ok(Some(request_resolver)) => {
                    let svc = svc.clone();
                    let _ = tokio::spawn(async move {
                        let (request, stream) = request_resolver.resolve_request().await?;
                        let res = handle_request(request, stream, svc).await;
                        res.map_err(|e| {
                            error!("Handling request failed: {}", e);
                            e
                        })
                    });
                }
                Ok(None) => {
                    trace!("connection accepted, no pending requests");
                    break;
                }
                Err(e) => {
                    error!("encounter an error: {e:?}");
                    break;
                }
            }
        }
    }
}

async fn unimplemented() -> impl axum::response::IntoResponse {
    error!("unimplemented");
    let status = http::StatusCode::OK;
    let grpc_unimplemented_code = i32::from(xlinerpc::Code::Unimplemented).to_string();
    let headers = [
        (
            http::header::HeaderName::from_static("grpc-status"),
            http::HeaderValue::from_str(&grpc_unimplemented_code)
                .unwrap_or_else(|_| http::HeaderValue::from_static("12")),
        ),
        (
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/grpc"),
        ),
    ];
    (status, headers)
}

async fn handle_request<T, SVC, ResBody>(
    request: Request<()>,
    stream: RequestStream<T, Bytes>,
    mut service: SVC,
) -> Result<(), anyhow::Error>
where
    T: BidiStream<Bytes> + 'static,
    SVC: Service<
            Request<xlinerpc::server::QuicIncomingBody<T::RecvStream>>,
            Response = Response<ResBody>,
        > + Clone
        + Send
        + 'static,
    SVC::Future: Send + 'static,
    SVC::Error: Into<anyhow::Error> + Send + Sync + std::error::Error,
    ResBody: http_body::Body<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<anyhow::Error> + Send + Sync + std::error::Error + 'static,
{
    let (mut send, recv) = stream.split();
    let body = xlinerpc::server::QuicIncomingBody::new(
        recv,
        request
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|len| len.to_str().ok().and_then(|x| x.parse().ok())),
    );
    // Wait for the service to be ready before processing the request
    // This is required by the Tower Service contract
    let _ = service.ready().await.map_err(|e| {
        error!("service not ready: {}", e);
        anyhow::anyhow!("service not ready: {}", e)
    })?;
    let (parts, _) = request.into_parts();
    let resp = service.call(Request::from_parts(parts, body)).await?;
    let (parts, body) = resp.into_parts();
    send.send_response(Response::from_parts(parts, ())).await?;
    copy_response_body(send, body).await?;
    Ok(())
}

async fn copy_response_body<S, ResBody>(
    mut send: RequestStream<S, Bytes>,
    body: ResBody,
) -> Result<(), anyhow::Error>
where
    S: SendStream<Bytes>,
    ResBody: http_body::Body<Data = Bytes>,
    ResBody::Error: Into<anyhow::Error> + Send + Sync + std::error::Error + 'static,
{
    let mut body = std::pin::pin!(body);

    while let Some(frame) = futures::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        match frame?.into_data() {
            Ok(data) => send.send_data(data).await?,
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    send.send_trailers(trailers).await?;
                } else {
                    warn!("failed to get body frame");
                }
                continue;
            }
        }
    }

    send.finish().await?;

    Ok(())
}
