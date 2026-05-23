use std::collections::HashSet;

use dquic::prelude::QuicListeners;
use tracing::{debug, info, warn};
use xlinerpc::server::{extract_host_from_url, parse_bind_uri};

use super::port_router::ServerRoutingInfo;
use super::quic_runtime::SharedQuicHandle;
use crate::server::h3_server::RouterBuilder;

pub(crate) struct ServerRegistry<'a> {
    listeners: &'a QuicListeners,
    handle: SharedQuicHandle,
}

impl<'a> ServerRegistry<'a> {
    pub(crate) fn new(listeners: &'a QuicListeners, handle: SharedQuicHandle) -> Self {
        Self { listeners, handle }
    }

    pub(crate) async fn register_server(
        &self,
        server_name: &str,
        router_builder: RouterBuilder,
        all_urls: Vec<String>,
        client_ports: &HashSet<u16>,
        peer_ports: &HashSet<u16>,
        grpc_server: std::sync::Arc<
            curp::rpc::QuicGrpcServer<
                xlineapi::command::Command,
                crate::server::command::CommandExecutor,
                crate::state::State<std::sync::Arc<xlineapi::command::CurpClient>>,
                crate::server::quic_service::XlineQuicService,
            >,
        >,
    ) -> anyhow::Result<()> {
        info!(server_name, ?all_urls, "registering server");

        let cert_path = router_builder
            .tls_config
            .peer_cert_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("server tls cert config is needed: {server_name}"))?;
        let key_path = router_builder
            .tls_config
            .peer_key_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("server tls key config is needed: {server_name}"))?;

        let bind_uris: Vec<_> = all_urls
            .iter()
            .map(|s| parse_bind_uri(s))
            .collect::<anyhow::Result<Vec<_>>>()?;
        debug!(server_name, ?bind_uris, "bind URIs parsed");

        self.listeners
            .add_server(
                server_name,
                cert_path.as_path(),
                key_path.as_path(),
                bind_uris,
                None,
            )
            .await?;
        info!(server_name, "server add_server done");

        self.register_sni_aliases(server_name, &cert_path, &key_path, &all_urls)
            .await;

        self.verify_bind(server_name)?;

        let routing_info = ServerRoutingInfo {
            client_ports: client_ports.clone(),
            peer_ports: peer_ports.clone(),
            h3_router: router_builder.into_inner(),
            grpc_server,
        };

        self.add_routing_entries(server_name, &routing_info, &all_urls)
            .await;

        Ok(())
    }

    async fn register_sni_aliases(
        &self,
        server_name: &str,
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
        all_urls: &[String],
    ) {
        if self.listeners.get_server(server_name).is_none() {
            if let Some(url_str) = all_urls.first() {
                if let Ok(bind_uris) = vec![parse_bind_uri(url_str)]
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                {
                    debug!("Registering server_name '{}' as SNI alias", server_name);
                    if let Err(e) = self
                        .listeners
                        .add_server(
                            server_name,
                            cert_path,
                            key_path,
                            bind_uris,
                            None,
                        )
                        .await
                    {
                        warn!(
                            "server_name SNI alias '{}' registration failed: {}",
                            server_name, e
                        );
                    }
                }
            }
        }

        for url_str in all_urls {
            if let Some(host) = extract_host_from_url(url_str) {
                debug!(
                    "Checking SNI alias: host='{}', server_name='{}'",
                    host, server_name
                );
                let bind_uris: Vec<_> = vec![match parse_bind_uri(url_str) {
                    Ok(u) => u,
                    Err(e) => {
                        warn!("Failed to parse bind URI '{}': {}", url_str, e);
                        continue;
                    }
                }];
                debug!(
                    "Registering SNI alias '{}' for server '{}' with URL {}",
                    host, server_name, url_str
                );
                match self
                    .listeners
                    .add_server(host, cert_path, key_path, bind_uris, None)
                    .await
                {
                    Ok(()) => {
                        info!("Registered SNI alias '{}' for server '{}'", host, server_name);
                    }
                    Err(e) => {
                        warn!(
                            "SNI alias '{}' registration failed (non-fatal): {}",
                            host, e
                        );
                    }
                }
            }
        }
    }

    fn verify_bind(&self, server_name: &str) -> anyhow::Result<()> {
        let bind_map = self
            .listeners
            .get_server(server_name)
            .ok_or_else(|| anyhow::anyhow!("server {} not found after registration", server_name))?
            .bind_interfaces();
        debug!(
            server_name,
            bind_count = bind_map.len(),
            "bind interfaces after add_server"
        );

        let (_, state) = bind_map
            .iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("server {} has no bound interfaces", server_name))?;
        let _interface = state.borrow();
        debug!(server_name, "server interface bind verified");
        Ok(())
    }

    async fn add_routing_entries(
        &self,
        server_name: &str,
        routing_info: &ServerRoutingInfo,
        all_urls: &[String],
    ) {
        let mut servers = self.handle.servers.write().await;

        let existing = servers.insert(server_name.to_string(), routing_info.clone());
        if existing.is_some() {
            warn!(server_name, "server routing info already existed, replaced");
        }
        info!(
            server_name,
            ?routing_info.client_ports,
            ?routing_info.peer_ports,
            "server routing info added to shared map"
        );

        for url_str in all_urls {
            if let Some(host) = extract_host_from_url(url_str) {
                if host != server_name && !servers.contains_key(host) {
                    let alias_info = ServerRoutingInfo {
                        client_ports: routing_info.client_ports.clone(),
                        peer_ports: routing_info.peer_ports.clone(),
                        h3_router: routing_info.h3_router.clone(),
                        grpc_server: std::sync::Arc::clone(&routing_info.grpc_server),
                    };
                    let _ = servers.insert(host.to_string(), alias_info);
                    debug!(alias = host, server = server_name, "SNI routing alias added");
                }
            }
        }
    }

    pub(crate) fn into_servers(
        self,
    ) -> std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, ServerRoutingInfo>>>
    {
        self.handle.servers
    }
}
