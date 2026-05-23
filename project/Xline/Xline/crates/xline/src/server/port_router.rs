use std::collections::{HashMap, HashSet};

use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionTarget {
    ClientH3,
    PeerCurp,
    Unknown,
}

pub(crate) struct ServerRoutingInfo {
    pub(crate) client_ports: HashSet<u16>,
    pub(crate) peer_ports: HashSet<u16>,
    pub(crate) h3_router: xlinerpc::server::StateRouter<()>,
    pub(crate) grpc_server: std::sync::Arc<
        curp::rpc::QuicGrpcServer<
            xlineapi::command::Command,
            crate::server::command::CommandExecutor,
            crate::state::State<std::sync::Arc<xlineapi::command::CurpClient>>,
            crate::server::quic_service::XlineQuicService,
        >,
    >,
}

impl Clone for ServerRoutingInfo {
    fn clone(&self) -> Self {
        Self {
            client_ports: self.client_ports.clone(),
            peer_ports: self.peer_ports.clone(),
            h3_router: self.h3_router.clone(),
            grpc_server: std::sync::Arc::clone(&self.grpc_server),
        }
    }
}

pub(crate) struct PortRouter {
    servers: std::sync::Arc<tokio::sync::RwLock<HashMap<String, ServerRoutingInfo>>>,
}

impl PortRouter {
    pub(crate) fn new(
        servers: std::sync::Arc<tokio::sync::RwLock<HashMap<String, ServerRoutingInfo>>>,
    ) -> Self {
        Self { servers }
    }

    pub(crate) async fn route(&self, server_name: &str, local_port: u16) -> ConnectionTarget {
        let servers = self.servers.read().await;
        if let Some(routing) = servers.get(server_name) {
            if routing.peer_ports.contains(&local_port) {
                debug!(server_name, local_port, "routing to peer CURP");
                ConnectionTarget::PeerCurp
            } else if routing.client_ports.contains(&local_port) {
                debug!(server_name, local_port, "routing to client H3");
                ConnectionTarget::ClientH3
            } else {
                debug!(
                    server_name,
                    local_port,
                    ?routing.client_ports,
                    ?routing.peer_ports,
                    "unknown port — not in client_ports or peer_ports"
                );
                ConnectionTarget::Unknown
            }
        } else {
            let known: Vec<&String> = servers.keys().collect();
            debug!(server_name, ?known, "server not found in routing table");
            ConnectionTarget::Unknown
        }
    }

    pub(crate) async fn get_routing_info(&self, server_name: &str) -> Option<ServerRoutingInfo> {
        let servers = self.servers.read().await;
        servers.get(server_name).cloned()
    }

    pub(crate) async fn get_h3_router(
        &self,
        server_name: &str,
    ) -> Option<xlinerpc::server::StateRouter<()>> {
        let servers = self.servers.read().await;
        servers.get(server_name).map(|r| r.h3_router.clone())
    }
}
