use std::{collections::HashMap, env, path::PathBuf, time::Duration};

use anyhow::Result;
use clap::Parser;
use tokio::fs;
use utils::{
    ConfigFileError,
    config::{
        AuthConfig, AutoCompactConfig, ClientConfig, ClusterConfig, CompactConfig,
        CurpConfigBuilder, EngineConfig, InitialClusterState, LevelConfig, LogConfig,
        MetricsConfig, MetricsPushProtocol, RotationConfig, ServerTimeout, StorageConfig,
        TlsConfig, TraceConfig, XlineServerConfig, default_batch_max_size, default_batch_timeout,
        default_candidate_timeout_ticks, default_client_id_keep_alive_interval,
        default_client_wait_synced_timeout, default_cmd_workers, default_compact_batch_size,
        default_compact_sleep_interval, default_compact_timeout, default_follower_timeout_ticks,
        default_gc_interval, default_heartbeat_interval, default_initial_retry_timeout,
        default_log_entries_cap, default_log_level, default_max_retry_timeout,
        default_metrics_enable, default_metrics_path, default_metrics_port,
        default_metrics_push_endpoint, default_metrics_push_protocol, default_propose_timeout,
        default_quota, default_range_retry_timeout, default_retry_count, default_rotation,
        default_rpc_timeout, default_server_wait_synced_timeout, default_sync_victims_interval,
        default_watch_progress_notify_interval,
    },
    parse_batch_bytes, parse_duration, parse_log_file, parse_log_level, parse_members,
    parse_metrics_push_protocol, parse_rotation, parse_state,
};

/// Xline server config path env name
const XLINE_SERVER_CONFIG_ENV: &str = "XLINE_SERVER_CONFIG";
/// default xline server config path
const DEFAULT_XLINE_SERVER_CONFIG_PATH: &str = "/etc/xline_server.conf";

/// Command line arguments
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[allow(clippy::struct_excessive_bools)] // arguments
pub struct ServerArgs {
    /// Node name
    #[clap(long)]
    name: String,
    /// Node peer listen urls
    #[clap(long, required = true, num_args = 1.., value_delimiter = ',')]
    peer_listen_urls: Vec<String>,
    /// Node peer advertise urls
    #[clap(long, num_args = 1.., value_delimiter = ',')]
    peer_advertise_urls: Vec<String>,
    /// Node client listen urls
    #[clap(long, required = true, num_args = 1.., value_delimiter = ',')]
    client_listen_urls: Vec<String>,
    /// Node client advertise urls
    #[clap(long, num_args = 1.., value_delimiter = ',')]
    client_advertise_urls: Vec<String>,
    /// Cluster peers. eg: node1=192.168.x.x:8080,192.168.x.x:8081,node2=192.168.x.x:8083
    #[clap(long, value_parser = parse_members)]
    members: HashMap<String, Vec<String>>,
    /// If node is leader
    #[clap(long)]
    is_leader: bool,
    /// Private key used to sign the token
    #[clap(long)]
    auth_private_key: Option<PathBuf>,
    /// Public key used to verify the token
    #[clap(long)]
    auth_public_key: Option<PathBuf>,
    /// Open jaeger offline
    #[clap(long)]
    offline: bool,
    /// output dir for jaeger offline
    #[clap(long, default_value = "./jaeger_jsons")]
    output_dir: PathBuf,
    /// Open jaeger online
    #[clap(long)]
    online: bool,
    /// Trace level of jaeger
    #[clap(long, value_parser = parse_log_level, default_value_t = default_log_level())]
    level: LevelConfig,
    /// Whether to enable metrics
    #[clap(long, default_value_t = default_metrics_enable())]
    metrics_enable: bool,
    /// Metrics port, default to "9100"
    #[clap(long, default_value_t = default_metrics_port())]
    metrics_port: u16,
    /// Metrics path, default to "/metrics"
    #[clap(long, default_value_t = default_metrics_path())]
    metrics_path: String,
    /// Whether to enable metrics push mode
    #[clap(long)]
    metrics_push: bool,
    /// Collector endpoint to collect metrics
    #[clap(long, default_value_t = default_metrics_push_endpoint())]
    metrics_push_endpoint: String,
    /// Collector protocol to collect metrics
    #[clap(long, value_parser = parse_metrics_push_protocol, default_value_t = default_metrics_push_protocol())]
    metrics_push_protocol: MetricsPushProtocol,
    /// Log file path
    #[clap(long, value_parser = parse_log_file, default_value = None)]
    log_file: Option<PathBuf>,
    /// Log rotate strategy, eg: never, hourly, daily
    #[clap(long, value_parser = parse_rotation, default_value_t = default_rotation())]
    log_rotate: RotationConfig,
    /// Log verbosity level, eg: trace, debug, info, warn, error
    #[clap(long, value_parser = parse_log_level, default_value_t = default_log_level())]
    log_level: LevelConfig,
    /// Heartbeat interval between curp server nodes [default: 300ms]
    #[clap(long, value_parser = parse_duration)]
    heartbeat_interval: Option<Duration>,
    /// Curp wait sync timeout [default: 5s]
    #[clap(long, value_parser = parse_duration)]
    server_wait_synced_timeout: Option<Duration>,
    /// Curp propose retry timeout [default: 50ms]
    #[clap(long, value_parser = parse_duration)]
    retry_timeout: Option<Duration>,
    /// Curp propose retry count [default: 3]
    #[clap(long)]
    retry_count: Option<usize>,
    /// Curp rpc timeout [default: 50ms]
    #[clap(long, value_parser = parse_duration)]
    rpc_timeout: Option<Duration>,
    /// Curp append entries batch timeout [default: 15ms]
    #[clap(long, value_parser = parse_duration)]
    batch_timeout: Option<Duration>,
    /// Curp append entries batch max size [default: 2MB]
    #[clap(long, value_parser = parse_batch_bytes)]
    batch_max_size: Option<u64>,
    /// Follower election timeout ticks
    #[clap(long, default_value_t = default_follower_timeout_ticks())]
    follower_timeout_ticks: u8,
    /// Candidate election timeout ticks
    #[clap(long, default_value_t = default_candidate_timeout_ticks())]
    candidate_timeout_ticks: u8,
    /// Number of log entries to keep in memory
    #[clap(long, default_value_t = default_log_entries_cap())]
    log_entries_cap: usize,
    /// Curp client wait synced timeout [default: 2s]
    #[clap(long, value_parser = parse_duration)]
    client_wait_synced_timeout: Option<Duration>,
    /// Propose request timeout [default: 1s]
    #[clap(long, value_parser = parse_duration)]
    client_propose_timeout: Option<Duration>,
    /// Curp client initial retry timeout [default: 50ms]
    #[clap(long, value_parser = parse_duration)]
    client_initial_retry_timeout: Option<Duration>,
    /// Curp client max retry timeout [default: `10_000ms`]
    #[clap(long, value_parser = parse_duration)]
    client_max_retry_timeout: Option<Duration>,
    /// Curp client use fixed backoff
    #[clap(long)]
    client_fixed_backoff: bool,
    /// Curp client id keep alive interval [default: 1s]
    #[clap(long, value_parser = parse_duration)]
    client_keep_alive_interval: Option<Duration>,
    /// How often should the gc task run [default: 20s]
    #[clap(long, value_parser = parse_duration)]
    gc_interval: Option<Duration>,
    /// Range request retry timeout [default: 2s]
    #[clap(long, value_parser = parse_duration)]
    range_retry_timeout: Option<Duration>,
    /// Compact timeout [default: 5s]
    #[clap(long, value_parser = parse_duration)]
    compact_timeout: Option<Duration>,
    /// How often should the background task sync victim watchers [default: 10ms]
    #[clap(long,value_parser = parse_duration)]
    sync_victims_interval: Option<Duration>,
    /// How often should watch progress notify send a response [default: 600s]
    #[clap(long, value_parser = parse_duration)]
    watch_progress_notify_interval: Option<Duration>,
    /// Storage engine
    #[clap(long)]
    storage_engine: String,
    /// DB directory
    #[clap(long)]
    data_dir: PathBuf,
    /// Curp directory
    curp_dir: Option<PathBuf>,
    /// Curp command workers count
    #[clap(long, default_value_t = default_cmd_workers())]
    cmd_workers: u8,
    /// The max number of historical versions processed in a single compact operation
    #[clap(long, default_value_t = default_compact_batch_size())]
    compact_batch_size: usize,
    /// Interval between two compaction operations [default: 10ms]
    #[clap(long, value_parser = parse_duration)]
    compact_sleep_interval: Option<Duration>,
    /// Auto compact mode
    #[clap(long)]
    auto_compact_mode: Option<String>,
    /// Auto periodic compact retention
    #[clap(long, value_parser = parse_duration)]
    auto_periodic_retention: Option<Duration>,
    /// Auto revision compact retention
    #[clap(long)]
    auto_revision_retention: Option<i64>,
    /// Initial cluster state
    #[clap(long,value_parser = parse_state)]
    initial_cluster_state: Option<InitialClusterState>,
    /// Quota
    #[clap(long)]
    quota: Option<u64>,
    /// Server ca certificate path, used to verify client certificate
    #[clap(long)]
    peer_ca_cert_path: Option<PathBuf>,
    /// Server certificate path
    #[clap(long)]
    peer_cert_path: Option<PathBuf>,
    /// Server private key path
    #[clap(long)]
    peer_key_path: Option<PathBuf>,
    /// Client ca certificate path, used to verify server certificate
    #[clap(long)]
    client_ca_cert_path: Option<PathBuf>,
    /// Client certificate path
    #[clap(long)]
    client_cert_path: Option<PathBuf>,
    /// Client private key path
    #[clap(long)]
    client_key_path: Option<PathBuf>,
}

#[allow(clippy::too_many_lines)] // will be refactored in #604
impl From<ServerArgs> for XlineServerConfig {
    #[inline]
    #[allow(clippy::too_many_lines)] // not bad
    fn from(args: ServerArgs) -> Self {
        let (engine, curp_engine) = match args.storage_engine.as_str() {
            "memory" => (EngineConfig::Memory, EngineConfig::Memory),
            "rocksdb" => (
                EngineConfig::RocksDB(args.data_dir.clone()),
                EngineConfig::RocksDB(args.curp_dir.unwrap_or_else(|| {
                    let mut path = args.data_dir;
                    path.push("curp");
                    path
                })),
            ),
            &_ => unreachable!("xline only supports memory and rocksdb engine"),
        };

        let storage = StorageConfig::new(engine, args.quota.unwrap_or_else(default_quota));
        let Ok(curp_config) = CurpConfigBuilder::default()
            .heartbeat_interval(
                args.heartbeat_interval
                    .unwrap_or_else(default_heartbeat_interval),
            )
            .wait_synced_timeout(
                args.server_wait_synced_timeout
                    .unwrap_or_else(default_server_wait_synced_timeout),
            )
            .rpc_timeout(args.rpc_timeout.unwrap_or_else(default_rpc_timeout))
            .batch_timeout(args.batch_timeout.unwrap_or_else(default_batch_timeout))
            .batch_max_size(args.batch_max_size.unwrap_or_else(default_batch_max_size))
            .follower_timeout_ticks(args.follower_timeout_ticks)
            .candidate_timeout_ticks(args.candidate_timeout_ticks)
            .engine_cfg(curp_engine)
            .gc_interval(args.gc_interval.unwrap_or_else(default_gc_interval))
            .cmd_workers(args.cmd_workers)
            .build()
        else {
            panic!("failed to create curp config")
        };
        let client_config = ClientConfig::new(
            args.client_wait_synced_timeout
                .unwrap_or_else(default_client_wait_synced_timeout),
            args.client_propose_timeout
                .unwrap_or_else(default_propose_timeout),
            args.client_initial_retry_timeout
                .unwrap_or_else(default_initial_retry_timeout),
            args.client_max_retry_timeout
                .unwrap_or_else(default_max_retry_timeout),
            args.retry_count.unwrap_or_else(default_retry_count),
            args.client_fixed_backoff,
            args.client_keep_alive_interval
                .unwrap_or_else(default_client_id_keep_alive_interval),
        );
        let server_timeout = ServerTimeout::new(
            args.range_retry_timeout
                .unwrap_or_else(default_range_retry_timeout),
            args.compact_timeout.unwrap_or_else(default_compact_timeout),
            args.sync_victims_interval
                .unwrap_or_else(default_sync_victims_interval),
            args.watch_progress_notify_interval
                .unwrap_or_else(default_watch_progress_notify_interval),
        );
        let initial_cluster_state = args.initial_cluster_state.unwrap_or_default();
        let cluster = ClusterConfig::new(
            args.name,
            args.peer_listen_urls,
            args.peer_advertise_urls,
            args.client_listen_urls,
            args.client_advertise_urls,
            args.members,
            args.is_leader,
            curp_config,
            client_config,
            server_timeout,
            initial_cluster_state,
        );
        let log = LogConfig::new(args.log_file, args.log_rotate, args.log_level);
        let trace = TraceConfig::new(args.online, args.offline, args.output_dir, args.level);
        let auth = AuthConfig::new(args.auth_public_key, args.auth_private_key);
        let auto_compactor_cfg = if let Some(mode) = args.auto_compact_mode {
            match mode.as_str() {
                "periodic" => {
                    let period = args.auto_periodic_retention.unwrap_or_else(|| {
                        panic!("missing auto_periodic_retention argument");
                    });
                    Some(AutoCompactConfig::Periodic(period))
                }
                "revision" => {
                    let retention = args.auto_revision_retention.unwrap_or_else(|| {
                        panic!("missing auto_revision_retention argument");
                    });
                    Some(AutoCompactConfig::Revision(retention))
                }
                &_ => unreachable!(
                    "xline only supports two auto-compaction modes: periodic, revision"
                ),
            }
        } else {
            None
        };
        let compact = CompactConfig::new(
            args.compact_batch_size,
            args.compact_sleep_interval
                .unwrap_or_else(default_compact_sleep_interval),
            auto_compactor_cfg,
        );
        let tls = TlsConfig::new(
            args.peer_ca_cert_path,
            args.peer_cert_path,
            args.peer_key_path,
            args.client_ca_cert_path,
            args.client_cert_path,
            args.client_key_path,
        );
        let metrics = MetricsConfig::new(
            args.metrics_enable,
            args.metrics_port,
            args.metrics_path,
            args.metrics_push,
            args.metrics_push_endpoint,
            args.metrics_push_protocol,
        );
        XlineServerConfig::new(cluster, storage, log, trace, auth, compact, tls, metrics)
    }
}

/// Parse config from command line arguments or config file
///
/// # Errors
///
/// Return error if parse failed
#[inline]
pub async fn parse_config() -> Result<XlineServerConfig> {
    let config = if env::args_os().len() == 1 {
        let path = env::var(XLINE_SERVER_CONFIG_ENV)
            .unwrap_or_else(|_| DEFAULT_XLINE_SERVER_CONFIG_PATH.to_owned());
        let config_file = fs::read_to_string(&path)
            .await
            .map_err(|err| ConfigFileError::FileError(path, err))?;
        toml::from_str(&config_file)?
    } else {
        let server_args: ServerArgs = ServerArgs::parse();
        server_args.into()
    };
    validate_server_config(&config)?;
    Ok(config)
}

/// Validate server configuration for common misconfigurations.
///
/// Checks for:
/// - Port conflicts between metrics, client listen, and peer listen ports
/// - TLS configuration completeness (cert+key must both be present)
/// - Advertise URLs using DNS names vs IP addresses
///
/// # Errors
///
/// Returns an error if a fatal misconfiguration is detected (e.g., port conflict).
#[inline]
fn validate_server_config(config: &XlineServerConfig) -> Result<()> {
    use std::collections::HashSet;

    let cluster = config.cluster();
    let tls = config.tls();
    let metrics = config.metrics();
    let name = cluster.name();

    // ── 1. Port conflict detection ───────────────────────────────────────
    let mut all_ports: Vec<(u16, String)> = Vec::new();

    // Extract ports from client listen URLs
    for url in cluster.client_listen_urls() {
        if let Some(port) = extract_port_from_url(url) {
            all_ports.push((port, format!("client-listen-urls ({url})")));
        }
    }
    // Extract ports from peer listen URLs
    for url in cluster.peer_listen_urls() {
        if let Some(port) = extract_port_from_url(url) {
            all_ports.push((port, format!("peer-listen-urls ({url})")));
        }
    }
    // Add metrics port if enabled
    if *metrics.enable() {
        all_ports.push((
            *metrics.port(),
            format!("--metrics-port ({})", metrics.port()),
        ));
    }

    let mut seen: HashSet<u16> = HashSet::new();
    for (port, _source) in &all_ports {
        if !seen.insert(*port) {
            let conflicts: Vec<String> = all_ports
                .iter()
                .filter(|(p, _)| *p == *port)
                .map(|(_, s)| s.clone())
                .collect();
            anyhow::bail!(
                "Port conflict: port {port} is used by multiple sources: {}\n\
                 Hint: Use different ports for client-listen, peer-listen, and metrics.\n\
                 Example: --client-listen-urls https://127.0.0.1:2379 --peer-listen-urls https://127.0.0.1:2380 --metrics-port 9100",
                conflicts.join(", ")
            );
        }
    }

    // ── 2. TLS completeness ──────────────────────────────────────────────
    match (tls.peer_cert_path(), tls.peer_key_path()) {
        (Some(_), None) => {
            anyhow::bail!(
                "TLS misconfiguration for server '{name}': --peer-cert-path is set but --peer-key-path is missing.\n\
                 Hint: Both --peer-cert-path and --peer-key-path must be provided together."
            );
        }
        (None, Some(_)) => {
            anyhow::bail!(
                "TLS misconfiguration for server '{name}': --peer-key-path is set but --peer-cert-path is missing.\n\
                 Hint: Both --peer-cert-path and --peer-key-path must be provided together."
            );
        }
        _ => {}
    }
    match (tls.client_cert_path(), tls.client_key_path()) {
        (Some(_), None) => {
            anyhow::bail!(
                "TLS misconfiguration for server '{name}': --client-cert-path is set but --client-key-path is missing.\n\
                 Hint: Both --client-cert-path and --client-key-path must be provided together."
            );
        }
        (None, Some(_)) => {
            anyhow::bail!(
                "TLS misconfiguration for server '{name}': --client-key-path is set but --client-cert-path is missing.\n\
                 Hint: Both --client-cert-path and --client-key-path must be provided together."
            );
        }
        _ => {}
    }

    // ── 3. Advertise URL DNS vs IP warnings ──────────────────────────────
    for url in cluster.client_advertise_urls() {
        if let Some(host) = extract_host_from_url(url) {
            if host.parse::<std::net::IpAddr>().is_err() {
                tracing::info!(
                    "server '{name}': client-advertise-urls uses DNS name '{host}' — \
                     ensure /etc/hosts or DNS resolves '{host}' to this server's listen address, \
                     and that the TLS certificate includes DNS:{host} in its Subject Alternative Name"
                );
            }
        }
    }
    for url in cluster.peer_advertise_urls() {
        if let Some(host) = extract_host_from_url(url) {
            if host.parse::<std::net::IpAddr>().is_err() {
                tracing::info!(
                    "server '{name}': peer-advertise-urls uses DNS name '{host}' — \
                     ensure /etc/hosts or DNS resolves '{host}' to this server's listen address, \
                     and that the TLS certificate includes DNS:{host} in its Subject Alternative Name"
                );
            }
        }
    }

    // ── 4. Advertise URL without TLS warning ─────────────────────────────
    if !tls.server_tls_enabled() {
        for url in cluster
            .client_advertise_urls()
            .iter()
            .chain(cluster.peer_advertise_urls().iter())
        {
            if url.starts_with("https://") {
                tracing::warn!(
                    "server '{name}': advertise URL '{url}' uses https:// but no TLS certificate is configured.\n\
                     Hint: Provide --peer-cert-path and --peer-key-path, or use http:// for advertise URLs."
                );
            }
        }
    }

    Ok(())
}

/// Extract port number from a URL like "https://host:port" or "http://host:port".
fn extract_port_from_url(url: &str) -> Option<u16> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("quic://"))?;
    // Find the port after the last ':'
    let port_str = rest.rsplit(':').next()?;
    // Strip any trailing path
    let port_str = port_str.split('/').next()?;
    port_str.parse().ok()
}

/// Extract the host portion from a URL string like "https://host:port/path".
fn extract_host_from_url(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("quic://"))?;
    let end = rest
        .find(':')
        .or_else(|| rest.find('/'))
        .unwrap_or(rest.len());
    let host = &rest[..end];
    if host.is_empty() { None } else { Some(host) }
}
