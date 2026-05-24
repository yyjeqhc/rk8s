use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgMatches, Command, arg};

/// Result of checking a single endpoint's syntax and semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EndpointCheck {
    /// No scheme (https:// or http://) present.
    MissingScheme,
    /// Scheme is not https:// or http://.
    UnknownScheme(String),
    /// No port number after the host.
    MissingPort,
    /// Port is not a valid u16.
    InvalidPort,
    /// Host is an IP address (not supported by QUIC SNI routing).
    IpEndpoint,
    /// Host is "localhost" (not supported by QUIC SNI routing).
    LocalhostEndpoint,
    /// Endpoint parsed and DNS resolved successfully.
    /// The `scheme` field is "https" or "http".
    Ok {
        scheme: String,
        host: String,
        port: u16,
    },
}

/// Result of checking a CA certificate file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CaStatus {
    /// File exists and is non-empty.
    Found(u64),
    /// File exists but is empty.
    Empty,
    /// File does not exist.
    NotFound,
    /// Cannot read file metadata.
    MetadataError(String),
}

/// Pure parsing: extract scheme, host, port from an endpoint string.
/// Returns `EndpointCheck::Ok` on success, or an error variant.
fn parse_endpoint_parts(ep: &str) -> EndpointCheck {
    if !ep.contains("://") {
        return EndpointCheck::MissingScheme;
    }

    let is_https = ep.starts_with("https://");
    let is_http = ep.starts_with("http://");

    if !is_https && !is_http {
        let scheme = ep.split("://").next().unwrap_or(ep).to_string();
        return EndpointCheck::UnknownScheme(scheme);
    }

    let stripped = ep
        .strip_prefix("https://")
        .or_else(|| ep.strip_prefix("http://"))
        .unwrap_or(ep);

    if !stripped.contains(':') {
        return EndpointCheck::MissingPort;
    }

    let host = stripped
        .rsplit_once(':')
        .map(|(h, _)| h.trim_start_matches('[').trim_end_matches(']'))
        .unwrap_or(stripped);

    let port = match stripped
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
    {
        Some(p) => p,
        None => return EndpointCheck::InvalidPort,
    };

    if host.parse::<std::net::IpAddr>().is_ok() {
        return EndpointCheck::IpEndpoint;
    }
    if host == "localhost" {
        return EndpointCheck::LocalhostEndpoint;
    }

    let scheme = if is_https { "https" } else { "http" }.to_string();
    EndpointCheck::Ok {
        scheme,
        host: host.to_string(),
        port,
    }
}

/// Pure host classification: returns true if host is IP or localhost.
fn is_ip_or_localhost(host: &str) -> bool {
    host.parse::<std::net::IpAddr>().is_ok() || host == "localhost"
}

/// Pure DNS check: try resolving host:port. Returns Ok(()) or Err(message).
fn check_dns(host: &str, port: u16) -> Result<(), String> {
    let addr_str = format!("{host}:{port}");
    match std::net::ToSocketAddrs::to_socket_addrs(&addr_str) {
        Ok(mut addrs) => {
            if addrs.next().is_some() {
                Ok(())
            } else {
                Err("DNS lookup returned no addresses".to_string())
            }
        }
        Err(e) => Err(format!("{e}")),
    }
}

/// Pure file check: returns CA status without side effects.
fn check_ca_path_status(path: &std::path::Path) -> CaStatus {
    if !path.exists() {
        return CaStatus::NotFound;
    }
    match std::fs::metadata(path) {
        Ok(meta) => {
            if meta.len() == 0 {
                CaStatus::Empty
            } else {
                CaStatus::Found(meta.len())
            }
        }
        Err(e) => CaStatus::MetadataError(e.to_string()),
    }
}

pub(crate) fn command() -> Command {
    Command::new("doctor")
        .about("Diagnose endpoint, TLS, DNS, and connection issues")
        .arg(
            arg!(--check_connection "Attempt to connect to the cluster (slow)")
                .required(false)
                .action(clap::ArgAction::SetTrue),
        )
}

pub(crate) async fn execute(
    matches: &ArgMatches,
    endpoints: Vec<String>,
    ca_path: Option<PathBuf>,
    curp_cache_cli_flag: bool,
) -> Result<()> {
    let check_connection = matches.get_flag("check_connection");

    let mut critical = 0_u32;
    let mut warnings = 0_u32;

    println!("========================================");
    println!("  xlinectl doctor");
    println!("========================================");
    println!();

    println!("── Endpoint Checks ──");
    if endpoints.is_empty() {
        println!("  ❌ No endpoints provided");
        println!("     Hint: Use --endpoints https://server0:2379");
        critical += 1;
    } else {
        for ep in &endpoints {
            check_endpoint(ep, &mut critical, &mut warnings);
        }
    }
    println!();

    println!("── TLS Checks ──");
    check_tls(&ca_path, &mut critical, &mut warnings);
    println!();

    println!("── SNI Routing Checks ──");
    check_sni_routing(&endpoints, &mut warnings);
    println!();

    println!("── Experimental Features ──");
    check_experimental_features(&mut warnings, curp_cache_cli_flag);
    println!();

    if check_connection {
        println!("── Connection Check ──");
        if critical == 0 {
            check_connection_async(&endpoints, &ca_path, &mut critical).await;
        } else {
            println!("  ⏭️  Skipped because critical static checks failed");
            println!("     Fix the errors above, then rerun with --check_connection.");
        }
        println!();
    }

    println!("========================================");
    if critical == 0 {
        println!("  ✅ All critical checks passed");
        if warnings > 0 {
            println!("  ⚠️  {warnings} warning(s) — see above");
        }
        println!("========================================");
        Ok(())
    } else {
        println!("  ❌ {critical} critical failure(s)");
        if warnings > 0 {
            println!("  ⚠️  {warnings} warning(s)");
        }
        println!("========================================");
        anyhow::bail!("{critical} critical check(s) failed")
    }
}

fn check_endpoint(ep: &str, critical: &mut u32, warnings: &mut u32) {
    let check = parse_endpoint_parts(ep);
    match &check {
        EndpointCheck::MissingScheme => {
            println!("  ❌ {ep}");
            println!("     Missing scheme (https:// or http://)");
            *critical += 1;
            return;
        }
        EndpointCheck::UnknownScheme(_) => {
            println!("  ❌ {ep}");
            println!("     Unknown scheme — expected https:// or http://");
            *critical += 1;
            return;
        }
        EndpointCheck::Ok { scheme, .. } if scheme == "http" => {
            println!("  ⚠️  {ep}");
            println!("     Uses http:// (plaintext) — traffic is not encrypted");
            *warnings += 1;
        }
        EndpointCheck::Ok { .. } => {
            println!("  ✅ {ep}");
        }
        EndpointCheck::MissingPort
        | EndpointCheck::InvalidPort
        | EndpointCheck::IpEndpoint
        | EndpointCheck::LocalhostEndpoint => {}
    }

    match &check {
        EndpointCheck::MissingPort => {
            println!("     ❌ Missing port number");
            println!("        Hint: Use 'host:port' format, e.g., 'https://server0:2379'");
            *critical += 1;
            return;
        }
        EndpointCheck::InvalidPort => {
            println!("     ❌ Invalid port number");
            *critical += 1;
            return;
        }
        _ => {}
    }

    if let EndpointCheck::IpEndpoint = &check {
        println!("     ❌ IP address endpoint — not supported by QUIC SNI routing");
        println!("        Use DNS server names instead (e.g., server0, server1, server2)");
        println!(
            "        Map in /etc/hosts: echo '127.0.0.1 server0 server1 server2' >> /etc/hosts"
        );
        *critical += 1;
        return;
    }
    if let EndpointCheck::LocalhostEndpoint = &check {
        println!("     ❌ 'localhost' endpoint — not supported by QUIC SNI routing");
        println!("        Use DNS server names instead (e.g., server0, server1, server2)");
        *critical += 1;
        return;
    }

    if let EndpointCheck::Ok { host, port, .. } = &check {
        match check_dns(host, *port) {
            Ok(()) => {
                let addr_str = format!("{host}:{port}");
                if let Ok(mut addrs) = std::net::ToSocketAddrs::to_socket_addrs(&addr_str) {
                    if let Some(addr) = addrs.next() {
                        println!("     ✅ DNS resolves to {addr}");
                    }
                }
            }
            Err(e) => {
                println!("     ❌ DNS lookup failed: {e}");
                println!("        Hint: Check /etc/hosts or DNS for '{host}'");
                *critical += 1;
            }
        }
    }
}

fn check_tls(ca_path: &Option<PathBuf>, critical: &mut u32, warnings: &mut u32) {
    match ca_path {
        Some(path) => match check_ca_path_status(path) {
            CaStatus::Found(len) => {
                println!("  ✅ CA file exists: {} ({len} bytes)", path.display());
            }
            CaStatus::Empty => {
                println!("  ❌ CA file is empty: {}", path.display());
                *critical += 1;
            }
            CaStatus::NotFound => {
                println!("  ❌ CA file not found: {}", path.display());
                println!("     Hint: Provide a valid path with --ca_cert_pem_path");
                *critical += 1;
            }
            CaStatus::MetadataError(e) => {
                println!("  ❌ Cannot read CA file: {e}");
                *critical += 1;
            }
        },
        None => {
            let default_ca =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/ca.crt");
            if default_ca.exists() {
                println!(
                    "  ✅ Using development fixture CA: {}",
                    default_ca.display()
                );
            } else {
                println!("  ⚠️  No CA certificate configured");
                println!(
                    "     An empty RootCertStore will be used — all connections fail with UnknownIssuer"
                );
                println!("     Hint: Use --ca_cert_pem_path <PATH> to specify the CA certificate");
                *warnings += 1;
            }
        }
    }
}

fn endpoints_have_ip_or_localhost(endpoints: &[String]) -> bool {
    endpoints.iter().any(|ep| {
        let stripped = ep
            .strip_prefix("https://")
            .or_else(|| ep.strip_prefix("http://"))
            .unwrap_or(ep);
        let host = stripped
            .rsplit_once(':')
            .map(|(h, _)| h.trim_start_matches('[').trim_end_matches(']'))
            .unwrap_or(stripped);
        is_ip_or_localhost(host)
    })
}

fn check_sni_routing(endpoints: &[String], warnings: &mut u32) {
    if endpoints_have_ip_or_localhost(endpoints) {
        println!("  ⚠️  IP/localhost endpoints detected — QUIC SNI routing will fail");
        println!("     Xline uses the endpoint hostname as TLS SNI to route connections.");
        println!("     IP addresses can only be registered by one server.");
        println!("     Use DNS names (server0, server1, server2) with /etc/hosts mapping.");
        *warnings += 1;
    } else {
        println!("  ✅ All endpoints use DNS names (compatible with SNI routing)");
    }
}

fn curp_cache_enabled_from_env_value(value: Option<&str>) -> bool {
    matches!(value, Some("1") | Some("true"))
}

fn cache_source_label(env_enabled: bool, cli_flag: bool) -> Option<&'static str> {
    match (env_enabled, cli_flag) {
        (true, true) => Some("env + CLI flag"),
        (true, false) => Some("env var"),
        (false, true) => Some("CLI flag"),
        (false, false) => None,
    }
}

fn rust_log_enabled_from_env_value(value: Option<&str>) -> bool {
    value.is_some()
}

fn check_experimental_features(warnings: &mut u32, curp_cache_cli_flag: bool) {
    let cache_var = std::env::var("XLINE_CURP_CONN_CACHE").ok();
    let env_enabled = curp_cache_enabled_from_env_value(cache_var.as_deref());
    let effective = env_enabled || curp_cache_cli_flag;
    if effective {
        let source = cache_source_label(env_enabled, curp_cache_cli_flag)
            .expect("at least one source is enabled");
        println!("  ℹ️  CURP connection cache enabled ({source}) — experimental");
    } else {
        println!(
            "  ℹ️  CURP connection cache disabled (use --experimental-curp-connection-cache or XLINE_CURP_CONN_CACHE=1 to enable)"
        );
    }

    let rust_log = std::env::var("RUST_LOG").ok();
    if rust_log_enabled_from_env_value(rust_log.as_deref()) {
        println!(
            "  ℹ️  RUST_LOG={} (debug logging enabled)",
            rust_log.unwrap()
        );
        *warnings += 1;
    }
}

async fn check_connection_async(
    endpoints: &[String],
    ca_path: &Option<PathBuf>,
    critical: &mut u32,
) {
    use xline_client::{Client, ClientOptions};
    use xlinerpc::QuicTlsConfig;

    if endpoints.is_empty() {
        println!("  ⏭️  Skipped (no endpoints)");
        return;
    }

    let quic_tls = match ca_path {
        Some(path) => match std::fs::read(path) {
            Ok(ca_pem) => Some(QuicTlsConfig::default().with_peer_ca_cert_pem(ca_pem)),
            Err(e) => {
                println!("  ❌ Cannot read CA file: {e}");
                *critical += 1;
                return;
            }
        },
        None => {
            let default_ca =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/ca.crt");
            if default_ca.exists() {
                match std::fs::read(&default_ca) {
                    Ok(ca_pem) => Some(QuicTlsConfig::default().with_peer_ca_cert_pem(ca_pem)),
                    Err(e) => {
                        println!("  ❌ Cannot read default CA: {e}");
                        *critical += 1;
                        return;
                    }
                }
            } else {
                println!("  ⚠️  No CA configured — connection will likely fail with UnknownIssuer");
                None
            }
        }
    };

    let options = ClientOptions::new(None, quic_tls, Default::default());

    println!("  Connecting to {:?}...", endpoints);
    match Client::connect(endpoints, options).await {
        Ok(_client) => {
            println!("  ✅ Connection successful");
        }
        Err(e) => {
            println!("  ❌ Connection failed: {e}");
            println!("     Troubleshooting:");
            println!("     1. Verify endpoints are reachable: ping <host>, nc -zu <host> <port>");
            println!("     2. Check /etc/hosts: grep server0 /etc/hosts");
            println!("     3. Verify CA certificate: openssl x509 -in <ca.pem> -noout -subject");
            println!(
                "     4. Check server TLS SANs: openssl x509 -in <server.crt> -noout -ext subjectAltName"
            );
            println!("     5. Enable debug logs: RUST_LOG=xlinerpc=debug xlinectl doctor ...");
            *critical += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_missing_scheme_is_critical() {
        assert_eq!(
            parse_endpoint_parts("server0:2379"),
            EndpointCheck::MissingScheme
        );
    }

    #[test]
    fn endpoint_missing_port_is_critical() {
        assert_eq!(
            parse_endpoint_parts("https://server0"),
            EndpointCheck::MissingPort
        );
    }

    #[test]
    fn endpoint_unknown_scheme_is_critical() {
        assert_eq!(
            parse_endpoint_parts("ftp://server0:2379"),
            EndpointCheck::UnknownScheme("ftp".to_string())
        );
    }

    #[test]
    fn endpoint_https_dns_is_ok() {
        assert_eq!(
            parse_endpoint_parts("https://server0:2379"),
            EndpointCheck::Ok {
                scheme: "https".to_string(),
                host: "server0".to_string(),
                port: 2379
            }
        );
    }

    #[test]
    fn endpoint_ipv4_is_critical() {
        assert_eq!(
            parse_endpoint_parts("https://127.0.0.1:2379"),
            EndpointCheck::IpEndpoint
        );
    }

    #[test]
    fn endpoint_ipv6_is_critical() {
        assert_eq!(
            parse_endpoint_parts("https://[::1]:2379"),
            EndpointCheck::IpEndpoint
        );
    }

    #[test]
    fn endpoint_localhost_is_critical() {
        assert_eq!(
            parse_endpoint_parts("https://localhost:2379"),
            EndpointCheck::LocalhostEndpoint
        );
    }

    #[test]
    fn endpoint_invalid_port_is_critical() {
        assert_eq!(
            parse_endpoint_parts("https://server0:99999"),
            EndpointCheck::InvalidPort
        );
    }

    #[test]
    fn endpoint_http_is_ok_with_scheme_field() {
        assert_eq!(
            parse_endpoint_parts("http://server0:2379"),
            EndpointCheck::Ok {
                scheme: "http".to_string(),
                host: "server0".to_string(),
                port: 2379
            }
        );
    }

    #[test]
    fn classify_host_ip() {
        assert!(is_ip_or_localhost("127.0.0.1"));
        assert!(is_ip_or_localhost("::1"));
        assert!(is_ip_or_localhost("10.0.0.1"));
    }

    #[test]
    fn classify_host_localhost() {
        assert!(is_ip_or_localhost("localhost"));
    }

    #[test]
    fn classify_host_dns_name() {
        assert!(!is_ip_or_localhost("server0"));
        assert!(!is_ip_or_localhost("example.com"));
    }

    #[test]
    fn ca_missing_file_is_not_found() {
        let path = std::path::Path::new("/nonexistent/path/ca.crt");
        assert_eq!(check_ca_path_status(path), CaStatus::NotFound);
    }

    #[test]
    fn ca_empty_file_is_empty() {
        let dir = std::env::temp_dir().join(format!("xlinectl_doctor_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("empty_ca.crt");
        std::fs::write(&path, "").unwrap();
        assert_eq!(check_ca_path_status(&path), CaStatus::Empty);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn ca_nonempty_file_is_found() {
        let dir = std::env::temp_dir().join(format!("xlinectl_doctor_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("ca.crt");
        std::fs::write(&path, "fake cert data").unwrap();
        assert_eq!(check_ca_path_status(&path), CaStatus::Found(14));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn sni_routing_detects_ip_endpoint() {
        let eps = vec!["https://127.0.0.1:2379".to_string()];
        assert!(endpoints_have_ip_or_localhost(&eps));
    }

    #[test]
    fn sni_routing_detects_localhost_endpoint() {
        let eps = vec!["https://localhost:2379".to_string()];
        assert!(endpoints_have_ip_or_localhost(&eps));
    }

    #[test]
    fn sni_routing_ok_with_dns_names() {
        let eps = vec![
            "https://server0:2379".to_string(),
            "https://server1:2379".to_string(),
        ];
        assert!(!endpoints_have_ip_or_localhost(&eps));
    }

    #[test]
    fn experimental_cache_enabled() {
        assert!(curp_cache_enabled_from_env_value(Some("1")));
        assert!(curp_cache_enabled_from_env_value(Some("true")));
    }

    #[test]
    fn experimental_cache_disabled() {
        assert!(!curp_cache_enabled_from_env_value(None));
        assert!(!curp_cache_enabled_from_env_value(Some("0")));
        assert!(!curp_cache_enabled_from_env_value(Some("false")));
    }

    #[test]
    fn cache_source_label_disabled() {
        assert_eq!(cache_source_label(false, false), None);
    }

    #[test]
    fn cache_source_label_env_only() {
        assert_eq!(cache_source_label(true, false), Some("env var"));
    }

    #[test]
    fn cache_source_label_cli_only() {
        assert_eq!(cache_source_label(false, true), Some("CLI flag"));
    }

    #[test]
    fn cache_source_label_both() {
        assert_eq!(cache_source_label(true, true), Some("env + CLI flag"));
    }

    #[test]
    fn experimental_rust_log_enabled() {
        assert!(rust_log_enabled_from_env_value(Some("debug")));
        assert!(rust_log_enabled_from_env_value(Some("xlinerpc=info")));
    }

    #[test]
    fn experimental_rust_log_disabled() {
        assert!(!rust_log_enabled_from_env_value(None));
    }
}
