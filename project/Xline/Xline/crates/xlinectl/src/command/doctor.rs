use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgMatches, Command, arg};

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
    check_experimental_features(&mut warnings);
    println!();

    if check_connection {
        println!("── Connection Check ──");
        check_connection_async(&endpoints, &ca_path, &mut critical).await;
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
    if !ep.contains("://") {
        println!("  ❌ {ep}");
        println!("     Missing scheme (https:// or http://)");
        *critical += 1;
        return;
    }

    let is_https = ep.starts_with("https://");
    let is_http = ep.starts_with("http://");

    if is_http {
        println!("  ⚠️  {ep}");
        println!("     Uses http:// (plaintext) — traffic is not encrypted");
        *warnings += 1;
    } else if is_https {
        println!("  ✅ {ep}");
    } else {
        println!("  ❌ {ep}");
        println!("     Unknown scheme — expected https:// or http://");
        *critical += 1;
        return;
    }

    let stripped = ep
        .strip_prefix("https://")
        .or_else(|| ep.strip_prefix("http://"))
        .or_else(|| ep.strip_prefix("quic://"))
        .unwrap_or(ep);

    if !stripped.contains(':') {
        println!("     ❌ Missing port number");
        println!("        Hint: Use 'host:port' format, e.g., 'https://server0:2379'");
        *critical += 1;
        return;
    }

    let host = stripped
        .rsplit_once(':')
        .map(|(h, _)| h.trim_start_matches('[').trim_end_matches(']'))
        .unwrap_or(stripped);

    let port = stripped
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok());

    if port.is_none() {
        println!("     ❌ Invalid port number");
        *critical += 1;
        return;
    }

    if host.parse::<std::net::IpAddr>().is_ok() {
        println!("     ❌ IP address endpoint — not supported by QUIC SNI routing");
        println!("        Use DNS server names instead (e.g., server0, server1, server2)");
        println!(
            "        Map in /etc/hosts: echo '127.0.0.1 server0 server1 server2' >> /etc/hosts"
        );
        *critical += 1;
    } else if host == "localhost" {
        println!("     ❌ 'localhost' endpoint — not supported by QUIC SNI routing");
        println!("        Use DNS server names instead (e.g., server0, server1, server2)");
        *critical += 1;
    }

    let addr_str = format!("{host}:{}", port.unwrap_or(0));
    match std::net::ToSocketAddrs::to_socket_addrs(&addr_str) {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                println!("     ✅ DNS resolves to {addr}");
            } else {
                println!("     ⚠️  DNS lookup returned no addresses");
                *warnings += 1;
            }
        }
        Err(e) => {
            println!("     ❌ DNS lookup failed: {e}");
            println!("        Hint: Check /etc/hosts or DNS for '{host}'");
            *critical += 1;
        }
    }
}

fn check_tls(ca_path: &Option<PathBuf>, critical: &mut u32, warnings: &mut u32) {
    match ca_path {
        Some(path) => {
            if path.exists() {
                match std::fs::metadata(path) {
                    Ok(meta) => {
                        if meta.len() == 0 {
                            println!("  ❌ CA file is empty: {}", path.display());
                            *critical += 1;
                        } else {
                            println!(
                                "  ✅ CA file exists: {} ({} bytes)",
                                path.display(),
                                meta.len()
                            );
                        }
                    }
                    Err(e) => {
                        println!("  ❌ Cannot read CA file: {e}");
                        *critical += 1;
                    }
                }
            } else {
                println!("  ❌ CA file not found: {}", path.display());
                println!("     Hint: Provide a valid path with --ca_cert_pem_path");
                *critical += 1;
            }
        }
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

fn check_sni_routing(endpoints: &[String], warnings: &mut u32) {
    let has_ip_or_localhost = endpoints.iter().any(|ep| {
        let stripped = ep
            .strip_prefix("https://")
            .or_else(|| ep.strip_prefix("http://"))
            .unwrap_or(ep);
        let host = stripped
            .rsplit_once(':')
            .map(|(h, _)| h.trim_start_matches('[').trim_end_matches(']'))
            .unwrap_or(stripped);
        host.parse::<std::net::IpAddr>().is_ok() || host == "localhost"
    });

    if has_ip_or_localhost {
        println!("  ⚠️  IP/localhost endpoints detected — QUIC SNI routing will fail");
        println!("     Xline uses the endpoint hostname as TLS SNI to route connections.");
        println!("     IP addresses can only be registered by one server.");
        println!("     Use DNS names (server0, server1, server2) with /etc/hosts mapping.");
        *warnings += 1;
    } else {
        println!("  ✅ All endpoints use DNS names (compatible with SNI routing)");
    }
}

fn check_experimental_features(warnings: &mut u32) {
    let cache_var = std::env::var("XLINE_CURP_CONN_CACHE");
    match cache_var.as_deref() {
        Ok("1") | Ok("true") => {
            println!("  ℹ️  XLINE_CURP_CONN_CACHE=1 (CURP connection cache enabled)");
        }
        _ => {
            println!("  ℹ️  XLINE_CURP_CONN_CACHE not set (CURP connection cache disabled)");
        }
    }

    if let Ok(val) = std::env::var("RUST_LOG") {
        println!("  ℹ️  RUST_LOG={val} (debug logging enabled)");
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
