use std::net::{IpAddr, SocketAddr};

/// DNS resolution fallback policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsFallback {
    /// DNS failure is a hard error (production default).
    Disabled,
    /// Fall back to 127.0.0.1 with the original hostname as SNI.
    LocalhostForTest,
}

/// A fully resolved endpoint ready for QUIC connection.
#[derive(Debug, Clone)]
pub struct ResolvedEndpoint {
    /// The server name (hostname without port), used as TLS SNI.
    pub server_name: String,
    /// The resolved socket address to connect to.
    pub socket_addr: SocketAddr,
}

/// Strip `quic://`, `https://`, or `http://` scheme prefix.
#[must_use]
pub fn strip_scheme(endpoint: &str) -> &str {
    endpoint
        .strip_prefix("quic://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .or_else(|| endpoint.strip_prefix("http://"))
        .unwrap_or(endpoint)
}

/// Parse endpoint into (host, port). Handles IPv4, IPv6, DNS formats.
///
/// # Errors
///
/// Returns error if format is invalid or port is missing.
pub fn parse_host_port(endpoint: &str) -> Result<(String, u16), String> {
    let stripped = strip_scheme(endpoint);

    if stripped.starts_with('[') {
        let bracket_end = stripped
            .find(']')
            .ok_or_else(|| format!("missing ']' in IPv6 endpoint: {endpoint}"))?;
        let host = &stripped[1..bracket_end];
        let rest = &stripped[bracket_end + 1..];
        let port_str = rest
            .strip_prefix(':')
            .ok_or_else(|| format!("missing port after ']' in endpoint: {endpoint}"))?;
        let port: u16 = port_str
            .parse()
            .map_err(|e| format!("invalid port in endpoint '{endpoint}': {e}"))?;
        Ok((host.to_string(), port))
    } else {
        let (host, port_str) = stripped
            .rsplit_once(':')
            .ok_or_else(|| format!("missing ':' in endpoint: {endpoint}"))?;
        let port: u16 = port_str
            .parse()
            .map_err(|e| format!("invalid port in endpoint '{endpoint}': {e}"))?;
        Ok((host.to_string(), port))
    }
}

/// Resolve endpoint with DNS lookup (production: DNS failure = error).
///
/// # Errors
///
/// Returns error if DNS resolution fails.
pub async fn resolve_endpoint(endpoint: &str) -> Result<ResolvedEndpoint, String> {
    resolve_endpoint_with_fallback(endpoint, DnsFallback::Disabled).await
}

/// Resolve endpoint with configurable DNS fallback.
///
/// # Errors
///
/// Returns error if DNS resolution fails and fallback is disabled.
pub async fn resolve_endpoint_with_fallback(
    endpoint: &str,
    fallback: DnsFallback,
) -> Result<ResolvedEndpoint, String> {
    let (host, port) = parse_host_port(endpoint)?;

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ResolvedEndpoint {
            server_name: host,
            socket_addr: SocketAddr::new(ip, port),
        });
    }

    let host_lookup = host.clone();
    match tokio::net::lookup_host((host_lookup.as_str(), port)).await {
        Ok(mut addrs) => {
            let addr = addrs
                .next()
                .ok_or_else(|| format!("DNS lookup returned no addresses for '{host}:{port}'"))?;
            Ok(ResolvedEndpoint {
                server_name: host,
                socket_addr: addr,
            })
        }
        Err(dns_err) => match fallback {
            DnsFallback::Disabled => Err(format!(
                "DNS lookup failed for '{host}:{port}': {dns_err}"
            )),
            DnsFallback::LocalhostForTest => {
                let fallback_addr = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port);
                tracing::warn!(
                    "DNS lookup failed for '{host}:{port}' ({dns_err}), \
                     falling back to {fallback_addr} (test mode)"
                );
                Ok(ResolvedEndpoint {
                    server_name: host,
                    socket_addr: fallback_addr,
                })
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_scheme() {
        assert_eq!(strip_scheme("http://server0:2379"), "server0:2379");
        assert_eq!(strip_scheme("https://server0:2379"), "server0:2379");
        assert_eq!(strip_scheme("quic://server0:2379"), "server0:2379");
        assert_eq!(strip_scheme("server0:2379"), "server0:2379");
        assert_eq!(strip_scheme("127.0.0.1:2379"), "127.0.0.1:2379");
    }

    #[test]
    fn test_parse_host_port_ipv4() {
        let (host, port) = parse_host_port("127.0.0.1:2379").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 2379);
    }

    #[test]
    fn test_parse_host_port_ipv6() {
        let (host, port) = parse_host_port("[::1]:2379").unwrap();
        assert_eq!(host, "::1");
        assert_eq!(port, 2379);
    }

    #[test]
    fn test_parse_host_port_dns() {
        let (host, port) = parse_host_port("http://server0:2379").unwrap();
        assert_eq!(host, "server0");
        assert_eq!(port, 2379);
    }

    #[test]
    fn test_parse_host_port_no_port() {
        assert!(parse_host_port("server0").is_err());
    }

    #[tokio::test]
    async fn test_resolve_ip_endpoint() {
        let ep = resolve_endpoint("127.0.0.1:2379").await.unwrap();
        assert_eq!(ep.server_name, "127.0.0.1");
        assert_eq!(ep.socket_addr, "127.0.0.1:2379".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    async fn test_resolve_localhost() {
        let ep = resolve_endpoint("localhost:2379").await.unwrap();
        assert_eq!(ep.server_name, "localhost");
        assert_eq!(ep.socket_addr.port(), 2379);
    }
}
