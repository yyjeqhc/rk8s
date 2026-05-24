# QUIC/H3 Troubleshooting Guide

This guide is the entry point for diagnosing QUIC/H3 cluster setup, client connection,
TLS, SNI, metrics, and deployment issues. It connects all QUIC-related documentation
and provides actionable troubleshooting steps.

## Table of Contents

1. [Quick Start: Local 3-Node QUIC Cluster](#1-quick-start-local-3-node-quic-cluster)
2. [First Diagnostic Command: xlinectl doctor](#2-first-diagnostic-command-xlinectl-doctor)
3. [Common Failure Matrix](#3-common-failure-matrix)
4. [TLS / CA Policy](#4-tls--ca-policy)
5. [SNI Routing and IP Endpoints](#5-sni-routing-and-ip-endpoints)
6. [Metrics and Debug Logs](#6-metrics-and-debug-logs)
7. [CURP Connection Cache](#7-curp-connection-cache)
8. [Benchmark Docs](#8-benchmark-docs)
9. [Related Docs Map](#9-related-docs-map)

---

## 1. Quick Start: Local 3-Node QUIC Cluster

### /etc/hosts

Xline uses QUIC SNI routing, which requires DNS hostnames — not IP addresses.
Add these entries to `/etc/hosts`:

```text
127.0.0.1 server0 server1 server2
```

> **Why not `127.0.0.1` or `localhost`?** QUIC SNI routing maps connections by hostname.
> When multiple servers bind to the same IP, only one can win the SNI slot.
> See [§5 SNI Routing and IP Endpoints](#5-sni-routing-and-ip-endpoints).

### Start a 3-node cluster

```bash
# Option A: smoke test script (builds + starts + verifies + cleans up)
scripts/quic_ci_smoke.sh

# Option B: manual start (cluster stays running)
scripts/quic_local_cluster.sh
```

### xlinectl examples

```bash
# Doctor — diagnose config before connecting
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt doctor

# Put / Get
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt put foo bar
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt get foo

# Delete
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt delete foo

# Member list
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt member list

# Watch (Ctrl-C to stop)
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt watch foo
```

> **Dev builds**: In development builds, `fixtures/ca.crt` is discovered automatically
> via `CARGO_MANIFEST_DIR`. You can omit `--ca_cert_pem_path` if running from the
> project directory. In production, always pass it explicitly.

---

## 2. First Diagnostic Command: xlinectl doctor

**Always start here when something doesn't work.**

```bash
# Basic diagnostic
xlinectl doctor

# With explicit config
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt doctor

# Include live connection test (requires running cluster)
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt doctor --check_connection
```

### What doctor checks (14 checks)

| Check | Critical? | Description |
|-------|-----------|-------------|
| Scheme presence | Yes | Endpoint must start with `https://` or `http://` |
| Unknown scheme | Yes | Only `https://` and `http://` are recognized |
| Port presence | Yes | Endpoint must include `:port` |
| Plaintext `http://` | Warning | Traffic is not encrypted |
| IP address endpoint | Yes | QUIC SNI routing requires DNS names, not IPs |
| `localhost` endpoint | Yes | Same as IP — SNI routing fails |
| DNS resolution | Yes | Hostname must resolve to an address |
| CA file exists | Yes | File must exist and be readable |
| CA file non-empty | Yes | Zero-byte CA file is rejected |
| Development fixture CA | Info | In dev builds, `fixtures/ca.crt` is used automatically |
| SNI routing compatibility | Warning | IP/localhost endpoints break SNI routing |
| `XLINE_CURP_CONN_CACHE` | Info | Reports if CURP connection cache is enabled |
| `RUST_LOG` | Warning | Reports if debug logging is active |
| Connection check | Yes (optional) | Attempts a real QUIC connection to the cluster |

### What doctor does NOT do

- Does **not** modify `/etc/hosts`
- Does **not** start or stop servers
- Does **not** enable experimental features
- Does **not** validate data directory contents
- Does **not** check cluster membership or leader status

### When to use `--check_connection`

Use `--check_connection` when static checks pass but you still can't connect.
It attempts a real QUIC handshake to verify TLS, SNI, and network connectivity.

If static checks already have critical failures, `--check_connection` is skipped
automatically — fix the static errors first.

See [xlinectl doctor docs](xlinectl-doctor.md) for full details.

---

## 3. Common Failure Matrix

### Endpoint Errors

| Symptom | Likely Cause | How to Verify | Fix |
|---------|-------------|---------------|-----|
| `Missing scheme (https:// or http://)` | Endpoint passed without scheme | Run `xlinectl doctor` | Prefix with `https://` |
| `Missing port number` | Endpoint passed without port | Run `xlinectl doctor` | Append `:2379` (or appropriate port) |
| `Unknown scheme: ftp://` | Unsupported scheme | Run `xlinectl doctor` | Use `https://` |
| `IP address endpoint — not supported` | Used `127.0.0.1` or other IP | Run `xlinectl doctor` | Use DNS name (`server0`) |
| `'localhost' endpoint — not supported` | Used `localhost` | Run `xlinectl doctor` | Use DNS name (`server0`) |

### DNS Errors

| Symptom | Likely Cause | How to Verify | Fix |
|---------|-------------|---------------|-----|
| `DNS lookup failed for 'server0'` | Hostname not in /etc/hosts | `grep server0 /etc/hosts` | Add `127.0.0.1 server0 server1 server2` to `/etc/hosts` |
| `DNS lookup failed for 'nonexistent'` | Typo or wrong hostname | `getent hosts nonexistent` | Fix hostname or add to `/etc/hosts` |

### TLS Errors

| Symptom | Likely Cause | How to Verify | Fix |
|---------|-------------|---------------|-----|
| `UnknownIssuer` | Wrong or missing CA cert | Check `--ca_cert_pem_path` points to correct CA | Use `fixtures/ca.crt` for dev, or your CA for prod |
| `CA file is empty` | Zero-byte CA file | `wc -c < /path/to/ca.crt` | Provide valid CA certificate |
| `No CA certificate configured` (warning) | No `--ca_cert_pem_path` and no dev fixture | Check if `fixtures/ca.crt` exists | Pass `--ca_cert_pem_path` explicitly |

### QUIC / Connection Errors

| Symptom | Likely Cause | How to Verify | Fix |
|---------|-------------|---------------|-----|
| `No viable network path` | SNI routing failure (IP endpoint) | Run `xlinectl doctor` | Use DNS names, not IPs |
| `QUIC handshake timeout` | Server not running or wrong port | `pgrep xline`, check ports | Start cluster, verify port in endpoint |
| `port already in use` | Previous xline process still running | `lsof -i :2379` | Kill old process or use `scripts/quic_local_cluster.sh` (auto-cleans) |
| `Connection refused` | Server not listening on that address | `ss -tlnp \| grep 2379` | Start server or check listen address |

### Metrics Errors

| Symptom | Likely Cause | How to Verify | Fix |
|---------|-------------|---------------|-----|
| Metrics endpoint not reachable | Server not started or wrong port | `curl http://server0:9100/metrics` | Start server with `--metrics-enable --metrics-port 9100` |
| `quic_connect_attempts_total` not showing | No QUIC connections yet | Check if cluster is healthy | Normal for idle cluster |
| CURP cache metrics not in Prometheus | xlinectl has no metrics endpoint | Expected behavior | Use `RUST_LOG=curp::rpc::quic_transport::channel=debug` for client cache stats |

---

## 4. TLS / CA Policy

Xline does **not** silently skip TLS verification:

- If no CA certificate is configured and the development fixture CA is not found,
  an empty `RootCertStore` is used. All connections fail with `UnknownIssuer`.
- There is no `--insecure` flag. TLS verification is always enforced.
- The development fixture CA (`fixtures/ca.crt`) is only auto-discovered in dev builds.

### Certificate chain (development)

```
fixtures/ca.crt              ← self-signed CA (development only)
fixtures/server0.crt         ← signed by ca.crt, SAN: DNS:server0, DNS:localhost, IP:127.0.0.1
fixtures/server1.crt         ← signed by ca.crt, SAN: DNS:server1, DNS:localhost, IP:127.0.0.1
fixtures/server2.crt         ← signed by ca.crt, SAN: DNS:server2, DNS:localhost, IP:127.0.0.1
```

### Production recommendations

- Generate your own CA and server certificates
- Always pass `--ca_cert_pem_path` explicitly
- Server certs should include DNS SANs for all hostnames clients will use
- Do not rely on auto-discovery of `fixtures/ca.crt`

---

## 5. SNI Routing and IP Endpoints

QUIC SNI routing maps incoming connections to servers by the TLS SNI hostname.
This means:

- **DNS hostnames work**: `server0`, `server1`, `server2` each map to one server
- **IP addresses don't work for multi-node**: All servers bind to `127.0.0.1`, so only
  one server can register the `127.0.0.1` SNI slot. Connections to other servers fail
  with `No viable network path`.
- **`localhost` doesn't work**: Same reason — resolves to a shared IP.

### Why this happens

Each Xline server registers its `server_name` (from CLI args) as an SNI alias.
When multiple servers share the same IP, only the first to register wins the SNI slot.
The QUIC server then routes by `(sni_name, local_port)` — if the SNI maps to the wrong
server, the connection is refused.

### Recommended setup

```text
# /etc/hosts
127.0.0.1 server0 server1 server2
```

Then use `https://server0:2379`, `https://server1:2381`, `https://server2:2383`.

For full analysis, see [QUIC Refactor Audit §26](quic-refactor-audit.md).

---

## 6. Metrics and Debug Logs

### Server-side metrics (Prometheus)

Xline servers export metrics at `--metrics-port` (default: 9100) at `/metrics`:

```bash
curl http://server0:9100/metrics
```

#### QUIC/H3 transport metrics

| Metric | Type | Description |
|--------|------|-------------|
| `port_router_unknown_total` | Counter | Connections routed to Unknown target (server_name or port mismatch) |
| `quic_connect_attempts_total` | Counter | Server's QUIC connections to peer servers (CURP consensus) |
| `quic_connect_failures_total` | Counter | Server's failed QUIC connections to all peers |
| `curp_conn_cache_hits_total` | Counter | CURP connection cache hits (server-side, when cache enabled) |
| `curp_conn_cache_misses_total` | Counter | CURP connection cache misses (server-side) |
| `curp_conn_cache_evictions_total` | Counter | CURP connection cache evictions (server-side) |

#### General server metrics

| Metric | Type | Description |
|--------|------|-------------|
| `has_leader` | Gauge | Whether the cluster has a leader |
| `is_leader` | Gauge | Whether this server is the leader |
| `server_id` | Gauge | Server's unique ID |
| `fd_used` / `fd_limit` | Gauge | File descriptor usage |

> **Important**: `quic_connect_attempts_total` counts the **server's** connections to
> peer servers (for CURP consensus), NOT client connections to the server.

### Client-side debug logs

xlinectl has **no metrics endpoint**. Client-side observability uses debug logging:

```bash
# Enable debug logs for H3 client (endpoint resolution, connection, retry)
RUST_LOG=xlinerpc=debug xlinectl --endpoints https://server0:2379 put foo bar

# Enable debug logs for CURP channel (connection cache, QUIC connect)
RUST_LOG=curp::rpc::quic_transport::channel=debug xlinectl --endpoints https://server0:2379 put foo bar

# Enable both
RUST_LOG=xlinerpc=debug,curp::rpc::quic_transport::channel=debug xlinectl --endpoints https://server0:2379 put foo bar
```

Client-side CURP cache stats (hits/misses/evictions) are only visible via these debug
logs — they are NOT exported to Prometheus.

---

## 7. CURP Connection Cache

The CURP connection cache reuses QUIC connections within a single process, reducing
QUIC+TLS handshake overhead for long-running clients.

### Status

- **Default: OFF** — the cache is not enabled unless explicitly activated
- **Experimental** — suitable for benchmarking and evaluation
- **Env gate**: `XLINE_CURP_CONN_CACHE=1` enables the cache

### Usage

```bash
# Enable cache for a single command
XLINE_CURP_CONN_CACHE=1 xlinectl --endpoints https://server0:2379 put foo bar

# Enable cache for a benchmark run
XLINE_CURP_CONN_CACHE=1 scripts/quic_curp_benchmark.sh --requests 100
```

### What it does

- Caches QUIC connections per `QuicChannel` instance (per-peer)
- On cache hit: reuses existing connection, opens new QUIC bidi stream
- On cache miss: creates new QUIC connection, caches it
- Conservative eviction: evicts on any error (open_bi_stream, write, read, timeout)
- Max age: 300 seconds, max idle: 30 seconds, max entries: 16

### What it does NOT do

- Does NOT affect KV operations in short-lived xlinectl (each invocation creates a fresh cache)
- Does NOT cache H3 connections (KV ops use CURP QuicChannel, not H3Channel)
- Does NOT persist cache across processes

### Key finding from benchmarking

KV operations (put/get/delete) go through `curp_client.propose()` → CURP's `QuicChannel`,
NOT through `H3Channel`. An H3 session cache would have zero effect on KV workloads.

For full analysis, see [CURP Benchmark](quic-curp-benchmark.md).

---

## 8. Benchmark Docs

### When to read quic-h3-benchmark.md

- You want to understand the H3 client connection lifecycle
- You're evaluating H3 connection pooling feasibility
- You need to know why KV operations bypass H3Channel
- You're designing future H3 optimizations

**Key conclusion**: KV operations use CURP QuicChannel, not H3Channel. H3 connection
pooling would only benefit non-KV operations (compact, auth, cluster, maintenance).

→ [H3 Benchmark](quic-h3-benchmark.md)

### When to read quic-curp-benchmark.md

- You want to understand CURP connection cost per operation
- You're evaluating the `XLINE_CURP_CONN_CACHE` feature
- You need to understand server-side vs client-side metrics
- You're designing CURP connection optimization

**Key conclusion**: Each KV operation creates 3 QUIC connections (1 leader + 2 followers).
The cache reduces server-side connections by 98-100% when enabled.

→ [CURP Benchmark](quic-curp-benchmark.md)

---

## 9. Related Docs Map

| Document | When to Read |
|----------|-------------|
| [QUIC Docs Index](quic-docs-index.md) | Central index for all QUIC/H3 documentation |
| [xlinectl doctor](xlinectl-doctor.md) | Diagnosing endpoint, DNS, TLS, and SNI issues |
| [QUIC Refactor Audit](quic-refactor-audit.md) | Core QUIC/H3 architecture analysis |
| [H3 Benchmark](quic-h3-benchmark.md) | H3 connection lifecycle and pool feasibility |
| [CURP Benchmark](quic-curp-benchmark.md) | CURP connection cost and cache feasibility |

### QUIC Refactor Audit sections by topic

| Topic | Section | Status |
|-------|---------|--------|
| Architecture overview | §1-§6 | Core (full content) |
| Verification scripts | §7-§9 | Summary → [Troubleshooting §1](#1-quick-start-local-3-node-quic-cluster) |
| Clean shutdown | §10 | Core (full content) |
| dquic singleton / test strategy | §11 | Core (full content) |
| Known limitations | §12 | Core (full content) |
| Quality metrics / verification | §13-§14 | Summary |
| Roadmap | §15 | Core (full content) |
| API compatibility (LeaseStreaming) | §18 | Core (full content) |
| Streaming / cancellation | §19-§21 | Summary → [xlinectl doctor](xlinectl-doctor.md) |
| Metrics / observability | §22-§23 | Summary → [CURP Benchmark §10](quic-curp-benchmark.md) |
| Deployment / client config | §24-§25 | Summary → [xlinectl doctor](xlinectl-doctor.md) |
| TLS verification policy | §26 | Summary → [§4 TLS/CA Policy](#4-tls--ca-policy) |
| IP endpoint / SNI routing | §27 | Summary → [§5 SNI Routing](#5-sni-routing-and-ip-endpoints) |
