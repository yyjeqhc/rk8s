# xlinectl doctor

## Overview

`xlinectl doctor` diagnoses endpoint, DNS, TLS CA, QUIC SNI routing, and connection
issues before you start using the cluster. It reports problems but does **not** modify
system configuration, `/etc/hosts`, or any server state.

## Basic usage

```bash
# Use defaults (https://server0:2379, development fixture CA)
xlinectl doctor

# Explicit endpoints and CA
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt doctor

# Include a live connection test
xlinectl --endpoints https://server0:2379 --ca_cert_pem_path fixtures/ca.crt doctor --check_connection
```

## What it checks

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

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All critical checks passed (warnings are allowed) |
| 1 | One or more critical checks failed |

## Common failures

### Missing scheme

```
$ xlinectl --endpoints server0:2379 doctor
❌ server0:2379
   Missing scheme (https:// or http://)
```

**Fix:** Use `https://server0:2379`.

### Missing port

```
$ xlinectl --endpoints https://server0 doctor
❌ https://server0
   Missing port number
```

**Fix:** Use `https://server0:2379`.

### IP endpoint

```
$ xlinectl --endpoints https://127.0.0.1:2379 --ca_cert_pem_path fixtures/ca.crt doctor
❌ https://127.0.0.1:2379
   IP address endpoint — not supported by QUIC SNI routing
```

**Fix:** Use DNS server names (server0, server1, server2) and map them in `/etc/hosts`.
See [Local 3-node setup](#local-3-node-setup) below.

### localhost endpoint

```
$ xlinectl --endpoints https://localhost:2379 --ca_cert_pem_path fixtures/ca.crt doctor
❌ https://localhost:2379
   'localhost' endpoint — not supported by QUIC SNI routing
```

**Fix:** Use `https://server0:2379`.

### DNS failure

```
$ xlinectl --endpoints https://nonexistent:2379 doctor
   ❌ DNS lookup failed: failed to lookup address information: Name or service not known
      Hint: Check /etc/hosts or DNS for 'nonexistent'
```

**Fix:** Add the hostname to `/etc/hosts` or ensure it resolves via DNS.

### Empty/missing CA

```
$ xlinectl --endpoints https://server0:2379 --ca_cert_pem_path /dev/null doctor
❌ CA file is empty: /dev/null
```

**Fix:** Pass the correct CA certificate path.

### Connection check skipped after static failures

```
$ xlinectl --endpoints https://127.0.0.1:2379 --ca_cert_pem_path fixtures/ca.crt doctor --check_connection
── Endpoint Checks ──
  ✅ https://127.0.0.1:2379
     ❌ IP address endpoint — not supported by QUIC SNI routing
...
── Connection Check ──
  ⏭️  Skipped because critical static checks failed
     Fix the errors above, then rerun with --check_connection.
```

When static critical checks already failed, `--check_connection` is skipped to avoid
attempting a connection on a clearly invalid configuration.

## Local 3-node setup

For a local 3-node Xline cluster, add these entries to `/etc/hosts`:

```text
127.0.0.1 server0 server1 server2
```

Key points:

- QUIC SNI routing uses the endpoint hostname as the TLS server name.
- IP endpoints (`127.0.0.1`) are **not supported** for local multi-node routing because
  each server registers the same IP, and only one server can win the SNI slot.
- Use `server0`, `server1`, `server2` — not `127.0.0.1`.
- Each server listens on a different client port (2379, 2381, 2383) and peer port (2380,
  2382, 2384).

## TLS policy

Xline does **not** silently skip TLS verification:

- If no CA certificate is configured and the development fixture CA is not found, an
  empty `RootCertStore` is used. All connections fail with `UnknownIssuer`.
- In development builds, `fixtures/ca.crt` is discovered automatically via the
  `CARGO_MANIFEST_DIR` environment variable.
- In production, always pass `--ca_cert_pem_path <PATH>` explicitly.

## Experimental features

| Feature | Description |
|---------|-------------|
| `XLINE_CURP_CONN_CACHE=1` | Enables the CURP per-channel connection cache (experimental, default off) |
| `RUST_LOG=<filter>` | Enables debug-level tracing (e.g., `RUST_LOG=xlinerpc=debug`) |

`doctor` only reports these values — it does not enable or disable them.

## Related docs

- [QUIC Troubleshooting Guide](quic-troubleshooting.md) — entry point for all QUIC/H3 issues
- [QUIC refactor audit](quic-refactor-audit.md) — full QUIC/H3 architecture analysis
- [H3 benchmark feasibility](quic-h3-benchmark.md) — H3 connection lifecycle benchmarks
- [CURP benchmark](quic-curp-benchmark.md) — CURP connection cost and cache feasibility
