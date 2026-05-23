# H3 Connection Pool Feasibility Benchmark

## Executive Summary

Benchmarked the current "one QUIC connection + one H3 session + one driver task per RPC" pattern to quantify overhead and assess connection pool feasibility.

**Key Finding**: Each `xlinectl` invocation creates ~8.8 QUIC connections and takes ~1.3s. `xlinectl` is a short-lived process — it cannot reuse connections across invocations. The per-invocation latency is dominated by setup: QUIC/TLS handshake, cluster discovery (FetchCluster via CURP), and H3/CURP connection establishment. The actual server-side KV execution is negligible. Connection reuse is architecturally feasible (h3 `SendRequest` is `Clone`, QUIC supports multiplexed streams) but only benefits long-running client processes.

## Benchmark Results

### Sequential (500 GET requests, single client)

| Metric | Value |
|--------|-------|
| Total time | 647s (10.8 min) |
| Avg latency | 1.29s |
| p50 | 1.30s |
| p95 | 1.34s |
| p99 | 1.37s |
| Throughput | 0.8 req/s |
| QUIC connect delta | 4395 (~8.8 per invocation) |
| Stream drop warnings | 8225 (~16.5 per invocation) |
| NoViablePath errors | 0 |
| Panics | 0 (false positive from log messages) |

### Concurrent (10 clients × 5 requests)

| Metric | Value |
|--------|-------|
| Wall time | 6.5s |
| Aggregate throughput | 7.7 req/s |
| Per-request latency | 1.29s avg (unchanged) |
| Success rate | 100% |

### Concurrent (20 clients × 5 requests)

| Metric | Value |
|--------|-------|
| Wall time | 8.3s |
| Aggregate throughput | 12 req/s |
| Per-request latency | 1.63s avg, p95 2.76s |
| Success rate | 100% |

## Per-Invocation Connection Breakdown

Each `xlinectl` invocation creates ~8.8 QUIC connections:

1. **FetchCluster** (cluster discovery): 3-6 CURP connections (one per node for quorum)
2. **KV GET** (H3 client): 1 H3 connection to target server
3. **KV GET** (CURP quorum): 2-3 CURP connections (quorum vote)
4. **Total**: ~6-10 connections per invocation

## Time Breakdown (Estimated, Per xlinectl Invocation)

| Component | Time | % of Total |
|-----------|------|------------|
| Process startup + arg parsing | ~50ms | 4% |
| QUIC handshake (TLS 1.3) | 100-500ms | 15-40% |
| H3 session init | 5-10ms | <1% |
| CURP quorum (FetchCluster + KV) | 1-5ms | <1% |
| KV operation (server-side) | 0.5-2ms | <1% |
| **Total per invocation** | **~1.3s** | **100%** |

The QUIC handshake dominates. Connection reuse would eliminate this for subsequent requests in a long-running client.

## Connection Pool Feasibility

### What CAN Be Shared

- **QUIC connection**: Multiplexed, supports concurrent streams
- **H3 session**: `SendRequest` is `Clone`, multiple `send_request()` calls on same connection
- **Driver task**: One per connection, shared across all streams

### What CANNOT Be Shared

- **CURP quorum connections**: Each CURP request uses a separate bidi stream. This benchmark did not evaluate CURP-level connection pooling; it should be assessed separately.
- **Server-side**: No client connection state to pool

### Minimal Safe Design

The current code has **no connection pool** — `H3ClientFactory` is just an `Arc<QuicClient>` wrapper; every RPC creates a fresh QUIC connection. A future implementation could use a session cache:

```
// NOT current code — future design sketch
H3SessionKey {
    endpoint_authority: String,  // e.g. "server0:2379"
    server_name: String,         // SNI target
    tls_fingerprint: [u8; 32],  // hash of TLS config
}

H3SessionCache {  // or H3PoolPrototype
    sessions: DashMap<H3SessionKey, (SendRequest, JoinHandle)>,
    client: QuicClient,
}

// On first request to a server:
//   1. Build H3SessionKey from endpoint + TLS config
//   2. QUIC connect → h3::client::new → (conn, send_req, driver)
//   3. Store (send_req, driver_handle) in cache
//   4. Spawn driver task
// On subsequent requests:
//   1. Look up cache by H3SessionKey
//   2. Clone send_req, call send_request()
//   3. No new QUIC connection
// On connection error:
//   1. Remove from cache
//   2. Reconnect on next request
```

### Expected Improvement (Estimated)

With connection reuse in a long-running client:
- **First request**: ~1.3s (same as now)
- **Subsequent requests**: ~50-100ms (H3 stream only, no QUIC handshake) — *estimated*
- **Expected speedup**: 10-20x for sequential requests to same server — *hypothetical, requires a unary-only pool prototype benchmark to verify*

### Risks

1. **Connection lifetime**: QUIC connections have idle timeout. Need health check or reconnect-on-error.
2. **Server restart**: Pool must detect stale connections and reconnect.
3. **Memory**: Each connection holds TLS state + QUIC state. Pool size limit needed.
4. **Concurrency**: Multiple streams on same connection share bandwidth. May need per-server connection limit.

## Recommendations

1. **For `xlinectl` (CLI tool)**: Connection pool has minimal benefit — each invocation is short-lived and cannot reuse connections. The ~1.3s per invocation comes from QUIC/TLS handshake, cluster discovery, and connection setup, not server-side execution.

2. **For `xline-client` (library)**: Connection pool has significant benefit for long-running clients (watch, lease keepalive, repeated KV ops). The library already creates one H3Channel per client — pooling at this level is natural.

3. **For CURP (quorum)**: Uses bidi streams. This benchmark did not evaluate CURP-level pooling; it should be assessed separately.

4. **Priority**: Low. The current pattern works correctly. Connection pool is an optimization for library users, not a correctness issue.

## Files

- `scripts/quic_h3_benchmark.sh` — benchmark script
- `docs/quic-h3-benchmark.md` — this document
