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

### Minimal Safe Design (Future Idea — Not Implemented)

The current code has **no connection pool** — `H3ClientFactory` is just an `Arc<QuicClient>` wrapper; every RPC creates a fresh QUIC connection. A future implementation could use a session cache, but as shown in the CRITICAL FINDING below, this would not benefit KV workloads:

```
// Future idea — NOT implemented, NOT committed
H3SessionKey {
    endpoint_authority: String,  // e.g. "server0:2379"
    server_name: String,         // SNI target
    tls_fingerprint: [u8; 32],  // hash of TLS config
}

H3SessionCache {  // or H3PoolPrototype
    sessions: DashMap<H3SessionKey, (SendRequest, JoinHandle)>,
    client: QuicClient,
}
```

**Why this was not implemented**: KV operations bypass H3Channel entirely (see CRITICAL FINDING). The cache would only benefit `compact()` and `status()` — rarely called operations.

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

## CRITICAL FINDING: KV Operations Bypass H3Channel Entirely

**All KV operations (put, get, delete, txn, compact) go through `curp_client.propose()` → CURP's `QuicChannel`, NOT through `H3Channel`.**

| Operation | Transport Path | H3Channel Used? |
|-----------|---------------|-----------------|
| KV put/get/delete/range/txn | `curp_client.propose()` → CURP QuicChannel | **NO** |
| Watch | `channel.client_streaming()` → H3Channel | Yes (but not unary cache) |
| Lease keepalive | `channel.client_streaming()` → H3Channel | Yes (but not unary cache) |
| Compact | `channel.unary()` → H3Channel | **Yes** |
| Status | `channel.unary()` → H3Channel | **Yes** |
| Auth | varies → H3Channel | Yes |
| Cluster member operations | `curp_client.propose()` → CURP | **NO** |

### Impact on Connection Cache (Tested, Not Committed)

A prototype H3 session cache was built and benchmarked. It only cached `H3Channel::unary()` connections. Since KV operations bypass H3Channel entirely, **the cache had zero effect on KV workloads**.

Both baseline and cached benchmarks showed identical performance (~265ms avg, ~3.8 req/s) because the workload (KV put+get) doesn't touch H3Channel at all. The prototype was rolled back — no runtime H3 cache code is committed.

The cache would only benefit:
- `compact()` operations (rarely called)
- `status()` operations (monitoring only)
- Auth operations (once per session)

### Implications for Connection Pool Direction

1. **H3Channel caching is the wrong target** for KV workload optimization
2. **CURP QuicChannel** is where KV operations live — that's where connection reuse matters
3. Watch and lease use `client_streaming()`, not `unary()` — the cache doesn't apply there either
4. The per-RPC connection overhead comes from CURP, not H3

### Revised Connection Reuse Strategy

If connection reuse is pursued, it should target **CURP's `QuicChannel`**, not `H3Channel`:

```
Current: KvClient → curp_client.propose() → QuicChannel.get_connection() → new QUIC conn per RPC
Target:  KvClient → curp_client.propose() → QuicChannel.get_connection() → cached QUIC conn
```

The `QuicChannel` already has round-robin retry across endpoints. Adding a connection cache at this level would benefit all KV operations.

## Recommendations

1. **For `xlinectl` (CLI tool)**: Connection pool has minimal benefit — each invocation is short-lived and cannot reuse connections. The ~1.3s per invocation comes from QUIC/TLS handshake, cluster discovery, and connection setup, not server-side execution.

2. **For `xline-client` (library)**: H3Channel connection caching has minimal benefit — KV operations bypass H3Channel. CURP connection caching would benefit KV operations, but requires changes in the `curp` crate.

3. **For CURP (quorum)**: Connection reuse at the QuicChannel level would eliminate per-RPC QUIC handshake overhead for KV operations. This is the highest-value target.

4. **Priority**: Low. The current pattern works correctly. Connection pool is an optimization for library users, not a correctness issue. If pursued, target CURP's QuicChannel, not H3Channel.

## Files

- `scripts/quic_h3_benchmark.sh` — benchmark script
- `crates/xline-client/examples/client_kv_benchmark.rs` — long-running KV benchmark (demonstrates H3 cache is not on hot path)
- `docs/quic-h3-benchmark.md` — this document
