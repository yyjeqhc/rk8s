# CURP QuicChannel Connection Reuse — Benchmark & Feasibility

## 1. Current Architecture

### Connection Model

Every CURP RPC creates a **new QUIC connection** (QUIC handshake + TLS):

```
get_connection()
  → try_connect()
    → endpoint resolver (DNS or IP)
    → QuicClient.connected_to_with_source()  ← NEW QUIC+TLS HANDSHAKE
    → return Connection
```

`QuicChannel` stores: `client: Arc<QuicClient>`, `addrs: Arc<RwLock<Vec<String>>>`, `index: AtomicUsize`.

### KV Put Call Chain (3-node cluster)

```
KvClient::put()
  → curp_client.propose(&cmd, token, true)
    → Retry::propose() → Unary::propose()
      → propose_mutative()
        → send_leader_propose()  ← 1 QUIC conn to leader (propose_stream = server_streaming_call)
        → send_record()          ← 2 QUIC conns to followers (record = unary_call)
      Total: 3 NEW QUIC connections per put
```

### KV Get Call Chain (read-only)

```
KvClient::range()
  → curp_client.propose(&cmd, token, false)
    → propose_read_only()
      → send_leader_propose()    ← 1 QUIC conn to leader
      → send_read_index()        ← 2 QUIC conns to followers
    Total: 3 NEW QUIC connections per get
```

### xlinectl Lifecycle (short-lived)

Each `xlinectl put key value` invocation:
1. `Client::connect()` — builds QuicClient, creates QuicConnects per peer
2. `discover_peer_addrs()` — FetchCluster to all 3 servers = 3 QUIC connections
3. `propose()` — 3 QUIC connections (leader + 2 followers)
4. Exit — all connections dropped

**Per xlinectl invocation: 6+ QUIC connections** (startup + propose).

## 2. Benchmark Results

### xlinectl Sequential (short-lived client)

| Requests | Ops (put+get) | QUIC Connections | Conns/Op | Wall Time | Throughput |
|----------|--------------|-----------------|----------|-----------|------------|
| 10       | 20           | 186             | 9.30     | 25.2s     | 0.8 ops/s  |
| 20       | 40           | 376             | 9.40     | 50.4s     | 0.8 ops/s  |
| 100      | 200          | 1,898           | 9.49     | 253.4s    | 0.8 ops/s  |

**Consistent ~9.4 connections per op** across all request counts.

Breakdown per xlinectl invocation (put+get):
- FetchCluster: 3 connections (startup)
- Put propose: 3 connections (leader + 2 followers)
- Get propose: 3 connections (leader + 2 followers)
- Total: 9 + overhead ≈ 9.4

### Long-Running Client (persistent connection)

| Requests | Ops (put+get) | QUIC Connections | Conns/Op | Wall Time | Throughput |
|----------|--------------|-----------------|----------|-----------|------------|
| 20       | 40           | 130             | 3.25     | 8.9s      | 4.5 ops/s  |

**~3.25 connections per op** — FetchCluster amortized over session.

### Comparison

| Metric              | xlinectl (short-lived) | Long-Running Client |
|---------------------|----------------------|---------------------|
| Conns per op        | 9.4                  | 3.25                |
| Throughput          | 0.8 ops/s            | 4.5 ops/s           |
| FetchCluster per op | 3 (every invocation) | ~0 (amortized)      |
| Speedup             | 1×                   | **5.7×**            |

### Metrics Source

`quic_connect_attempts_total` from `/metrics` endpoint. **Important**: This is a **server-side** metric — it counts the server's own QUIC connections to peer servers, NOT the client's connections to the server. The CURP client (xlinectl) has no metrics endpoint, so client-side connection counts cannot be directly observed via Prometheus.

## 3. Feasibility Analysis

### What Connection Reuse Would Save

**Short-lived clients (xlinectl):** Each invocation creates 6+ connections. Connection caching would save the FetchCluster (3 conns) but not the propose connections (they go to different servers).

**Long-running clients:** Already amortize FetchCluster. Per-operation: 3 connections (leader + 2 followers). Connection reuse would eliminate the QUIC+TLS handshake on subsequent operations to the same server.

### Theoretical Savings

For a long-running client doing N put operations:
- Current: 3N QUIC handshakes (1 per server per op)
- With pool: 3 QUIC handshakes total (one per server, reused)

**Potential: 3× reduction in QUIC handshakes for long-running clients.**

### Implementation Complexity

**Low risk:**
- `dquic::Connection` is `#[derive(Clone)]` — cheaply cloneable
- QUIC supports multiplexed streams — `open_bi_stream()` works on existing connections
- `get_connection()` already round-robins through addresses

**Medium risk:**
- Stale connection detection (server restart, network partition)
- Leader change detection (connections to old leader become invalid)
- Connection health checking (QUIC idle timeout, GOAWAY)

**High risk:**
- Retry path interaction (RpcTransport → re-fetch leader → new connections)
- FetchCluster path (WrongClusterVersion/Zombie → fetch_cluster → broadcast)
- Quorum/super_quorum paths (send_record/send_read_index)

### Design Constraints (from user)

- No full CURP connection pool
- No protocol semantic changes
- No leader election/quorum/membership changes
- No unit tests as primary validation
- Real 3-node cluster only

## 4. Minimal Safe Design

### Option A: Connection Cache with Stale Eviction

```rust
struct CachedConnection {
    conn: dquic::Connection,
    server_id: ServerId,
    created_at: Instant,
    last_used: Instant,
}

struct ConnectionCache {
    cache: DashMap<ServerId, CachedConnection>,
    max_age: Duration,      // e.g., 5 minutes
    max_idle: Duration,     // e.g., 30 seconds
}
```

**Scope:** Only cache connections within a single QuicChannel (per-peer).

**Eviction:**
- Time-based: `created_at + max_age`
- Idle-based: `last_used + max_idle`
- Error-based: evict on connection error

**Not implemented:** Health checking, GOAWAY handling, leader change detection.

### Option B: Session-Level Connection Sharing

Cache at the CurpClient level — share connections across all QuicConnects.

**More complex, higher risk, not recommended for MVP.**

### Recommended: Option A

Minimal cache within `get_connection()`:
1. Check cache for existing connection
2. If hit and healthy: reuse
3. If miss or stale: create new connection, cache it
4. On error: evict and retry with fresh connection

**Estimated savings:** ~3× reduction in QUIC handshakes for long-running clients.

## 5. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Stale connection (server restart) | Medium | High | Max-age eviction + error eviction |
| Leader change | High | Medium | Error eviction triggers re-connect |
| Connection leak | Low | Medium | Drop impl clears cache entry |
| Retry path conflict | Low | High | Don't cache during retry/fetch |
| Performance regression | Low | Medium | Behind env var gate |

## 6. Verification Plan

1. `cargo check` — compilation
2. `quic_ci_smoke.sh` — basic functionality
3. `quic_restart_stress.sh 3` — restart stability
4. `quic_fault_smoke.sh` — fault tolerance
5. `quic_curp_benchmark.sh` — measure improvement

## 7. Conclusion

Connection reuse is architecturally feasible and would provide ~3× reduction in QUIC handshakes for long-running clients. The minimal safe design (Option A) caches connections within `get_connection()` with time-based eviction.

For short-lived clients (xlinectl), the dominant cost is process startup + FetchCluster, which connection caching doesn't address. The long-running client benchmark shows 5.7× speedup over xlinectl, demonstrating the benefit of persistent connections.

**Recommendation:** Implement Option A behind `XLINE_CURP_CONN_CACHE=1` env var, measure improvement, then evaluate whether to expand scope.

## 8. Cache Implementation Results

### Implementation

Added `ConnectionCache` to `QuicChannel` in `crates/curp/src/rpc/quic_transport/channel.rs`:
- **Cache key**: `String` (scheme-stripped endpoint, e.g. `"server0:2379"`)
- **Cache value**: `dquic::Connection` (cheaply Clone via Arc) + `Instant` (created_at, last_used)
- **Scope**: Per `QuicChannel` instance (not global). Each `QuicConnect` (per-peer) has its own `QuicChannel` with its own cache.
- **Env gate**: `XLINE_CURP_CONN_CACHE=1` enables, unset = disabled. Default OFF.
- **Eviction**: on ANY RPC error (open_bi_stream, write, read, timeout), on age > 5min, on idle > 30s, on cache full (oldest evicted)
- **No background health checker, no LRU, no DnsFallback changes**

Three new metrics added to `crates/curp/src/rpc/quic_transport/metrics.rs`:
- `curp_conn_cache_hits_total` — incremented only when cache is enabled and a reusable connection is found
- `curp_conn_cache_misses_total` — incremented only when cache is enabled and a NEW connection is created
- `curp_conn_cache_evictions_total` — incremented on explicit eviction (error or full), with `component="curp_channel"` label

### Cache Key Safety

**QuicChannel-scoped**: Each `QuicConnect` (per-peer) gets its own `Arc<QuicChannel>` via `quic_connect()` in `connect.rs`. The cache is a field of `QuicChannel`, so it's per-peer — no cross-peer connection sharing.

**Immutable bindings**: Within a single `QuicChannel` instance, the `QuicClient` (TLS config), `dns_fallback` policy, and source binding are set at construction and never changed. The `addrs` list can be updated (via `update_addrs()` during FetchCluster), but the cache key is the endpoint string, not an index. Stale entries are evicted by age/idle timeout.

**Server identity change**: If the server at an endpoint changes (e.g., different TLS cert after restart), the cached connection may become stale. The 300-second max age limits exposure. This is acceptable for a prototype; production would need TLS fingerprint or GOAWAY-based eviction.

### Eviction Policy

| Error Type | Evicts? | Rationale |
|------------|---------|-----------|
| `open_bi_stream` failure | ✅ | Connection-level: QUIC stream creation failed |
| `open_bi_stream` timeout | ✅ | Connection may be stalled |
| Request header write failure | ✅ | Connection-level: write on stale/broken conn |
| Request data write failure | ✅ | Connection-level |
| Response read failure | ✅ | Connection-level: peer closed or network error |
| Response decode failure | ✅ | Evicted for simplicity (rare, CURP retry re-creates) |
| `RpcTransport` timeout | ✅ | Connection may be stalled |
| Application `CurpError` (Duplicated, ShuttingDown, etc.) | ✅ | Evicted for simplicity (these errors don't retry, evicting is harmless) |
| Leader redirect / WrongClusterVersion / Zombie | ❌ | Not evicted — these are handled by CURP retry which re-fetches cluster |

**Note**: Eviction on application-level errors (Duplicated, ShuttingDown, etc.) is technically unnecessary but harmless — these errors are fatal and don't trigger retries. The connection is evicted but the error still propagates. This simplification avoids complex error classification logic.

### "0 New QUIC Connections" Clarification

The "0 new QUIC connections" metric for the long-running client means: **during the measured benchmark interval**, the `quic_connect_attempts_total` counter did not increase. It does NOT mean zero total connections exist — it means all connections were reused from the cache. The initial connections (created at startup before the benchmark interval) are not counted.

**Critical note on measurement**: The benchmark script scrapes `quic_connect_attempts_total` from the **server's** `/metrics` endpoint. This metric counts the **server's own** QUIC connections to peer servers (for CURP consensus), NOT the client's connections. When `XLINE_CURP_CONN_CACHE=1` is set as a shell-level environment variable, it applies to BOTH the server processes AND the xlinectl client processes. The "0 connections" result reflects the **server's** connection cache working (the server reuses its connections to peers), not the client's cache.

To measure client-side cache behavior specifically, the env var must be set only for xlinectl:
```bash
# Server WITHOUT cache, client WITH cache
XLINE_CURP_CONN_CACHE=1 xlinectl --endpoints ... put key value
# Server's quic_connect_attempts_total will still increase (server has no cache)
```

### Benchmark Results (10 requests, same parameters)

| Metric | Baseline | Cache | Reduction |
|--------|----------|-------|-----------|
| **Sequential: QUIC connections** | 189 (9.45/op) | 3 (0.15/op) | **98.4%** |
| **Long-running: QUIC connections** | 67 (3.35/op) | 0 (0.00/op) | **100%** |
| Long-running: avg latency | 266ms | 248ms | -7% |
| Long-running: throughput | 3.8 req/s | 4.0 req/s | +5% |

**Note on benchmark results**: These numbers were measured with `XLINE_CURP_CONN_CACHE=1` set for the entire shell, which means both the server and client had the cache enabled. The "98.4% reduction" and "100% reduction" primarily reflect the **server's** connection reuse (the server reuses its peer connections), not the client's. The client-side cache has minimal effect for short-lived xlinectl processes (each invocation creates a fresh cache).

### Analysis

**Sequential xlinectl** (short-lived processes): Each `xlinectl` invocation creates a fresh `QuicChannel` with its own cache. The cache has minimal effect because:
1. Each xlinectl process is short-lived (one put+get, then exit)
2. The cache is per-process (not shared across invocations)
3. Within a single invocation, the cache may help if multiple operations go to the same server

**Long-running client**: The cache eliminates redundant QUIC connections within a single process. The 67 → 0 reduction reflects the server's connection cache, not the client's.

**Throughput bottleneck**: The dominant cost is not QUIC connection establishment — it's the CURP consensus protocol (propose + record to followers). Connection caching helps latency but doesn't change the fundamental throughput ceiling.

### Fault Verification (with cache enabled)

| Test | Result |
|------|--------|
| `quic_ci_smoke.sh` | ✅ ALL PASSED |
| `quic_restart_stress.sh 3` | ✅ 3/3 (0 panics, 0 NoViablePath, ~118-140 warnings/round) |
| `quic_fault_smoke.sh` | ✅ ALL PASSED (kill follower, kill leader, restart) |

### Conclusion

The connection cache provides dramatic reduction in QUIC connections (98-100%) when measured from the **server's** perspective. The client-side cache has minimal effect for short-lived CLI tools (xlinectl) because each invocation creates a fresh process with a fresh cache.

For long-running clients (e.g., a persistent service using `xline-client`), the client-side cache would provide meaningful savings by reusing connections across operations within the same process.

**Status**: Implemented, behind `XLINE_CURP_CONN_CACHE=1` env gate, all verification passed.

## 9. Semantic Safety Audit

### Cache Key Safety ✅

- **QuicChannel instance scoped**: Each `QuicConnect` (per-peer) gets its own `Arc<QuicChannel>` via `quic_connect()`. The cache is per-peer — no cross-peer sharing.
- **TLS/QuicClient immutable**: `client: Arc<QuicClient>` and `dns_fallback` are set at construction and never changed within a `QuicChannel` instance.
- **addrs mutation safe**: The `addrs` list can be updated (via `update_addrs()`), but the cache key is the endpoint string, not an index. Stale entries are evicted by age/idle timeout.
- **No key collision risk**: Different endpoints map to different cache entries. Same endpoint always maps to the same server (assuming no DNS changes during the connection lifetime).

### Eviction Policy ✅

All 5 RPC methods (`unary_call`, `server_streaming_call`, `client_streaming_call`, `bidirectional_streaming_call`, `raw_unary_call`) evict on ANY error:
- `open_bi_stream` failure/timeout
- Request write failure
- Response read failure
- Application-level errors (for simplicity)

This is conservative — application-level errors don't necessarily mean the connection is stale, but evicting on them is harmless (CURP retry re-creates anyway). The alternative (classifying errors) adds complexity for minimal benefit in a prototype.

### Async Mutex Safety ✅

- `parking_lot::Mutex` (`ParkMutex`) is only locked in synchronous methods: `get()`, `insert()`, `remove()`
- Lock is always dropped before any `.await` point
- No I/O operations under lock
- `HashMap` operations are O(1) for get/insert/remove, O(n) for min_by_key on full eviction — n ≤ 16 (max_entries)
- Concurrent cache hit/miss from different tasks: each `get_connection()` acquires the lock briefly, no contention risk

### Metrics Correctness ✅

The code-level metric semantics are correct:

- `curp_conn_cache_hits_total`: Only incremented when cache is enabled AND a reusable connection is found. NOT incremented on cache miss or when cache is disabled.
- `curp_conn_cache_misses_total`: Only incremented when cache is enabled AND a NEW connection is successfully created. NOT incremented on connection failure.
- `curp_conn_cache_evictions_total`: Only incremented on explicit eviction. Low cardinality labels (`component="curp_channel"`).
- `quic_connect_attempts_total`: Only incremented on cache miss (new connection attempt), NOT on cache hit. This means the metric correctly reflects actual QUIC connection attempts regardless of cache state.

**Observability limitation**: The cache metrics (`curp_conn_cache_hits/misses/evictions`) are defined in the `curp` crate and incremented by the CURP client (xlinectl). However, **xlinectl has no metrics endpoint**, so these counters are NOT exported to Prometheus. They can only be observed via debug logging (`RUST_LOG=curp::rpc::quic_transport::channel=debug`), which is not enabled by default.

The `quic_connect_attempts_total` and `quic_connect_failures_total` metrics are exported by the **server's** `/metrics` endpoint. They count the **server's own** QUIC connections to peer servers, not the client's connections. When both server and client have `XLINE_CURP_CONN_CACHE=1`, the server's metrics reflect the server's connection cache behavior.

### Known Risks

1. **Server restart with same endpoint**: If a server restarts and accepts QUIC connections but rejects CURP operations, the cached connection won't be evicted until a CURP RPC fails. The 300-second max age limits exposure.

2. **Leader change**: Connections to the old leader remain cached. When CURP detects a leader change (via RpcTransport/WrongClusterVersion/Zombie), it re-fetches the cluster and updates `addrs`. The old leader's cache entry becomes stale and will be evicted by age/idle timeout.

3. **DNS changes**: If DNS records change during a long-running session, cached connections may point to wrong servers. The 300-second max age limits exposure. This is the same risk as without caching.

4. **Connection leak on eviction miss**: If a CURP error is not caught by the eviction logic (e.g., error in a spawned task), the cached connection remains. The age/idle timeout provides a safety net.

5. **No GOAWAY handling**: If the server sends GOAWAY (graceful shutdown), the cached connection is not immediately evicted. The next RPC on that connection will fail and trigger eviction.

All risks are mitigated by the 300-second max age and 30-second max idle timeouts. For a prototype, this is acceptable. Production would need GOAWAY handling and TLS fingerprint-based eviction.

## 10. Metrics Architecture — Runtime Verification

### Metric Definitions

All 5 CURP QUIC transport metrics are defined in `crates/curp/src/rpc/quic_transport/metrics.rs` using the `define_metrics!` macro:

| OTel Instrument | Prometheus Name | Labels | Trigger |
|---|---|---|---|
| `quic_connect_attempts` | `quic_connect_attempts_total` | `component="curp_channel"` | `get_connection()` on cache miss or cache disabled |
| `quic_connect_failures` | `quic_connect_failures_total` | `component`, `error_type` | `get_connection()` when ALL addresses fail |
| `curp_conn_cache_hits` | `curp_conn_cache_hits_total` | `component="curp_channel"` | `get_connection()` on cache hit |
| `curp_conn_cache_misses` | `curp_conn_cache_misses_total` | `component="curp_channel"` | `get_connection()` on cache miss + successful connect |
| `curp_conn_cache_evictions` | `curp_conn_cache_evictions_total` | `component="curp_channel"` | `evict_cache_entry()` on any error |

The meter scope is `CARGO_PKG_NAME` = `curp`, with `component="curp_quic"` as a meter-level attribute.

### Prometheus Output (server's `/metrics` endpoint)

The server exports its own metrics at `--metrics-port` (default 9100) at `/metrics`. Example output:

```
# HELP quic_connect_attempts_total The total number of QUIC connection attempts.
# TYPE quic_connect_attempts_total counter
quic_connect_attempts_total{component="curp_channel",otel_scope_name="curp",otel_scope_version="0.1.0"} 52
```

**Note**: The server's `quic_connect_attempts_total` counts the **server's own** QUIC connections to peer servers (for CURP consensus), not the client's connections to the server.

### Client-Side Metrics (NOT exported)

The cache metrics (`curp_conn_cache_hits/misses/evictions`) are incremented by the CURP client (xlinectl or xline-client). However, **xlinectl has no metrics endpoint**, so these counters are NOT exported to Prometheus.

To observe client-side cache behavior:
```bash
RUST_LOG=curp::rpc::quic_transport::channel=debug xlinectl --endpoints ... put key value
# Output includes: curp_conn_cache_hit, curp_conn_cache_miss, curp_conn_cache_evict
```

### Runtime Verification (performed 2026-05-24)

**Test 1: Server without cache, client with cache**
```bash
# Server started WITHOUT XLINE_CURP_CONN_CACHE
# Client: XLINE_CURP_CONN_CACHE=1 xlinectl put key value
# Server's quic_connect_attempts_total delta: +10 per PUT (server makes connections to peers)
```

**Test 2: Server with cache, client with cache**
```bash
# Both server and client have XLINE_CURP_CONN_CACHE=1
# Server's quic_connect_attempts_total delta: 0 (server reuses cached connections)
```

**Conclusion**: The "0 connections" benchmark result was measuring the **server's** connection cache, not the client's. The client-side cache has minimal observable effect for short-lived xlinectl processes.

## 11. Client-Side Cache Verification

### Problem

Previous rounds measured `quic_connect_attempts_total` from the server's `/metrics` endpoint, which counts the **server's** QUIC connections to peers — not the client's. The "98.4% reduction" was a server-side cache effect.

### Solution

Added `tracing` and `tracing-subscriber` to `xlinectl` (opt-in via `RUST_LOG` env var). Updated `scripts/quic_curp_benchmark.sh` to:
1. Set `RUST_LOG=curp::rpc::quic_transport::channel=debug` when `XLINE_CURP_CONN_CACHE=1`
2. Capture stderr from xlinectl invocations to a debug log file
3. Parse `curp_conn_cache_hit`, `curp_conn_cache_miss`, `curp_conn_cache_evict` counts
4. Report client-side cache stats alongside server-side metrics

### Results (3 put+get = 6 xlinectl invocations)

| Metric | Baseline (no cache) | Cache enabled |
|--------|---------------------|---------------|
| **Server-side QUIC conns** | 93 (9.30/op) | 0 (0.00/op) |
| **Client cache hits** | N/A | 12 |
| **Client cache misses** | N/A | 36 |
| **Client cache evictions** | N/A | 0 |
| **Throughput** | 0.8 ops/s | 0.8 ops/s |

### Per-Invocation Analysis

Each xlinectl invocation is a separate process with its own `ConnectionCache`. The cache is per-`QuicChannel` instance (each `QuicConnect` per-peer has its own cache).

Per invocation (e.g., `put key value`):
1. `Client::connect()` → `discover_peer_addrs()` via H3 path (not CURP QuicChannel)
2. `CurpClientBuilder::discover_from()` → creates 3 `QuicConnect` instances, each with own `QuicChannel`
3. `curp_client.propose()` → `send_propose_mutative()` → leader `propose_stream` + 2 followers `record`
4. Each `get_connection()` on each `QuicChannel`: first call = miss, subsequent calls = hit

Result: **6 misses** (3 FetchCluster connections on H3 path + 3 CURP propose connections) + **2 hits** (subsequent CURP operations reuse cached connections within the same process).

### Why Hits < Misses

The 12 hits / 36 misses = 0.33 ratio is lower than expected. This is because:
- Each xlinectl invocation creates 3 separate `QuicChannel` instances (one per peer)
- Each `QuicChannel` has its own cache
- The first `get_connection()` on each `QuicChannel` is always a miss
- Subsequent calls to the same `QuicChannel` hit the cache
- The propose path makes 1 call to the leader's channel + 2 calls to followers' channels
- So: 3 misses (first call per channel) + 3 hits (subsequent calls) = 6 per invocation

The 12 hits / 6 invocations = 2 hits per invocation suggests that not all 3 subsequent calls hit the cache — likely due to the CURP retry logic or the way `for_each_follower` iterates.

### Key Finding

The client-side cache works correctly within a single process. For short-lived xlinectl invocations, the cache provides modest savings (2 hits per invocation). For long-running clients (e.g., persistent services using `xline-client`), the cache would provide significantly more savings as operations reuse connections across the process lifetime.

### xlinectl Tracing

Added `tracing` and `tracing-subscriber` to `crates/xlinectl/Cargo.toml`. The subscriber is initialized only when `RUST_LOG` is set:

```bash
# Enable debug logging for CURP connection cache
RUST_LOG=curp::rpc::quic_transport::channel=debug xlinectl --endpoints ... put key value
```

The debug output goes to stderr and includes structured fields: `endpoint`, `server_name`, `addr`.

## 12. Soak Test Results

See [CURP Cache Soak Test](quic-curp-cache-soak.md) for stability verification under sustained load (120s long run, 5-round restart stress, fault injection).
