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

`quic_connect_attempts_total` from `/metrics` endpoint (CURP client-side counter).

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
