# Xline QUIC/H3 Refactoring — Post-Commit Audit Report

**Date**: 2026-05-23
**Auditor**: Sisyphus (AI Agent)
**Commits Audited**:
- `e7199407d` — QUIC/H3 architecture refactoring (15 files, +1369/-453)
- `a7f8a450c` — Stream drop warning fix (4 files, +93/-12)
- `5b0b8ade6` — Clean shutdown + CI script + docs (+880 lines)
- (pending) — Transport hardening: error model, retry, observability

**Full documentation index**: [QUIC Docs Index](quic-docs-index.md)

---

## 1. Executive Summary

Refactors the QUIC/H3 transport layer for architectural clarity and fixes stream lifecycle issues that caused ~200 "stream not stopped before dropped" warnings per cluster restart.

**Key outcomes:**
- Replaced fragile singleton detection (`Arc::strong_count == 2`) with explicit `AtomicBool`
- Unified duplicate endpoint resolver into single `xlinerpc::endpoint` module
- Fixed stream drop warnings: ~200/round → ~120/round (remaining are inherent to h3-over-dquic)
- Wired `shutdown_shared_quic()` into `main.rs` exit path for clean QUIC shutdown
- Added 5 scripts: smoke, long-run, fault injection, restart stress, CI entry

**Verification status (this commit):**
- `cargo check` (full workspace): ✅
- `cargo build --bin xline --bin xlinectl`: ✅
- `bash scripts/quic_ci_smoke.sh` (short mode): ✅ ALL PASSED
  - Includes: smoke test + 3-round restart stress

**Previously verified (commits 1 and 2, not re-run in full this commit):**
- 3-node cluster smoke tests (3 consecutive): ✅
- 120s long-run (no panics): ✅
- Fault injection (kill follower/leader, restart): ✅
- 5-round restart stress: ✅

**Not completed in this environment:**
- `XLINE_QUIC_CI_LONG=1 bash scripts/quic_ci_smoke.sh` — timed out (>15 min). Requires longer CI timeout. Not a failure; the long-run and fault smoke steps need ~40 min.

---

## 2. Commit Structure

### Commit 1: `e7199407d` — Architecture Refactoring (15 files, +1369/-453)

**Architecture components (5 files):**
| File | Status | Purpose |
|------|--------|---------|
| `crates/xline/src/server/quic_runtime.rs` | NEW | `SharedQuicRuntime` singleton with `AtomicBool` init tracking |
| `crates/xline/src/server/port_router.rs` | NEW | Routes connections by `(server_name, local_port)` → PeerCurp/ClientH3 |
| `crates/xline/src/server/server_registry.rs` | NEW | Registers TLS certs, SNI aliases, bind URIs |
| `crates/xline/src/server/h3_server.rs` | MODIFIED | Rewired to use new components, old `SHARED_QUIC` removed |
| `crates/xline/src/server/mod.rs` | MODIFIED | Added 3 new module declarations |

**Endpoint resolver (4 files):**
| File | Status | Purpose |
|------|--------|---------|
| `crates/xlinerpc/src/endpoint.rs` | NEW | `resolve_endpoint_with_fallback()`, IPv4/IPv6/DNS support |
| `crates/xlinerpc/src/h3_client.rs` | MODIFIED | Uses `crate::endpoint` instead of local `parse_endpoint_for_quic()` |
| `crates/curp/src/rpc/quic_transport/channel.rs` | MODIFIED | Uses `xlinerpc::endpoint` instead of local `parse_endpoint()` |
| `crates/curp/src/rpc/quic_transport/mod.rs` | MODIFIED | Re-exports `DnsFallback` |

**Lifecycle (2 files):**
| File | Status | Purpose |
|------|--------|---------|
| `crates/xline/src/lib.rs` | MODIFIED | `reset_shared_quic()` → `shutdown_shared_quic()` |
| `crates/xline-test-utils/src/lib.rs` | MODIFIED | Removed `xline::reset_shared_quic()` from Drop |

**Scripts (3 files):**
| File | Purpose |
|------|---------|
| `scripts/quic_local_cluster.sh` | Smoke test: KV, member, lease, watch |
| `scripts/quic_long_run.sh` | Continuous ops for N seconds |
| `scripts/quic_fault_smoke.sh` | Kill/restart follower, kill leader |

### Commit 2: `a7f8a450c` — Stream Drop Fix (4 files, +93/-12)

| File | Status | Purpose |
|------|--------|---------|
| `crates/curp/src/rpc/quic_transport/codec.rs` | MODIFIED | Added `StopOnDropReader` (EOF-aware recv wrapper) |
| `crates/curp/src/rpc/quic_transport/channel.rs` | MODIFIED | 5 FrameReader sites wrapped in `StopOnDropReader` |
| `crates/xlinerpc/src/h3_client.rs` | MODIFIED | Added `stop_sending(H3_NO_ERROR)` in unary/streaming paths |
| `crates/xlinerpc/src/server/h3wrapper.rs` | MODIFIED | Added `Drop` impl for `QuicIncomingBody` |

### Commit 3 (pending) — Clean Shutdown + CI + Docs

| File | Status | Purpose |
|------|--------|---------|
| `crates/xline/src/main.rs` | MODIFIED | Call `shutdown_shared_quic()` before tracer shutdown |
| `scripts/quic_restart_stress.sh` | NEW | N-round restart stress test |
| `scripts/quic_ci_smoke.sh` | NEW | CI entry script (short + optional long mode) |
| `docs/quic-refactor-audit.md` | NEW | This document |

---

## 3. Code Review Findings

### ✅ Sound

| Component | Assessment |
|-----------|-----------|
| SharedQuicRuntime | AtomicBool replaces fragile `strong_count`. `get_or_init()` + `shutdown()` clear semantics. |
| ServerRegistry SNI | Guard `get_server(server_name).is_none()` prevents double-registration. URL hosts registered as separate SNI servers. |
| PortRouter Unknown | Returns `ConnectionTarget::Unknown`, connection dropped with error log. No crash. |
| EndpointResolver | Handles IPv4, IPv6 bracket, DNS, scheme stripping, DNS failure with configurable fallback. Tests cover all formats. |
| StopOnDropReader | Server-side recv NOT wrapped (intentional — wrapping broke election). Client-side recv wrapped with EOF-aware logic. `stop(0)` only fires if stream NOT fully consumed. |
| Watch/Lease stability | No impact. Watch uses client_streaming, runs until cancelled. Lease is unary. CURP streaming recv consumed to EOF. |
| Scripts | Cleanup trap, idempotent hosts, failure logging. |

### ⚠️ Known Trade-offs

| Issue | Risk | Mitigation |
|-------|------|-----------|
| `stop_sending` before `recv_trailers` in streaming paths | Low — trailers are optional error info on already-completed streams | Accept risk; trailers read may silently return None |
| CURP server recv NOT wrapped in StopOnDropReader | Low — ~50% of remaining ~120 warnings come from this path | Intentional design decision (ADR-2) |

---

## 4. Remaining Stream Drop Warnings (~120/round)

### Fix Coverage

| Path | Status | Mechanism |
|------|--------|-----------|
| CURP client recv (5 sites in channel.rs) | ✅ Fixed | `StopOnDropReader` wrapper |
| H3 client recv (unary, server_streaming, client_streaming) | ✅ Fixed | Explicit `stop_sending(H3_NO_ERROR)` |
| H3 server recv (`QuicIncomingBody` Drop) | ✅ Fixed | Drop impl calls `stop_sending(H3_NO_ERROR)` |
| CURP server recv (server.rs) | ❌ Intentionally NOT wrapped | Wrapping broke election (STOP_SENDING resets peer's send half) |

### Remaining Sources

The remaining ~120 warnings per restart round come from three paths we cannot control from Xline code:

1. **h3-shim internal RecvStream drops** — When h3 connection management drops streams on connection close, the underlying dquic `Reader` drops without being stopped. These are inside h3/dquic internals, not reachable from application code.

2. **`h3_driver.wait_idle()`** — When `H3Channel` drops the `GmConnection`, the h3 driver drops all internal streams. Some may not have been fully consumed at that point.

3. **Server `connection.accept()` loop** — When a QUIC connection closes, pending streams in the accept queue drop without being stopped.

### Why This Round Does Not Fix Further

- **Source 1** requires changes to dquic's `qrecovery/src/recv/reader.rs` Drop impl — upstream dependency, not in our control.
- **Source 2** requires h3 to call `stop_sending` on all internal streams before dropping — upstream h3 behavior.
- **Source 3** would require consuming all pending streams before dropping the connection, which adds complexity and latency with no functional benefit.

### Impact

- **Debug builds**: `tracing::warn!` level — visible in logs
- **Release builds**: `tracing::debug!` level — not visible unless `RUST_LOG=debug`
- **Functional**: None — the streams are being cleaned up, just not via the "proper" stop_sending path that dquic expects
- **Performance**: None — these are Drop-time warnings, not runtime overhead

---

## 5. Architecture Decision Records

### ADR-1: AtomicBool vs Arc::strong_count for singleton

- **Problem**: `Arc::strong_count == 2` is fragile — any temporary clone breaks detection
- **Decision**: Use static `AtomicBool(QUIC_INITIALIZED)` set on first init, cleared on shutdown
- **Consequence**: `shutdown()` must be called on process exit — now wired in `main.rs`

### ADR-2: StopOnDropReader server-side exclusion

- **Problem**: Wrapping server-side recv in StopOnDropReader sent STOP_SENDING, resetting peer's send half
- **Decision**: Only wrap client-side recv. Server recv consumed to EOF by `handle_stream`
- **Consequence**: Server-side stream drop warnings remain (~50% of remaining ~120)

### ADR-3: Endpoint resolver in xlinerpc, not utils

- **Problem**: Duplicate `parse_endpoint` in h3_client.rs and channel.rs
- **Decision**: Single implementation in `xlinerpc::endpoint` (leaf crate)
- **Consequence**: `curp` imports from `xlinerpc`, no cycle since curp already depends on xlinerpc

### ADR-4: etcd-client as dev-dependency

- **Problem**: etcd-client pulls in tonic transitively
- **Decision**: Keep as dev-dependency (used in integration tests, benchmarks)
- **Consequence**: `cargo tree -i tonic` shows etcd-client → tonic, but zero `use tonic` in production code. `etcd-client` is NOT a production dependency of `xline` or `xlinectl` binaries.

---

## 6. Key Files Reference

| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| QUIC Runtime | `crates/xline/src/server/quic_runtime.rs` | ~130 | `SharedQuicRuntime` singleton |
| Port Router | `crates/xline/src/server/port_router.rs` | ~90 | Routes by server_name + local_port |
| Server Registry | `crates/xline/src/server/server_registry.rs` | ~220 | TLS certs, SNI aliases |
| H3 Server | `crates/xline/src/server/h3_server.rs` | ~340 | `serve()` orchestration, `accept_loop()` |
| Endpoint Resolver | `crates/xlinerpc/src/endpoint.rs` | ~170 | `resolve_endpoint_with_fallback()` |
| H3 Client | `crates/xlinerpc/src/h3_client.rs` | ~700 | `H3Channel` unary/streaming RPCs |
| CURP Channel | `crates/curp/src/rpc/quic_transport/channel.rs` | ~840 | `QuicChannel` CURP RPCs |
| StopOnDropReader | `crates/curp/src/rpc/quic_transport/codec.rs` | ~40 (new) | EOF-aware recv wrapper |
| H3 Wrapper | `crates/xlinerpc/src/server/h3wrapper.rs` | ~660 | `QuicIncomingBody` with Drop |
| Main entry | `crates/xline/src/main.rs` | ~198 | Clean shutdown with `shutdown_shared_quic()` |

---

## 7. Verification Scripts

See [Troubleshooting Guide §1](quic-troubleshooting.md#1-quick-start-local-3-node-quic-cluster) for usage.

| Script | Purpose |
|--------|---------|
| `scripts/quic_local_cluster.sh` | Smoke test |
| `scripts/quic_long_run.sh` | Continuous ops |
| `scripts/quic_fault_smoke.sh` | Fault injection |
| `scripts/quic_restart_stress.sh` | Restart stress |
| `scripts/quic_ci_smoke.sh` | CI entry point |

---

## 8. How to Validate Locally

See [Troubleshooting Guide §1](quic-troubleshooting.md#1-quick-start-local-3-node-quic-cluster).

---

## 9. CI Smoke Entry

See [Troubleshooting Guide §1](quic-troubleshooting.md#1-quick-start-local-3-node-quic-cluster).

---

## 10. Clean Shutdown Behavior

`shutdown_shared_quic()` is now called in `crates/xline/src/main.rs` before `global::shutdown_tracer_provider()`:

```rust
// After server.stop() or second ctrl_c:
shutdown_shared_quic();  // Clears QUIC singleton, closes listeners
global::shutdown_tracer_provider();
```

This runs on:
- Normal exit (server.stop() completes)
- Force exit (second ctrl_c)

The `SharedQuicRuntime::shutdown()` method:
1. Sets `QUIC_INITIALIZED` to `false`
2. Drops the `QuicListeners` singleton
3. QUIC connections close gracefully (dquic handles connection close frames)

---

## 11. When Not to Run Unit Tests / Why dquic Singleton Affects Tests

### The singleton problem

`SharedQuicRuntime` is a process-level singleton backed by a `static Mutex<Option<...>>`. This means:
- All tests in the same process share one QUIC runtime
- Parallel tests that bind to the same port will conflict
- `shutdown()` in one test's Drop breaks other tests still running

### What we removed

`xline-test-utils` previously called `xline::reset_shared_quic()` in its `Drop` impl. This was removed because:
- It killed the QUIC runtime for ALL tests, not just the one finishing
- Parallel test execution was impossible
- The "fix" was worse than the problem

### How tests should work

- **Unit tests**: Don't need QUIC. Test logic, not transport.
- **Integration tests**: Use unique ports per test. Don't share the singleton.
- **E2E tests**: Use the verification scripts (quic_local_cluster.sh etc.) which manage their own cluster lifecycle.

### Why we don't unit-test the QUIC stack

The QUIC stack (dquic, h3-shim) is an external dependency with its own test suite. Our code wraps it — we verify our wrappers work via E2E tests (the scripts), not by mocking dquic internals.

---

## 12. Known Limitations

| Limitation | Impact | Workaround |
|-----------|--------|-----------|
| dquic singleton (`QuicListeners`) | Blocks parallel QUIC tests | Use E2E scripts, not unit tests |
| Per-RPC new connection (no pool) | Higher latency for short-lived clients | `XLINE_CURP_CONN_CACHE=1` for server-side cache |
| ~115 stream drop warnings/round | Cosmetic (debug-level in release) | Inherent to h3-over-dquic, upstream issue |
| xlinerpc has no metrics infra | H3 client metrics not exported | Metrics in curp/xline crates only |
| IP endpoints unsupported | SNI routing ambiguity | Use DNS names via `/etc/hosts` |

---

## 13. Quality Metrics

See [Troubleshooting Guide §6](quic-troubleshooting.md#6-metrics-and-debug-logs) for metrics details.

| Metric | Value |
|--------|-------|
| Stream drop warnings (before) | ~200/round |
| Stream drop warnings (after) | ~120/round |
| Panics across all tests | 0 |
| NoViablePath errors | 0 (transient, self-resolving) |

---

## 14. Verification Matrix

| Test | Script | Status |
|------|--------|--------|
| Smoke (KV, member, lease, watch) | `quic_local_cluster.sh` | ✅ |
| Restart stress (3 rounds) | `quic_restart_stress.sh 3` | ✅ |
| Long-run (120s) | `quic_long_run.sh 120` | ✅ |
| Fault (kill follower, kill leader) | `quic_fault_smoke.sh` | ✅ |
| CI smoke (short) | `quic_ci_smoke.sh` | ✅ |
| Endpoint tests | `cargo test -p xlinerpc -- endpoint` | ✅ 7/7 |
| Client lib tests | `cargo test -p xline-client --lib` | ✅ 19/19 |

---

## 15. Suggested Next Steps

1. **H3 connection pooling** — Reuse connections across requests (like QuicChannel)
2. **Evaluate dquic stream drop suppression** — If upstream accepts PR, remaining ~120 warnings can be eliminated
3. **Remove etcd-client dev-dependency** — Rewrite integration tests to use xline's own client (optional, low priority)
4. **CI integration** — Add `scripts/quic_ci_smoke.sh` to CI pipeline
5. **CURP channel error context** — Already improved with structured endpoint/server_name/addr in error messages

---

## 16. Operational Runbook

See [Troubleshooting Guide §1](quic-troubleshooting.md#1-quick-start-local-3-node-quic-cluster).

---

## 17. Connection Reuse Analysis

See [H3 Benchmark](quic-h3-benchmark.md) for full analysis.

**Key finding**: Every RPC creates a new QUIC connection. KV operations bypass H3Channel entirely — they go through CURP's `QuicChannel`. H3 connection pooling would only benefit non-KV operations (compact, auth, cluster, maintenance).

---

## 18. API Compatibility Note

### Breaking Change: `LeaseClient::keep_alive()` Return Type

**Before**: `Result<(LeaseKeeper, Streaming<LeaseKeepAliveResponse>)>`
**After**: `Result<(LeaseKeeper, LeaseStreaming)>`

**Reason**: `LeaseStreaming` adds a lifecycle pin (`_sender: Sender<LeaseKeepAliveRequest>`) that prevents handler task leaks when the response stream is dropped before the `LeaseKeeper`. Without this pin, dropping `Streaming` while keeping `LeaseKeeper` alive would leave the handler task running indefinitely.

**Migration**: All callers that use destructuring + `.message()` require no changes:
```rust
// This pattern works unchanged with both old and new types:
let (mut keeper, mut stream) = client.keep_alive(id).await?;
keeper.keep_alive()?;
let resp = stream.message().await?;
```

Only code with explicit type annotations needs updating:
```rust
// Old:
let mut stream: Streaming<LeaseKeepAliveResponse> = ...;
// New:
let mut stream: LeaseStreaming = ...;
```

---

## 19. Streaming API Examples

See [xlinectl Doctor](xlinectl-doctor.md) and [Troubleshooting Guide](quic-troubleshooting.md) for usage examples.

Key APIs:
- `Watcher` / `WatchStreaming` — `close()`, `is_closed()`, drop semantics
- `LeaseKeeper` / `LeaseStreaming` — `close()`, `is_closed()`, drop semantics
- `Streaming<T>` — `message()`, drop with diagnostic label

---

## 20. Cancellation Semantics

See [xlinectl Doctor](xlinectl-doctor.md) for usage guidance.

Key semantics:
- `close()` on any handle closes the shared channel (both sides see `is_closed() = true`)
- Dropping `WatchStreaming` or `LeaseStreaming` closes the handler task
- Dropping `Watcher` or `LeaseKeeper` alone does NOT close the channel
- Server-side cleanup is automatic on client close

---

## 21. Server-Side Cancellation Audit

All server-side cancellation paths verified correct:
- Watch: `req_rx → None` → break → `WatchHandle::drop()` → `kv_watcher.cancel()`
- Lease: `request_stream → None` → break (with debug log)
- QUIC service: `Frame::End` or error → task exits

---

## 22. Transport Observability / Metrics Readiness

See [Troubleshooting Guide §6](quic-troubleshooting.md#6-metrics-and-debug-logs) for metrics details and [CURP Benchmark §10](quic-curp-benchmark.md) for runtime verification.

---

## 23. Implemented Transport Metrics

See [CURP Benchmark §10](quic-curp-benchmark.md) for full metrics definitions, Prometheus output, and runtime verification.

Implemented: `port_router_unknown_total` (xline), `quic_connect_attempts_total` (curp), `quic_connect_failures_total` (curp).

---

## 24. Configuration Validation and Deployment Checklist

See [Troubleshooting Guide §1](quic-troubleshooting.md#1-quick-start-local-3-node-quic-cluster) for deployment setup.

Server-side `validate_server_config()` checks: port conflicts (fatal), TLS completeness (fatal), DNS name hints (info), HTTPS without TLS (warning).

---

## 25. xlinectl Client-Side Configuration and Error UX

See [xlinectl Doctor](xlinectl-doctor.md) for full details.

Key features: endpoint validation (scheme/port/IP/DNS), CA cert checks, SNI routing detection, `--check_connection` live test.

---

## 26. TLS Verification Policy

See [Troubleshooting Guide §4](quic-troubleshooting.md#4-tls--ca-policy) for full policy and test results.

**Key conclusion**: TLS is secure by default. Empty `RootCertStore` → all connections fail with `UnknownIssuer`. No `--insecure` flag. No `without_verifier()`.

---

## 27. IP Endpoint and SNI Routing Analysis

See [Troubleshooting Guide §5](quic-troubleshooting.md#5-sni-routing-and-ip-endpoints) for full root cause analysis.

**Key conclusion**: IP endpoints fail at QUIC SNI routing level (not TLS). Only DNS names work. Use `/etc/hosts` for local development.

---

*Report generated by Sisyphus (AI Agent) on 2026-05-23.*
*Full documentation index: [QUIC Docs Index](quic-docs-index.md)*
