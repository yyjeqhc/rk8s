# QUIC/H3 Documentation Index

This document is the central index for all QUIC/H3-related documentation in Xline.

## Which Doc Should I Read?

| I want to... | Read this |
|--------------|-----------|
| Understand the QUIC/H3 architecture refactoring | [QUIC Refactor Audit](quic-refactor-audit.md) |
| Diagnose a connection, TLS, or SNI issue | [Troubleshooting Guide](quic-troubleshooting.md) |
| Run `xlinectl doctor` for endpoint checks | [xlinectl Doctor](xlinectl-doctor.md) |
| Understand H3 connection lifecycle / pooling | [H3 Benchmark](quic-h3-benchmark.md) |
| Understand CURP connection cost / cache | [CURP Benchmark](quic-curp-benchmark.md) |

## Architecture Docs

| Document | Focus | Key Content |
|----------|-------|-------------|
| [QUIC Refactor Audit](quic-refactor-audit.md) | Core architecture and audit | SharedQuicRuntime, ServerRegistry, PortRouter, EndpointResolver, StopOnDropReader, cancellation semantics, API compatibility |

## Troubleshooting and Diagnostics

| Document | Focus | Key Content |
|----------|-------|-------------|
| [Troubleshooting Guide](quic-troubleshooting.md) | Operational troubleshooting | Quick start, failure matrix, TLS/CA policy, SNI routing, metrics, debug logs |
| [xlinectl Doctor](xlinectl-doctor.md) | Client-side diagnostics | Endpoint checks, TLS CA, SNI routing, experimental features, exit codes |

## Benchmark and Performance

| Document | Focus | Key Content |
|----------|-------|-------------|
| [H3 Benchmark](quic-h3-benchmark.md) | H3 client lifecycle | Connection per RPC, KV bypasses H3Channel, pool feasibility |
| [CURP Benchmark](quic-curp-benchmark.md) | CURP connection cost | 3 connections per KV op, env/CLI-gated cache, server vs client metrics |
| [CURP Cache Soak](quic-curp-cache-soak.md) | Cache stability verification | 120s long run, restart stress, fault injection, metrics |

## Experimental Features

| Feature | Env Var | CLI Flag | Default | Doc |
|---------|---------|----------|---------|-----|
| CURP connection cache | `XLINE_CURP_CONN_CACHE=1` | `--experimental-curp-connection-cache` | off | [CURP Benchmark §8](quic-curp-benchmark.md) |

## Scripts

| Script | Purpose | Duration |
|--------|---------|----------|
| `scripts/quic_local_cluster.sh` | Smoke test (KV, member, lease, watch) | ~2 min |
| `scripts/quic_restart_stress.sh N` | N rounds of start → ops → stop → verify | ~3 min/round |
| `scripts/quic_long_run.sh N` | Continuous ops for N seconds | N seconds |
| `scripts/quic_fault_smoke.sh` | Kill/restart follower, kill leader | ~3 min |
| `scripts/quic_ci_smoke.sh` | CI entry (build + smoke + restart stress) | ~10 min |
| `scripts/quic_h3_benchmark.sh` | H3 connection benchmark | ~5 min |
| `scripts/quic_curp_benchmark.sh` | CURP connection benchmark | ~3 min |

## Doc Map (Cross-References)

```
quic-docs-index.md  ← you are here
├── quic-refactor-audit.md  (core architecture, §1-§18)
├── quic-troubleshooting.md (operational, failure matrix, TLS, SNI, metrics)
│   └── xlinectl-doctor.md  (client diagnostics, doctor command)
├── quic-h3-benchmark.md    (H3 lifecycle, pool feasibility)
└── quic-curp-benchmark.md  (CURP cost, cache, server/client metrics)
```
