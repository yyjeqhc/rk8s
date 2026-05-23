#!/usr/bin/env bash
# quic_curp_benchmark.sh — CURP QuicChannel connection cost benchmark
#
# Measures QUIC connection overhead for CURP propose path:
#   - xlinectl sequential put+get (short-lived client per invocation)
#   - Long-running client benchmark (single process, many ops)
#   - quic_connect_attempts_total delta from /metrics
#
# Usage:
#   ./scripts/quic_curp_benchmark.sh [--requests N] [--concurrency C] [--keep-cluster] [--skip-build]
#
# Requirements:
#   - Ports 2379-2384, 9100-9102 free
#   - /etc/hosts entries for server0/server1/server2
#   - fixtures/ directory with TLS certs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$PROJECT_ROOT/fixtures"
LOG_DIR="/tmp/xline-curp-bench"
METRICS_BASE_PORT=9100
CLIENT_BASE_PORT=2379
PEER_BASE_PORT=2380

# Defaults
NUM_REQUESTS=20
CONCURRENCY=1
KEEP_CLUSTER=0
SKIP_BUILD=0
OUTPUT_DIR=""

# Server definitions
SERVERS=("server0" "server1" "server2")
declare -A SERVER_CLIENT_PORT SERVER_PEER_PORT SERVER_METRICS_PORT
SERVER_CLIENT_PORT[server0]=2379 SERVER_PEER_PORT[server0]=2380 SERVER_METRICS_PORT[server0]=9100
SERVER_CLIENT_PORT[server1]=2381 SERVER_PEER_PORT[server1]=2382 SERVER_METRICS_PORT[server1]=9101
SERVER_CLIENT_PORT[server2]=2383 SERVER_PEER_PORT[server2]=2384 SERVER_METRICS_PORT[server2]=9102

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests) NUM_REQUESTS="$2"; shift 2 ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        --keep-cluster) KEEP_CLUSTER=1; shift ;;
        --skip-build) SKIP_BUILD=1; shift ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help) echo "Usage: $0 [--requests N] [--concurrency C] [--keep-cluster] [--skip-build] [--output-dir DIR]"; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="$LOG_DIR/results-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$OUTPUT_DIR"

XLINE_BIN="$PROJECT_ROOT/target/debug/xline"
XLINECTL_BIN="$PROJECT_ROOT/target/debug/xlinectl"
ENDPOINT="https://server0:2379"
CA_CERT="$FIXTURES_DIR/ca.crt"

# ─── Helpers ─────────────────────────────────────────────────────────

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }
die() { echo "FATAL: $*" >&2; exit 1; }

cleanup() {
    if [[ $KEEP_CLUSTER -eq 1 ]]; then
        log "Keeping cluster (--keep-cluster)"
        return
    fi
    log "Stopping cluster..."
    pkill -f "xline.*--name server" 2>/dev/null || true
    sleep 2
    pkill -9 -f "xline.*--name server" 2>/dev/null || true
    rm -rf /tmp/xline-node-* /tmp/xline-curp-bench 2>/dev/null || true
}

check_ports() {
    local ports=(2379 2380 2381 2382 2383 2384 9100 9101 9102)
    for port in "${ports[@]}"; do
        if ss -tlnp 2>/dev/null | grep -q ":${port} " || netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
            die "Port $port is in use"
        fi
    done
}

check_hosts() {
    for s in "${SERVERS[@]}"; do
        if ! getent hosts "$s" >/dev/null 2>&1; then
            die "/etc/hosts missing entry for $s"
        fi
    done
}

scrape_metric() {
    local port="$1" metric="$2"
    curl -s "http://127.0.0.1:${port}/metrics" 2>/dev/null | grep "^${metric}" | head -1 | awk '{print $NF}'
}

scrape_all_metrics() {
    local label="${1:-}"
    log "Scraping metrics $label..."
    local attempts=0
    for s in "${SERVERS[@]}"; do
        local p="${SERVER_METRICS_PORT[$s]}"
        local a=$(scrape_metric "$p" "quic_connect_attempts_total")
        local f=$(scrape_metric "$p" "quic_connect_failures_total")
        a=${a:-0}; f=${f:-0}
        attempts=$((attempts + a))
        log "  $s: attempts=$a failures=$f"
    done
    echo "$attempts"
}

start_cluster() {
    log "Starting 3-node cluster..."
    mkdir -p "$LOG_DIR"
    check_ports

    local peers_arg=""
    for i in "${!SERVERS[@]}"; do
        local s="${SERVERS[$i]}"
        local pp="${SERVER_PEER_PORT[$s]}"
        if [[ $i -gt 0 ]]; then peers_arg+=","; fi
        peers_arg+="${s}=https://${s}:${pp}"
    done

    for s in "${SERVERS[@]}"; do
        local client_port="${SERVER_CLIENT_PORT[$s]}"
        local peer_port="${SERVER_PEER_PORT[$s]}"
        local metrics_port="${SERVER_METRICS_PORT[$s]}"
        local data_dir="/tmp/xline-curp-bench/$s"
        mkdir -p "$data_dir"

        "$XLINE_BIN" \
            --name "$s" \
            --members "$peers_arg" \
            --data-dir "$data_dir/data" \
            --storage-engine rocksdb \
            --client-listen-urls "https://127.0.0.1:${client_port}" \
            --peer-listen-urls "https://127.0.0.1:${peer_port}" \
            --client-advertise-urls "https://${s}:${client_port}" \
            --peer-advertise-urls "https://${s}:${peer_port}" \
            --peer-cert-path "$FIXTURES_DIR/${s}.crt" \
            --peer-key-path "$FIXTURES_DIR/${s}.key" \
            --peer-ca-cert-path "$FIXTURES_DIR/ca.crt" \
            --client-ca-cert-path "$FIXTURES_DIR/ca.crt" \
            --metrics-enable \
            --metrics-port "$metrics_port" \
            > "$LOG_DIR/${s}.log" 2>&1 &
    done

    log "Waiting for leader election..."
    sleep 6

    local ok=0
    for attempt in $(seq 1 10); do
        if "$XLINECTL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put bench-warmup value >/dev/null 2>&1; then
            ok=1; break
        fi
        sleep 2
    done
    if [[ $ok -ne 1 ]]; then
        cat "$LOG_DIR/server0.log" | tail -20
        die "Cluster failed to start"
    fi
    log "Cluster ready"
}

# ─── Benchmark: xlinectl sequential (short-lived client) ──────────────

bench_xlinectl_sequential() {
    local n="$1"
    log "=== xlinectl sequential put+get × $n ==="
    local csv="$OUTPUT_DIR/xlinectl_sequential.csv"
    echo "op,latency_ns" > "$csv"

    local start_attempts=$(scrape_all_metrics "before sequential")

    local success=0
    local failure=0
    local start_ns=$(date +%s%N)
    for i in $(seq 1 "$n"); do
        local key="bench-seq-$i"
        local t0=$(date +%s%N)
        if "$XLINECTL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put "$key" "value-$i" >/dev/null 2>&1; then
            success=$((success + 1))
        else
            failure=$((failure + 1))
            log "  WARN: put $key failed"
        fi
        local t1=$(date +%s%N)
        echo "put,$((t1 - t0))" >> "$csv"

        t0=$(date +%s%N)
        if "$XLINECTL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" get "$key" >/dev/null 2>&1; then
            success=$((success + 1))
        else
            failure=$((failure + 1))
            log "  WARN: get $key failed"
        fi
        t1=$(date +%s%N)
        echo "get,$((t1 - t0))" >> "$csv"
    done
    local end_ns=$(date +%s%N)
    local total_ms=$(( (end_ns - start_ns) / 1000000 ))

    local end_attempts=$(scrape_all_metrics "after sequential")
    local conn_delta=$((end_attempts - start_attempts))
    local ops=$((n * 2))
    local conns_per_op=$(awk "BEGIN{printf \"%.2f\", $conn_delta / $ops}")
    log "  Total: ${total_ms}ms for $ops ops ($n put + $n get)"
    log "  Success: $success  Failure: $failure"
    log "  QUIC connections: $conn_delta ($conns_per_op per op)"
    log "  Throughput: $(awk "BEGIN{printf \"%.1f\", $ops * 1000 / $total_ms}") ops/s"

    return $failure
}

# ─── Benchmark: xlinectl concurrent ──────────────────────────────────

bench_xlinectl_concurrent() {
    local n="$1" c="$2"
    log "=== xlinectl concurrent put × $n × $c ==="
    local csv="$OUTPUT_DIR/xlinectl_concurrent.csv"
    echo "worker,op,latency_ns" > "$csv"

    local start_attempts=$(scrape_all_metrics "before concurrent")
    local start_ns=$(date +%s%N)

    local worker_exit_dir
    worker_exit_dir=$(mktemp -d)
    for worker in $(seq 1 "$c"); do
        (
            local w_success=0
            local w_failure=0
            for i in $(seq 1 "$n"); do
                local key="bench-conc-w${worker}-$i"
                local t0=$(date +%s%N)
                if "$XLINECTL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put "$key" "value-$i" >/dev/null 2>&1; then
                    w_success=$((w_success + 1))
                else
                    w_failure=$((w_failure + 1))
                fi
                local t1=$(date +%s%N)
                echo "$worker,put,$((t1 - t0))"
            done
            echo "$w_success $w_failure" > "$worker_exit_dir/w${worker}.txt"
            exit $w_failure
        ) >> "$csv" &
    done
    wait

    local end_ns=$(date +%s%N)
    local total_ms=$(( (end_ns - start_ns) / 1000000 ))

    local success=0
    local failure=0
    for f in "$worker_exit_dir"/*.txt; do
        if [[ -f "$f" ]]; then
            read s fl < "$f"
            success=$((success + s))
            failure=$((failure + fl))
        fi
    done
    rm -rf "$worker_exit_dir"

    local end_attempts=$(scrape_all_metrics "after concurrent")
    local conn_delta=$((end_attempts - start_attempts))
    local ops=$((n * c))
    local conns_per_op=$(awk "BEGIN{printf \"%.2f\", $conn_delta / $ops}")

    log "  Total: ${total_ms}ms for $ops ops ($c workers × $n ops)"
    log "  Success: $success  Failure: $failure"
    log "  QUIC connections: $conn_delta ($conns_per_op per op)"
    log "  Throughput: $(awk "BEGIN{printf \"%.1f\", $ops * 1000 / $total_ms}") ops/s"

    return $failure
}

# ─── Benchmark: long-running client ──────────────────────────────────

bench_long_running() {
    local n="$1"
    log "=== Long-running client put+get × $n ==="

    local start_attempts=$(scrape_all_metrics "before long-running")

    local rc=0
    local start_ns=$(date +%s%N)
    timeout 300 "$PROJECT_ROOT/target/debug/examples/client_kv_benchmark" \
        --requests "$n" \
        2>"$OUTPUT_DIR/long_running_stderr.log" || rc=$?
    local end_ns=$(date +%s%N)
    local total_ms=$(( (end_ns - start_ns) / 1000000 ))

    if [[ $rc -ne 0 ]]; then
        log "  WARN: long-running client exited with code $rc (timeout or failure)"
    fi

    local end_attempts=$(scrape_all_metrics "after long-running")
    local conn_delta=$((end_attempts - start_attempts))
    local ops=$((n * 2))
    local conns_per_op="N/A"
    if [[ $ops -gt 0 ]]; then
        conns_per_op=$(awk "BEGIN{printf \"%.2f\", $conn_delta / $ops}")
    fi

    log "  Total: ${total_ms}ms for $ops ops"
    log "  Exit code: $rc"
    log "  QUIC connections: $conn_delta ($conns_per_op per op)"
    log "  Throughput: $(awk "BEGIN{printf \"%.1f\", $ops * 1000 / $total_ms}") ops/s"

    return $rc
}

# ─── Main ────────────────────────────────────────────────────────────

main() {
    log "CURP QuicChannel Benchmark"
    log "  Requests: $NUM_REQUESTS"
    log "  Concurrency: $CONCURRENCY"
    log "  Output: $OUTPUT_DIR"

    trap cleanup EXIT

    if [[ $SKIP_BUILD -eq 0 ]]; then
        log "Building..."
        cargo build --bin xline --bin xlinectl --example client_kv_benchmark 2>&1 | tail -3
    fi

    check_hosts
    start_cluster

    local total_failures=0

    # Short-lived client benchmark
    local rc=0
    bench_xlinectl_sequential "$NUM_REQUESTS" || rc=$?
    total_failures=$((total_failures + rc))

    if [[ $CONCURRENCY -gt 1 ]]; then
        rc=0
        bench_xlinectl_concurrent "$NUM_REQUESTS" "$CONCURRENCY" || rc=$?
        total_failures=$((total_failures + rc))
    fi

    # Long-running client benchmark (if binary exists)
    if [[ -x "$PROJECT_ROOT/target/debug/examples/client_kv_benchmark" ]]; then
        rc=0
        bench_long_running "$NUM_REQUESTS" || rc=$?
        total_failures=$((total_failures + rc))
    else
        log "Skipping long-running benchmark (binary not found)"
    fi

    log "=== Results in $OUTPUT_DIR ==="
    ls -la "$OUTPUT_DIR/"

    if [[ $total_failures -gt 0 ]]; then
        log "FAILED: $total_failures benchmark operations failed"
        exit 1
    fi
    log "ALL BENCHMARKS PASSED"
}

main "$@"
