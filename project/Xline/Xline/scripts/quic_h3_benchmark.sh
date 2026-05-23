#!/bin/bash
set -euo pipefail

# =============================================================================
#  Xline H3 Connection Pool Feasibility Benchmark
# =============================================================================
#
#  Measures per-RPC latency and throughput to quantify the cost of the current
#  "one QUIC connection + one H3 session + one driver task per RPC" pattern.
#
#  Usage:
#    bash scripts/quic_h3_benchmark.sh [OPTIONS]
#
#  Options:
#    --requests N       Number of requests per benchmark (default: 1000)
#    --concurrency C    Number of concurrent clients (default: 1)
#    --mode MODE        Benchmark mode: get | put-get | mixed (default: get)
#    --endpoint URL     Client endpoint (default: https://server0:2379)
#    --ca PATH          CA cert path (default: fixtures/ca.crt)
#    --keep-cluster     Don't start/stop cluster (use existing)
#    --skip-build       Skip cargo build
#    --output-dir DIR   Directory for raw data (default: /tmp/xline-bench-XXXXXX)
#
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$PROJECT_DIR/fixtures"

# Defaults
REQUESTS=1000
CONCURRENCY=1
MODE="get"
ENDPOINT="https://server0:2379"
CA_CERT="$FIXTURES_DIR/ca.crt"
KEEP_CLUSTER=false
SKIP_BUILD=false
OUTPUT_DIR=""

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests) REQUESTS="$2"; shift 2 ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        --endpoint) ENDPOINT="$2"; shift 2 ;;
        --ca) CA_CERT="$2"; shift 2 ;;
        --keep-cluster) KEEP_CLUSTER=true; shift ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help)
            head -25 "$0" | tail -20
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Validate mode
case "$MODE" in
    get|put-get|mixed) ;;
    *) echo "ERROR: Invalid mode '$MODE'. Use: get, put-get, mixed"; exit 1 ;;
esac

# Setup
XLINE_BIN="$PROJECT_DIR/target/debug/xline"
XLINETL_BIN="$PROJECT_DIR/target/debug/xlinectl"
DATA_DIR=$(mktemp -d /tmp/xline-bench-XXXXXX)
HOSTS_MARKER="# xline-quic-local-cluster"
CLUSTER_STARTED_BY_US=false

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR=$(mktemp -d /tmp/xline-bench-data-XXXXXX)
fi
mkdir -p "$OUTPUT_DIR"

# Metrics ports (must match start_node)
METRICS_PORTS=(9100 9101 9102)

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    if [ "$CLUSTER_STARTED_BY_US" = true ]; then
        for pid in "$DATA_DIR"/node*.pid; do
            [ -f "$pid" ] && kill "$(cat "$pid")" 2>/dev/null || true
        done
    fi
    rm -rf "$DATA_DIR"
    echo "Data dir: $DATA_DIR removed"
    echo "Benchmark data: $OUTPUT_DIR"
}
trap cleanup EXIT INT TERM

ensure_hosts() {
    local entries=("server0" "server1" "server2")
    local needs_update=false
    for name in "${entries[@]}"; do
        if ! grep -q "$name" /etc/hosts 2>/dev/null; then
            needs_update=true
            break
        fi
    done
    if [ "$needs_update" = true ]; then
        echo "ERROR: /etc/hosts missing entries for server0/server1/server2."
        echo "Add them manually: echo '127.0.0.1 server0 server1 server2' >> /etc/hosts"
        exit 1
    fi
}

check_ports() {
    local ports=(2379 2380 2381 2382 2383 2384)
    local busy=()
    for port in "${ports[@]}"; do
        if ss -tlnp 2>/dev/null | grep -q ":${port} " || \
           netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
            busy+=("$port")
        fi
    done
    if [ ${#busy[@]} -gt 0 ]; then
        echo "ERROR: Ports already in use: ${busy[*]}"
        exit 1
    fi
}

build_binaries() {
    if [ "$SKIP_BUILD" = true ]; then
        echo "=== Skipping build (--skip-build) ==="
        return
    fi
    echo "=== Building xline and xlinectl ==="
    cd "$PROJECT_DIR"
    cargo build --bin xline --bin xlinectl 2>&1 | tail -3
    echo "Build complete."
}

start_node() {
    local idx=$1
    local name="server${idx}"
    local client_port=$((2379 + idx * 2))
    local peer_port=$((2380 + idx * 2))
    local metrics_port="${METRICS_PORTS[$idx]}"
    local node_data="$DATA_DIR/$name"
    mkdir -p "$node_data"

    local peer_urls="https://127.0.0.1:${peer_port}"
    local client_urls="https://127.0.0.1:${client_port}"
    local peer_advertise_urls="https://${name}:${peer_port}"
    local client_advertise_urls="https://${name}:${client_port}"

    local peers_arg=""
    for j in 0 1 2; do
        local pn="server${j}"
        local pp=$((2380 + j * 2))
        if [ $j -eq 0 ]; then
            peers_arg="${pn}=https://${pn}:${pp}"
        else
            peers_arg="${peers_arg},${pn}=https://${pn}:${pp}"
        fi
    done

    "$XLINE_BIN" \
        --name "$name" \
        --members "$peers_arg" \
        --client-listen-urls "$client_urls" \
        --peer-listen-urls "$peer_urls" \
        --client-advertise-urls "$client_advertise_urls" \
        --peer-advertise-urls "$peer_advertise_urls" \
        --storage-engine rocksdb \
        --data-dir "$node_data/data" \
        --peer-cert-path "$FIXTURES_DIR/${name}.crt" \
        --peer-key-path "$FIXTURES_DIR/${name}.key" \
        --peer-ca-cert-path "$FIXTURES_DIR/ca.crt" \
        --client-ca-cert-path "$FIXTURES_DIR/ca.crt" \
        --initial-cluster-state "new" \
        --metrics-enable \
        --metrics-port "$metrics_port" \
        > "$node_data/stdout.log" 2>&1 &

    echo $! > "$node_data/node.pid"
}

wait_for_cluster() {
    echo "=== Waiting for cluster to be ready ==="
    local max_wait=30
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put _bench_health ok 2>/dev/null | grep -q "OK"; then
            "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" delete _bench_health 2>/dev/null || true
            echo "  Cluster ready after ${waited}s"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "  ERROR: Cluster not ready after ${max_wait}s"
    for f in "$DATA_DIR"/server*/stdout.log; do
        echo "  --- $f ---"
        tail -10 "$f" 2>/dev/null || true
    done
    return 1
}

scrape_metrics() {
    local label="$1"
    local out_file="$OUTPUT_DIR/metrics_${label}.txt"
    > "$out_file"
    for i in 0 1 2; do
        local port="${METRICS_PORTS[$i]}"
        echo "=== server${i} (${label}) ===" >> "$out_file"
        curl -s "http://127.0.0.1:${port}/metrics" 2>/dev/null >> "$out_file" || echo "(unavailable)" >> "$out_file"
        echo "" >> "$out_file"
    done
    # Extract quic_connect_attempts_total
    echo "--- quic_connect_attempts_total (${label}) ---"
    grep "quic_connect_attempts_total" "$out_file" 2>/dev/null | head -5 || echo "  (not found)"
}

# =============================================================================
#  Benchmark: Sequential
# =============================================================================
bench_sequential() {
    local n=$REQUESTS
    local latency_file="$OUTPUT_DIR/latency_seq_${MODE}_${n}.csv"
    echo ""
    echo "=== Sequential Benchmark: mode=$MODE, requests=$n ==="

    # Pre-populate a key for get mode
    if [ "$MODE" = "get" ] || [ "$MODE" = "mixed" ]; then
        "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put bench_key bench_value >/dev/null 2>&1
    fi

    local start_ns
    start_ns=$(date +%s%N)

    local success=0
    local fail=0

    for ((i = 1; i <= n; i++)); do
        local req_start
        req_start=$(date +%s%N)

        case "$MODE" in
            get)
                "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" get bench_key >/dev/null 2>&1
                ;;
            put-get)
                "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put "bench_k_${i}" "bench_v_${i}" >/dev/null 2>&1
                "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" get "bench_k_${i}" >/dev/null 2>&1
                ;;
            mixed)
                if [ $((i % 2)) -eq 0 ]; then
                    "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" get bench_key >/dev/null 2>&1
                else
                    "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put "bench_k_${i}" "v" >/dev/null 2>&1
                fi
                ;;
        esac

        local rc=$?
        local req_end
        req_end=$(date +%s%N)

        if [ $rc -eq 0 ]; then
            success=$((success + 1))
            echo "$((req_end - req_start))" >> "$latency_file"
        else
            fail=$((fail + 1))
        fi

        # Progress
        if [ $((i % 100)) -eq 0 ] || [ $i -eq "$n" ]; then
            printf "\r  Progress: %d/%d (ok=%d, fail=%d)" "$i" "$n" "$success" "$fail"
        fi
    done

    local end_ns
    end_ns=$(date +%s%N)
    local total_ms=$(( (end_ns - start_ns) / 1000000 ))

    echo ""
    echo "  Total: ${total_ms}ms, Success: $success, Fail: $fail"
    echo "  Latency data: $latency_file"
}

# =============================================================================
#  Benchmark: Concurrent
# =============================================================================
bench_concurrent() {
    local n=$REQUESTS
    local c=$CONCURRENCY
    local per_client=$((n / c))
    local latency_file="$OUTPUT_DIR/latency_conc_${MODE}_${n}_${c}.csv"
    echo ""
    echo "=== Concurrent Benchmark: mode=$MODE, total=$n, concurrency=$c, per_client=$per_client ==="

    # Pre-populate
    if [ "$MODE" = "get" ] || [ "$MODE" = "mixed" ]; then
        "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put bench_key bench_value >/dev/null 2>&1
    fi

    local client_files=()
    local pids=()
    local start_ns
    start_ns=$(date +%s%N)

    for ((cl = 0; cl < c; cl++)); do
        local cl_file="$OUTPUT_DIR/latency_client_${cl}.csv"
        client_files+=("$cl_file")

        (
            local ok=0
            local fl=0
            for ((j = 1; j <= per_client; j++)); do
                local rs
                rs=$(date +%s%N)
                case "$MODE" in
                    get)
                        "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" get bench_key >/dev/null 2>&1
                        ;;
                    put-get)
                        "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put "conc_${cl}_${j}" "v" >/dev/null 2>&1
                        "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" get "conc_${cl}_${j}" >/dev/null 2>&1
                        ;;
                    mixed)
                        if [ $((j % 2)) -eq 0 ]; then
                            "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" get bench_key >/dev/null 2>&1
                        else
                            "$XLINETL_BIN" --endpoints "$ENDPOINT" --ca_cert_pem_path "$CA_CERT" put "conc_${cl}_${j}" "v" >/dev/null 2>&1
                        fi
                        ;;
                esac
                local rc=$?
                local re
                re=$(date +%s%N)
                if [ $rc -eq 0 ]; then
                    ok=$((ok + 1))
                    echo "$((re - rs))" >> "$cl_file"
                else
                    fl=$((fl + 1))
                fi
            done
            echo "${ok}:${fl}" > "${cl_file}.stats"
        ) &
        pids+=($!)
    done

    # Wait for all clients
    local total_success=0
    local total_fail=0
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    local end_ns
    end_ns=$(date +%s%N)
    local total_ms=$(( (end_ns - start_ns) / 1000000 ))

    # Merge latency files
    > "$latency_file"
    for cl_file in "${client_files[@]}"; do
        cat "$cl_file" >> "$latency_file" 2>/dev/null || true
        if [ -f "${cl_file}.stats" ]; then
            local stats
            stats=$(cat "${cl_file}.stats")
            local ok=${stats%%:*}
            local fl=${stats##*:}
            total_success=$((total_success + ok))
            total_fail=$((total_fail + fl))
        fi
    done

    echo "  Total: ${total_ms}ms, Success: $total_success, Fail: $total_fail"
    echo "  Latency data: $latency_file"
}

# =============================================================================
#  Latency Summary (Python stdlib)
# =============================================================================
summarize_latency() {
    local csv_file="$1"
    if [ ! -f "$csv_file" ] || [ ! -s "$csv_file" ]; then
        echo "  (no latency data)"
        return
    fi

    python3 - "$csv_file" <<'PYEOF'
import sys
import os

path = sys.argv[1]
values = []
with open(path) as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                values.append(int(line))
            except ValueError:
                pass

if not values:
    print("  (no valid latency samples)")
    sys.exit(0)

values.sort()
n = len(values)
total_ns = sum(values)
avg_ns = total_ns / n
min_ns = values[0]
max_ns = values[-1]
p50 = values[int(n * 0.50)]
p95 = values[int(n * 0.95)]
p99 = values[int(n * 0.99)]

def fmt(ns):
    if ns < 1_000_000:
        return f"{ns/1000:.1f}us"
    elif ns < 1_000_000_000:
        return f"{ns/1_000_000:.1f}ms"
    else:
        return f"{ns/1_000_000_000:.2f}s"

print(f"  Samples:  {n}")
print(f"  Min:      {fmt(min_ns)}")
print(f"  Max:      {fmt(max_ns)}")
print(f"  Avg:      {fmt(avg_ns)}")
print(f"  p50:      {fmt(p50)}")
print(f"  p95:      {fmt(p95)}")
print(f"  p99:      {fmt(p99)}")
if total_ns > 0:
    throughput = n / (total_ns / 1_000_000_000)
    print(f"  Throughput: {throughput:.1f} req/s (single-threaded, incl. process spawn)")
PYEOF
}

# =============================================================================
#  Long Stream Sanity
# =============================================================================
long_stream_sanity() {
    echo ""
    echo "=== Long Stream Sanity: watch + lease ==="
    local ep="$ENDPOINT"
    local cert="$CA_CERT"

    # Watch: create, trigger, close
    echo "  Watch test..."
    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" put _watch_target initial >/dev/null 2>&1
    timeout 3 "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" watch _watch_target -- echo "WATCH_EVENT" &
    local wpid=$!
    sleep 0.5
    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" put _watch_target updated >/dev/null 2>&1
    wait $wpid 2>/dev/null || true
    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" delete _watch_target >/dev/null 2>&1
    echo "  Watch: OK"

    # Lease: create, keepalive, revoke
    echo "  Lease test..."
    local lease_out
    lease_out=$("$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" lease grant 5 2>&1)
    local lid
    lid=$(echo "$lease_out" | head -1 | tr -d '[:space:]')
    if [ -n "$lid" ]; then
        "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" lease revoke "$lid" >/dev/null 2>&1
        echo "  Lease: OK (id=$lid)"
    else
        echo "  Lease: WARN (could not extract id)"
    fi
}

# =============================================================================
#  Resource Observation
# =============================================================================
observe_resources() {
    echo ""
    echo "=== Resource Observation ==="

    # Process count
    local xline_pids
    xline_pids=$(pgrep -f "xline.*--name" 2>/dev/null | wc -l)
    echo "  xline processes: $xline_pids"

    # fd count for each xline process
    for pid in $(pgrep -f "xline.*--name" 2>/dev/null); do
        local fd_count
        fd_count=$(ls /proc/$pid/fd 2>/dev/null | wc -l)
        echo "  PID $pid fds: $fd_count"
    done

    # Warning count from logs
    local warn_count=0
    for f in "$DATA_DIR"/server*/stdout.log; do
        if [ -f "$f" ]; then
            local wc
            wc=$(grep -c "stream.*not stopped.*before dropped" "$f" 2>/dev/null || true)
            wc=$(echo "$wc" | tr -d '[:space:]')
            warn_count=$((warn_count + ${wc:-0}))
        fi
    done
    echo "  Stream drop warnings (total): $warn_count"

    local nvp_count=0
    for f in "$DATA_DIR"/server*/stdout.log; do
        if [ -f "$f" ]; then
            local wc
            wc=$(grep -c "No viable network path" "$f" 2>/dev/null || true)
            wc=$(echo "$wc" | tr -d '[:space:]')
            nvp_count=$((nvp_count + ${wc:-0}))
        fi
    done
    echo "  NoViablePath errors: $nvp_count"

    local panic_count=0
    for f in "$DATA_DIR"/server*/stdout.log; do
        if [ -f "$f" ]; then
            local wc
            wc=$(grep -c "panicked at" "$f" 2>/dev/null || true)
            wc=$(echo "$wc" | tr -d '[:space:]')
            panic_count=$((panic_count + ${wc:-0}))
        fi
    done
    echo "  Panics: $panic_count"
}

# =============================================================================
#  Main
# =============================================================================
main() {
    echo "============================================"
    echo "  Xline H3 Connection Pool Feasibility Benchmark"
    echo "============================================"
    echo "  Requests:     $REQUESTS"
    echo "  Concurrency:  $CONCURRENCY"
    echo "  Mode:         $MODE"
    echo "  Endpoint:     $ENDPOINT"
    echo "  CA:           $CA_CERT"
    echo "  Output:       $OUTPUT_DIR"
    echo "============================================"
    echo ""

    ensure_hosts

    if [ "$KEEP_CLUSTER" = false ]; then
        check_ports
        build_binaries

        echo "=== Starting 3-node cluster ==="
        for i in 0 1 2; do
            start_node "$i"
        done
        CLUSTER_STARTED_BY_US=true
        wait_for_cluster
    else
        echo "=== Using existing cluster (--keep-cluster) ==="
    fi

    # Pre-benchmark metrics
    scrape_metrics "before"

    # Run benchmark
    if [ "$CONCURRENCY" -le 1 ]; then
        bench_sequential
        local latency_file="$OUTPUT_DIR/latency_seq_${MODE}_${REQUESTS}.csv"
        echo ""
        echo "--- Latency Summary ---"
        summarize_latency "$latency_file"
    else
        bench_concurrent
        local latency_file="$OUTPUT_DIR/latency_conc_${MODE}_${REQUESTS}_${CONCURRENCY}.csv"
        echo ""
        echo "--- Latency Summary ---"
        summarize_latency "$latency_file"
    fi

    # Post-benchmark metrics
    scrape_metrics "after"

    # Compute metrics delta
    echo ""
    echo "--- quic_connect_attempts_total delta ---"
    local before_total after_total
    before_total=$(grep "quic_connect_attempts_total" "$OUTPUT_DIR/metrics_before.txt" 2>/dev/null | awk '{sum += $2} END {print sum+0}')
    after_total=$(grep "quic_connect_attempts_total" "$OUTPUT_DIR/metrics_after.txt" 2>/dev/null | awk '{sum += $2} END {print sum+0}')
    local delta=$((after_total - before_total))
    echo "  Before: $before_total, After: $after_total, Delta: $delta"
    echo "  Expected (approx): $REQUESTS QUIC connections (1 per RPC)"

    # Long stream sanity
    long_stream_sanity

    # Resource observation
    observe_resources

    echo ""
    echo "============================================"
    echo "  Benchmark Complete"
    echo "============================================"
    echo "  Output directory: $OUTPUT_DIR"
    echo "  Latency CSV:      $(ls "$OUTPUT_DIR"/latency_*.csv 2>/dev/null | head -1)"
    echo "  Metrics before:   $OUTPUT_DIR/metrics_before.txt"
    echo "  Metrics after:    $OUTPUT_DIR/metrics_after.txt"
    echo "============================================"
}

main "$@"
