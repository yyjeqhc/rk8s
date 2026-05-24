#!/bin/bash
# QUIC Restart Stress Test
# Repeatedly: start 3-node cluster → run ops → stop → verify clean → repeat
# Tests for state leakage, port conflicts, QUIC session corruption between runs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$PROJECT_DIR/fixtures"
HOSTS_MARKER="# xline-quic-local-cluster"

XLINETL="$PROJECT_DIR/target/debug/xlinectl"
XLINE="$PROJECT_DIR/target/debug/xline"
CERT="$FIXTURES_DIR/ca.crt"

ROUNDS=${1:-20}
PASS=true
FAIL_COUNT=0
WARN_COUNT=0

# Accumulated warning category counts across all rounds
TOTAL_CONN_DROPPED=0
TOTAL_STREAM_NOT_CLOSED=0
TOTAL_STREAM_NOT_STOPPED=0
TOTAL_NO_VIABLE=0

log_pass() { echo "  ✅ $1"; }
log_fail() { echo "  ❌ $1"; PASS=false; FAIL_COUNT=$((FAIL_COUNT + 1)); }
log_warn() { echo "  ⚠️  $1"; WARN_COUNT=$((WARN_COUNT + 1)); }
log_info() { echo "  ℹ️  $1"; }

cleanup() {
    pkill -9 -f "xline --name server" 2>/dev/null || true
    rm -rf /tmp/xline-stress-* 2>/dev/null || true
}
trap cleanup EXIT

ensure_hosts() {
    for name in server0 server1 server2; do
        if ! grep -q "$name" /etc/hosts 2>/dev/null; then
            echo "127.0.0.1 $name $HOSTS_MARKER" >> /etc/hosts
        fi
    done
}

start_node() {
    local idx=$1
    local data_dir=$2
    local name="server${idx}"
    local client_port=$((2379 + idx * 2))
    local peer_port=$((2380 + idx * 2))
    local node_data="$data_dir/$name"
    mkdir -p "$node_data"

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

    "$XLINE" \
        --name "$name" \
        --members "$peers_arg" \
        --client-listen-urls "https://127.0.0.1:${client_port}" \
        --peer-listen-urls "https://127.0.0.1:${peer_port}" \
        --client-advertise-urls "https://${name}:${client_port}" \
        --peer-advertise-urls "https://${name}:${peer_port}" \
        --storage-engine rocksdb \
        --data-dir "$node_data/data" \
        --peer-cert-path "$FIXTURES_DIR/${name}.crt" \
        --peer-key-path "$FIXTURES_DIR/${name}.key" \
        --peer-ca-cert-path "$FIXTURES_DIR/ca.crt" \
        --client-ca-cert-path "$FIXTURES_DIR/ca.crt" \
        --initial-cluster-state new \
        > "$node_data/stdout.log" 2>&1 &
    echo $! > "$node_data/node.pid"
}

stop_all() {
    local data_dir=$1
    # Graceful SIGTERM
    for pidfile in "$data_dir"/server*/node.pid; do
        [ -f "$pidfile" ] && kill "$(cat "$pidfile")" 2>/dev/null || true
    done
    sleep 2
    # Force kill any stragglers
    for pidfile in "$data_dir"/server*/node.pid; do
        if [ -f "$pidfile" ]; then
            local pid
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
    done
    sleep 1
}

wait_for_cluster() {
    local max_wait=30
    for ((i=0; i<max_wait; i++)); do
        if "$XLINETL" --endpoints "https://server0:2379" --ca_cert_pem_path "$CERT" put _hc ok 2>/dev/null | grep -q "OK"; then
            "$XLINETL" --endpoints "https://server0:2379" --ca_cert_pem_path "$CERT" delete _hc 2>/dev/null || true
            return 0
        fi
        sleep 1
    done
    return 1
}

check_no_port_conflicts() {
    # After stopping, verify no xline processes are still listening on our ports
    local conflict=false
    for port in 2379 2380 2381 2382 2383 2384; do
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
            log_warn "Port $port still in use after shutdown"
            conflict=true
        fi
    done
    if [ "$conflict" = true ]; then
        # Force kill any remaining xline processes
        pkill -9 -f "xline --name server" 2>/dev/null || true
        sleep 2
    fi
}

run_round() {
    local round=$1
    local data_dir
    data_dir=$(mktemp -d "/tmp/xline-stress-${round}-XXXXXX")

    echo ""
    echo "=== Round $round/$ROUNDS ==="

    # Start cluster
    start_node 0 "$data_dir"
    start_node 1 "$data_dir"
    start_node 2 "$data_dir"

    if ! wait_for_cluster; then
        log_fail "Round $round: Cluster failed to start"
        stop_all "$data_dir"
        rm -rf "$data_dir"
        return 1
    fi

    # KV ops
    local ep="https://server0:2379"
    if ! "$XLINETL" --endpoints "$ep" --ca_cert_pem_path "$CERT" put "r${round}_key" "value_${round}" 2>/dev/null | grep -q "OK"; then
        log_fail "Round $round: KV put failed"
        stop_all "$data_dir"
        rm -rf "$data_dir"
        return 1
    fi

    local got
    got=$("$XLINETL" --endpoints "$ep" --ca_cert_pem_path "$CERT" get "r${round}_key" 2>&1)
    if ! echo "$got" | grep -q "value_${round}"; then
        log_fail "Round $round: KV get failed (got: $got)"
        stop_all "$data_dir"
        rm -rf "$data_dir"
        return 1
    fi

    # Member list
    local members
    members=$("$XLINETL" --endpoints "$ep" --ca_cert_pem_path "$CERT" member list 2>&1)
    local mcount
    mcount=$(echo "$members" | wc -l)
    if [ "$mcount" -lt 3 ]; then
        log_fail "Round $round: Member list shows $mcount members"
        stop_all "$data_dir"
        rm -rf "$data_dir"
        return 1
    fi

    log_info "Round $round: KV + member list OK"

    # Stop
    stop_all "$data_dir"
    check_no_port_conflicts

    # Verify clean shutdown
    local panics=0
    for f in "$data_dir"/server*/stdout.log; do
        local p
        p=$(grep -c "panic" "$f" 2>/dev/null || true)
        panics=$((panics + p))
    done
    if [ "$panics" -gt 0 ]; then
        log_fail "Round $round: Found $panics panic(s) in logs"
    fi

    # Check for "No viable network path" warnings
    local no_viable=0
    for f in "$data_dir"/server*/stdout.log; do
        local n
        n=$(grep -c "No viable network path" "$f" 2>/dev/null || true)
        no_viable=$((no_viable + n))
    done
    if [ "$no_viable" -gt 0 ]; then
        log_warn "Round $round: $no_viable 'No viable network path' warnings"
    fi

    local conn_dropped=0
    local stream_not_closed=0
    local stream_not_stopped=0
    for f in "$data_dir"/server*/stdout.log; do
        conn_dropped=$((conn_dropped + $(grep -c "connection is still active when dropped" "$f" 2>/dev/null || true)))
        stream_not_closed=$((stream_not_closed + $(grep -c "stream not closed before dropped" "$f" 2>/dev/null || true)))
        stream_not_stopped=$((stream_not_stopped + $(grep -c "not stopped with error before dropped" "$f" 2>/dev/null || true)))
    done
    local stream_warn=$((conn_dropped + stream_not_closed + stream_not_stopped))
    TOTAL_CONN_DROPPED=$((TOTAL_CONN_DROPPED + conn_dropped))
    TOTAL_STREAM_NOT_CLOSED=$((TOTAL_STREAM_NOT_CLOSED + stream_not_closed))
    TOTAL_STREAM_NOT_STOPPED=$((TOTAL_STREAM_NOT_STOPPED + stream_not_stopped))
    TOTAL_NO_VIABLE=$((TOTAL_NO_VIABLE + no_viable))
    if [ "$stream_warn" -gt 0 ]; then
        log_warn "Round $round: $stream_warn stream warnings (conn=$conn_dropped, not_closed=$stream_not_closed, not_stopped=$stream_not_stopped)"
    fi

    log_pass "Round $round: Complete (panics=$panics, no_viable=$no_viable, stream_warn=$stream_warn)"

    rm -rf "$data_dir"
}

# ─── Main ───

echo "========================================"
echo "  Xline QUIC Restart Stress Test"
echo "  Rounds: $ROUNDS"
echo "========================================"

ensure_hosts

# Verify binaries exist
if [ ! -x "$XLINETL" ] || [ ! -x "$XLINE" ]; then
    echo "ERROR: Binaries not found. Run 'cargo build --bin xline --bin xlinectl' first."
    exit 1
fi

# Kill any leftover xline processes
pkill -9 -f "xline --name server" 2>/dev/null || true
sleep 1

for ((round=1; round<=ROUNDS; round++)); do
    run_round "$round"
done

echo ""
echo "========================================"
echo "  Stress Test Summary"
echo "  Rounds: $ROUNDS"
echo "  Failures: $FAIL_COUNT"
echo "  Warnings: $WARN_COUNT"
echo "  --- Warning Categories ---"
echo "  connection dropped:       $TOTAL_CONN_DROPPED"
echo "  stream not closed:        $TOTAL_STREAM_NOT_CLOSED"
echo "  stream not stopped:       $TOTAL_STREAM_NOT_STOPPED"
echo "  no viable network path:   $TOTAL_NO_VIABLE"
if [ "$PASS" = true ]; then
    echo "  RESULT: ALL ROUNDS PASSED"
else
    echo "  RESULT: $FAIL_COUNT ROUND(S) FAILED"
fi
echo "========================================"

[ "$PASS" = true ]
