#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$PROJECT_DIR/fixtures"
DATA_DIR=$(mktemp -d /tmp/xline-fault-XXXXXX)
HOSTS_MARKER="# xline-quic-local-cluster"

XLINETL="$PROJECT_DIR/target/debug/xlinectl"
XLINE="$PROJECT_DIR/target/debug/xline"
CERT="$FIXTURES_DIR/ca.crt"

PASS=true
log_pass() { echo "  ✅ $1"; }
log_fail() { echo "  ❌ $1"; PASS=false; }

cleanup() {
    for pid in "$DATA_DIR"/node*.pid; do
        [ -f "$pid" ] && kill "$(cat "$pid")" 2>/dev/null || true
    done
    sleep 1
    for pid in "$DATA_DIR"/node*.pid; do
        [ -f "$pid" ] && kill -9 "$(cat "$pid")" 2>/dev/null || true
    done
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT INT TERM

ensure_hosts() {
    for name in server0 server1 server2; do
        if ! grep -q "$name" /etc/hosts 2>/dev/null; then
            echo "127.0.0.1 $name $HOSTS_MARKER" >> /etc/hosts
        fi
    done
}

start_node() {
    local idx=$1
    local name="server${idx}"
    local client_port=$((2379 + idx * 2))
    local peer_port=$((2380 + idx * 2))
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
        local cp=$((2379 + j * 2))
        if [ $j -eq 0 ]; then
            peers_arg="${pn}=https://${pn}:${pp}"
        else
            peers_arg="${peers_arg},${pn}=https://${pn}:${pp}"
        fi
    done

    local state="${2:-new}"

    "$XLINE" \
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
        --initial-cluster-state "$state" \
        > "$node_data/stdout.log" 2>&1 &
    echo $! > "$node_data/node.pid"
}

client_url() {
    local idx=$1
    local port=$((2379 + idx * 2))
    echo "https://server${idx}:${port}"
}

wait_for_cluster() {
    local max_wait=30
    local ep
    ep=$(client_url 0)
    for ((i=0; i<max_wait; i++)); do
        if "$XLINETL" --endpoints "$ep" --ca_cert_pem_path "$CERT" put _hc ok 2>/dev/null | grep -q "OK"; then
            "$XLINETL" --endpoints "$ep" --ca_cert_pem_path "$CERT" delete _hc 2>/dev/null || true
            return 0
        fi
        sleep 1
    done
    return 1
}

kv_op() {
    local ep="$1"
    "$XLINETL" --endpoints "$ep" --ca_cert_pem_path "$CERT" put fault_test val 2>/dev/null | grep -q "OK"
}

get_pid() {
    local idx=$1
    cat "$DATA_DIR/server${idx}/node.pid" 2>/dev/null
}

kill_node() {
    local idx=$1
    local pid
    pid=$(get_pid "$idx")
    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null || true
        sleep 1
    fi
}

echo "========================================"
echo "  Xline QUIC Fault Smoke Test"
echo "========================================"

ensure_hosts

echo "=== Starting 3-node cluster ==="
for i in 0 1 2; do start_node "$i"; done
wait_for_cluster
echo "  Cluster ready."

echo ""
echo "=== Test 1: Kill follower (server2), KV ops should still succeed ==="
kill_node 2
sleep 2
if kv_op "$(client_url 0)"; then
    log_pass "KV put after follower kill succeeded"
else
    log_fail "KV put after follower kill failed"
fi

echo ""
echo "=== Test 2: Restart follower (server2), member list should show 3 members ==="
start_node 2 "existing"
sleep 5
result=$("$XLINETL" --endpoints "$(client_url 0)" --ca_cert_pem_path "$CERT" member list 2>&1)
count=$(echo "$result" | wc -l)
if [ "$count" -ge 3 ]; then
    log_pass "Member list shows $count members after follower restart"
else
    log_fail "Member list shows $count members (expected >= 3)"
fi

echo ""
echo "=== Test 3: Kill leader, wait for re-election ==="
leader_pid=$(get_pid 0)
kill_node 0
echo "  Killed server0 (leader). Waiting for re-election..."
sleep 10

if kv_op "$(client_url 1)"; then
    log_pass "KV put after leader kill succeeded (new leader elected)"
else
    log_fail "KV put after leader kill failed"
fi

echo ""
echo "=== Test 4: Restart old leader (server0), verify full cluster ==="
start_node 0 "existing"
sleep 5
result=$("$XLINETL" --endpoints "$(client_url 1)" --ca_cert_pem_path "$CERT" member list 2>&1)
count=$(echo "$result" | wc -l)
if [ "$count" -ge 3 ]; then
    log_pass "Member list shows $count members after full recovery"
else
    log_fail "Member list shows $count members (expected >= 3)"
fi

echo ""
echo "=== Test 5: Final KV ops on all endpoints ==="
for i in 0 1 2; do
    ep="$(client_url $i)"
    if kv_op "$ep"; then
        log_pass "KV put to server${i} succeeded"
    else
        log_fail "KV put to server${i} failed"
    fi
done

echo ""
echo "=== Checking for excessive errors in logs ==="
for f in "$DATA_DIR"/server*/stdout.log; do
    panics=$(grep -c "panic" "$f" 2>/dev/null || true)
    if [ "$panics" -gt 0 ]; then
        log_fail "Found $panics panic(s) in $(basename "$(dirname "$f")")"
    fi
done

echo ""
echo "========================================"
if [ "$PASS" = true ]; then
    echo "  ALL FAULT TESTS PASSED"
else
    echo "  SOME FAULT TESTS FAILED"
fi
echo "========================================"

[ "$PASS" = true ]
