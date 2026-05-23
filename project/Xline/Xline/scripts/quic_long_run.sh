#!/bin/bash
set -euo pipefail

DURATION="${1:-120}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$PROJECT_DIR/fixtures"
DATA_DIR=$(mktemp -d /tmp/xline-longrun-XXXXXX)
HOSTS_MARKER="# xline-quic-local-cluster"

XLINETL="$PROJECT_DIR/target/debug/xlinectl"
XLINE="$PROJECT_DIR/target/debug/xline"
EP="https://server0:2379"
CERT="$FIXTURES_DIR/ca.crt"

STOPPED=false
cleanup() {
    STOPPED=true
    echo "=== Cleaning up ==="
    for pid in "$DATA_DIR"/node*.pid; do
        [ -f "$pid" ] && kill "$(cat "$pid")" 2>/dev/null || true
    done
    sleep 1
    for pid in "$DATA_DIR"/node*.pid; do
        [ -f "$pid" ] && kill -9 "$(cat "$pid")" 2>/dev/null || true
    done
    rm -rf "$DATA_DIR"
    echo "Cleaned up data dir: $DATA_DIR"
}
trap cleanup EXIT INT TERM

ensure_hosts() {
    local entries=("server0" "server1" "server2")
    for name in "${entries[@]}"; do
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
        if [ $j -eq 0 ]; then
            peers_arg="${pn}=https://${pn}:${pp}"
        else
            peers_arg="${peers_arg},${pn}=https://${pn}:${pp}"
        fi
    done

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
        --initial-cluster-state new \
        > "$node_data/stdout.log" 2>&1 &
    echo $! > "$node_data/node.pid"
}

wait_for_cluster() {
    local max_wait=30
    for ((i=0; i<max_wait; i++)); do
        if "$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" put _hc ok 2>/dev/null | grep -q "OK"; then
            "$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" delete _hc 2>/dev/null || true
            return 0
        fi
        sleep 1
    done
    echo "ERROR: cluster not ready"
    return 1
}

kv_worker() {
    local id=$1
    local count=0
    local errors=0
    while [ "$STOPPED" = false ]; do
        local key="lr_kv_${id}_${count}"
        if ! "$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" put "$key" "val_${count}" >/dev/null 2>&1; then
            errors=$((errors + 1))
            continue
        fi
        local got
        got=$("$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" get "$key" 2>&1)
        if ! echo "$got" | grep -q "val_${count}"; then
            errors=$((errors + 1))
        fi
        "$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" delete "$key" >/dev/null 2>&1 || true
        count=$((count + 1))
        sleep 0.1
    done
    echo "  kv_worker_$id: $count ops, $errors errors"
}

lease_worker() {
    local id=$1
    local count=0
    local errors=0
    while [ "$STOPPED" = false ]; do
        local lease_output
        lease_output=$("$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" lease grant 5 2>&1)
        local lease_id
        lease_id=$(echo "$lease_output" | head -1 | tr -d '[:space:]')
        if [ -z "$lease_id" ]; then
            errors=$((errors + 1))
            sleep 1
            continue
        fi
        "$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" put "lr_lease_${id}_${count}" "v" --lease "$lease_id" >/dev/null 2>&1 || errors=$((errors + 1))
        "$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" lease revoke "$lease_id" >/dev/null 2>&1 || true
        count=$((count + 1))
        sleep 0.2
    done
    echo "  lease_worker_$id: $count ops, $errors errors"
}

member_worker() {
    local count=0
    local errors=0
    while [ "$STOPPED" = false ]; do
        local result
        result=$("$XLINETL" --endpoints "$EP" --ca_cert_pem_path "$CERT" member list 2>&1)
        local lines
        lines=$(echo "$result" | wc -l)
        if [ "$lines" -lt 3 ]; then
            errors=$((errors + 1))
        fi
        count=$((count + 1))
        sleep 2
    done
    echo "  member_worker: $count ops, $errors errors"
}

echo "========================================"
echo "  Xline QUIC Long-Run Verification"
echo "  Duration: ${DURATION}s"
echo "========================================"

ensure_hosts

echo "=== Starting 3-node cluster ==="
for i in 0 1 2; do start_node "$i"; done
wait_for_cluster

echo "=== Starting ${DURATION}s continuous operations ==="
echo "  Workers: 5 KV + 3 lease + 1 member"

PIDS=()
kv_worker 0 & PIDS+=($!)
kv_worker 1 & PIDS+=($!)
kv_worker 2 & PIDS+=($!)
kv_worker 3 & PIDS+=($!)
kv_worker 4 & PIDS+=($!)
lease_worker 0 & PIDS+=($!)
lease_worker 1 & PIDS+=($!)
lease_worker 2 & PIDS+=($!)
member_worker & PIDS+=($!)

sleep "$DURATION"
STOPPED=true

for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

echo ""
echo "========================================"
echo "  LONG-RUN COMPLETE (${DURATION}s)"
echo "========================================"

PANICS=0
for f in "$DATA_DIR"/server*/stdout.log; do
    if grep -qi "panic" "$f" 2>/dev/null; then
        echo "  WARNING: panic found in $f"
        PANICS=$((PANICS + 1))
    fi
done

if [ "$PANICS" -gt 0 ]; then
    echo "  RESULT: FAIL ($PANICS panics detected)"
    exit 1
fi

echo "  RESULT: PASS (no panics)"
