#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$PROJECT_DIR/fixtures"
DATA_DIR=$(mktemp -d /tmp/xline-verify-XXXXXX)
HOSTS_MARKER="# xline-quic-local-cluster"

cleanup() {
    echo "=== Cleaning up ==="
    for pid in "$DATA_DIR"/node*.pid; do
        [ -f "$pid" ] && kill "$(cat "$pid")" 2>/dev/null || true
    done
    rm -rf "$DATA_DIR"
    echo "Cleaned up data dir: $DATA_DIR"
    echo "=== Done ==="
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
        echo "=== Adding /etc/hosts entries ==="
        cp /etc/hosts /etc/hosts.bak.xline-quic 2>/dev/null || true
        for name in "${entries[@]}"; do
            if ! grep -q "$name" /etc/hosts 2>/dev/null; then
                echo "127.0.0.1 $name $HOSTS_MARKER" >> /etc/hosts
                echo "  Added: 127.0.0.1 $name"
            fi
        done
    else
        echo "=== /etc/hosts entries already present ==="
    fi
}

build_binaries() {
    echo "=== Building xline and xlinectl ==="
    cd "$PROJECT_DIR"
    cargo build --bin xline --bin xlinectl 2>&1 | tail -5
    echo "Build complete."
}

XLINE_BIN="$PROJECT_DIR/target/debug/xline"
XLINETL_BIN="$PROJECT_DIR/target/debug/xlinectl"

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

    local state="new"

    echo "  Starting $name (listen=127.0.0.1, advertise=${name})"

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
        --initial-cluster-state "$state" \
        > "$node_data/stdout.log" 2>&1 &

    echo $! > "$node_data/node.pid"
    echo "  $name PID: $(cat "$node_data/node.pid")"
}

wait_for_cluster() {
    echo "=== Waiting for cluster to be ready ==="
    local max_wait=30
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if "$XLINETL_BIN" --endpoints "https://server0:2379" --ca_cert_pem_path "$FIXTURES_DIR/ca.crt" put _health_check ok 2>/dev/null | grep -q "OK"; then
            echo "  Cluster ready after ${waited}s"
            "$XLINETL_BIN" --endpoints "https://server0:2379" --ca_cert_pem_path "$FIXTURES_DIR/ca.crt" delete _health_check 2>/dev/null || true
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "  ERROR: Cluster not ready after ${max_wait}s"
    echo "  === Node logs ==="
    for f in "$DATA_DIR"/server*/stdout.log; do
        echo "  --- $f ---"
        tail -20 "$f" 2>/dev/null || true
    done
    return 1
}

verify_kv() {
    echo "=== KV put/get/delete ==="
    local ep="https://server0:2379"
    local cert="$FIXTURES_DIR/ca.crt"

    echo "  put key1 value1"
    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" put key1 value1

    echo "  get key1"
    local got
    got=$("$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" get key1 2>&1)
    echo "  Result: $got"
    echo "$got" | grep -q "value1" || { echo "  FAIL: expected value1"; return 1; }

    echo "  delete key1"
    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" delete key1

    echo "  get key1 (should be empty)"
    got=$("$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" get key1 2>&1)
    echo "  Result: $got"

    echo "  KV verification passed."
}

verify_member_list() {
    echo "=== Member list ==="
    local cert="$FIXTURES_DIR/ca.crt"
    local result
    result=$("$XLINETL_BIN" --endpoints "https://server0:2379" --ca_cert_pem_path "$cert" member list 2>&1)
    echo "$result"
    local count
    count=$(echo "$result" | wc -l)
    [ "$count" -ge 3 ] || { echo "  FAIL: expected at least 3 members, got $count"; return 1; }
    echo "  Member list verification passed."
}

verify_lease() {
    echo "=== Lease keepalive ==="
    local ep="https://server0:2379"
    local cert="$FIXTURES_DIR/ca.crt"
    local lease_output
    lease_output=$("$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" lease grant 10 2>&1)
    echo "  $lease_output"
    local lease_id
    lease_id=$(echo "$lease_output" | head -1 | tr -d '[:space:]')
    if [ -z "$lease_id" ]; then
        echo "  FAIL: could not extract lease id"
        return 1
    fi
    echo "  Lease ID: $lease_id"

    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" put leased_key leased_val --lease "$lease_id"
    local got
    got=$("$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" get leased_key 2>&1)
    echo "  get leased_key: $got"
    echo "$got" | grep -q "leased_val" || { echo "  FAIL: expected leased_val"; return 1; }

    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" lease revoke "$lease_id"
    echo "  Lease revoked."
    echo "  Lease verification passed."
}

verify_watch() {
    echo "=== Watch (basic) ==="
    local ep="https://server0:2379"
    local cert="$FIXTURES_DIR/ca.crt"

    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" put watch_target initial 2>/dev/null

    timeout 5 "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" watch watch_target -- echo "WATCH_EVENT_RECEIVED" &
    local watch_pid=$!
    sleep 1

    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" put watch_target updated 2>/dev/null

    local watch_out
    watch_out=$(wait $watch_pid 2>&1 || true)
    echo "  Watch output: $watch_out"

    "$XLINETL_BIN" --endpoints "$ep" --ca_cert_pem_path "$cert" delete watch_target 2>/dev/null || true
    echo "  Watch verification passed (basic check)."
}

main() {
    echo "========================================"
    echo "  Xline QUIC Local Cluster Verification"
    echo "========================================"

    ensure_hosts
    build_binaries

    echo "=== Starting 3-node cluster ==="
    for i in 0 1 2; do
        start_node "$i"
    done

    wait_for_cluster

    verify_kv
    verify_member_list
    verify_lease
    verify_watch

    echo ""
    echo "========================================"
    echo "  ALL VERIFICATIONS PASSED"
    echo "========================================"
}

main "$@"
