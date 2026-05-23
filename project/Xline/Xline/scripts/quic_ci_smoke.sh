#!/bin/bash
# QUIC CI Smoke Test — lightweight entry point for CI pipelines.
#
# Default mode (short):
#   cargo build --bin xline --bin xlinectl
#   bash scripts/quic_local_cluster.sh
#   bash scripts/quic_restart_stress.sh 3
#
# Long mode (set XLINE_QUIC_CI_LONG=1):
#   Everything above, plus:
#   bash scripts/quic_long_run.sh 120
#   bash scripts/quic_fault_smoke.sh
#
# Requirements:
#   - /etc/hosts must resolve server0, server1, server2 to 127.0.0.1
#   - Will NOT modify /etc/hosts automatically (fails with clear error)
#
# Usage:
#   bash scripts/quic_ci_smoke.sh
#   XLINE_QUIC_CI_LONG=1 bash scripts/quic_ci_smoke.sh
#
# Debug logging:
#   To enable transport debug logs during CI:
#   RUST_LOG=xline=debug,xlinerpc=debug,curp=debug bash scripts/quic_ci_smoke.sh
#
#   To filter specific components:
#   RUST_LOG=xlinerpc::h3_client=debug bash scripts/quic_ci_smoke.sh
#
#   To collect logs on failure:
#   RUST_LOG=xline=debug,xlinerpc=debug bash scripts/quic_ci_smoke.sh 2>&1 | tee quic-debug.log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors (disabled in non-TTY)
if [ -t 1 ]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; NC=''
fi

pass() { echo -e "${GREEN}✅ PASS: $1${NC}"; }
fail() { echo -e "${RED}❌ FAIL: $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}ℹ️  $1${NC}"; }

FAILURES=0

# ── Pre-flight checks ────────────────────────────────────────────────

check_hosts() {
    local missing=()
    for name in server0 server1 server2; do
        if ! grep -qw "$name" /etc/hosts 2>/dev/null; then
            missing+=("$name")
        fi
    done
    if [ ${#missing[@]} -gt 0 ]; then
        echo -e "${RED}ERROR: /etc/hosts is missing entries for: ${missing[*]}${NC}"
        echo ""
        echo "Add these lines to /etc/hosts:"
        echo ""
        for name in "${missing[@]}"; do
            echo "  127.0.0.1 $name"
        done
        echo ""
        echo "Or run:"
        echo "  sudo bash -c 'for h in ${missing[*]}; do echo \"127.0.0.1 \$h\" >> /etc/hosts; done'"
        echo ""
        exit 1
    fi
}

check_hosts

# ── Cleanup on exit ───────────────────────────────────────────────────

cleanup() {
    info "Cleaning up any leftover xline processes..."
    pkill -f "xline --name server" 2>/dev/null || true
    rm -rf /tmp/xline-verify-* /tmp/xline-stress-* /tmp/xline-long-* 2>/dev/null || true
}
trap cleanup EXIT

# ── Step 1: Build ─────────────────────────────────────────────────────

echo ""
echo "============================================"
echo " Step 1: cargo build --bin xline --bin xlinectl"
echo "============================================"

if cargo build --bin xline --bin xlinectl 2>&1 | tail -5; then
    pass "cargo build"
else
    fail "cargo build"
    echo ""
    echo "Build failed — cannot continue."
    exit 1
fi

# ── Step 2: Smoke test ────────────────────────────────────────────────

echo ""
echo "============================================"
echo " Step 2: quic_local_cluster.sh (smoke test)"
echo "============================================"

if bash "$SCRIPT_DIR/quic_local_cluster.sh" 2>&1 | tail -30; then
    pass "quic_local_cluster.sh"
else
    fail "quic_local_cluster.sh"
    echo ""
    info "Last 30 lines of output shown above."
    info "For full output, run: bash scripts/quic_local_cluster.sh"
fi

# ── Step 3: Restart stress (3 rounds) ─────────────────────────────────

echo ""
echo "============================================"
echo " Step 3: quic_restart_stress.sh 3"
echo "============================================"

if bash "$SCRIPT_DIR/quic_restart_stress.sh" 3 2>&1 | tail -40; then
    pass "quic_restart_stress.sh 3"
else
    fail "quic_restart_stress.sh 3"
    echo ""
    info "Last 40 lines of output shown above."
    info "For full output, run: bash scripts/quic_restart_stress.sh 3"
fi

# ── Step 4 (optional): Long run + fault test ──────────────────────────

if [ "${XLINE_QUIC_CI_LONG:-0}" = "1" ]; then
    echo ""
    echo "============================================"
    echo " Step 4a: quic_long_run.sh 120"
    echo "============================================"

    if bash "$SCRIPT_DIR/quic_long_run.sh" 120 2>&1 | tail -30; then
        pass "quic_long_run.sh 120"
    else
        fail "quic_long_run.sh 120"
        echo ""
        info "Last 30 lines of output shown above."
        info "For full output, run: bash scripts/quic_long_run.sh 120"
    fi

    echo ""
    echo "============================================"
    echo " Step 4b: quic_fault_smoke.sh"
    echo "============================================"

    if bash "$SCRIPT_DIR/quic_fault_smoke.sh" 2>&1 | tail -40; then
        pass "quic_fault_smoke.sh"
    else
        fail "quic_fault_smoke.sh"
        echo ""
        info "Last 40 lines of output shown above."
        info "For full output, run: bash scripts/quic_fault_smoke.sh"
    fi
else
    echo ""
    info "Skipping long-run and fault tests (set XLINE_QUIC_CI_LONG=1 to enable)"
fi

# ── Summary ───────────────────────────────────────────────────────────

echo ""
echo "============================================"
if [ "$FAILURES" -eq 0 ]; then
    echo -e " ${GREEN}ALL PASSED${NC}"
else
    echo -e " ${RED}$FAILURES FAILURE(S)${NC}"
fi
echo "============================================"
echo ""

exit "$FAILURES"
