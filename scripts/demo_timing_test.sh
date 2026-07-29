#!/usr/bin/env bash
# Fake-demo gate tests for scripts/demo_timing.sh (SL3 §4). Four cases:
# in-window pass with measured first-flow in [2,4) — an EOF-stamping
# harness measures ~4 here and goes red (F9) — late-first-flow red,
# late-total red, and hang red via the guard's distinct exit. The red
# cases ARE the timing script's red-before-green receipts.
set -euo pipefail
cd "$(dirname "$0")/.."

work=$(mktemp -d "${TMPDIR:-/tmp}/demo-timing-test.XXXXXX")
trap 'rm -rf "$work"' EXIT

# The fake demo emits every pinned marker in the pinned order: first-flow
# at ~2 s, done at ~4 s.
cat >"$work/fake_ok" <<'EOF'
#!/usr/bin/env bash
echo "— act one —"
echo "consumer-1 owns partitions 0,1,2,3"
echo "consumer-2 owns partitions 2,3"
echo "consumer-1 owns partitions 0,1"
sleep 2
echo "#event first-flow"
echo "— act two —"
echo "consumer-1 owns partitions 0,1,2,3"
sleep 2
echo "#event done"
EOF
cat >"$work/fake_hang" <<'EOF'
#!/usr/bin/env bash
echo "— act one —"
sleep 600
EOF
chmod +x "$work/fake_ok" "$work/fake_hang"

fail() {
    echo "FAIL: $1"
    exit 1
}

echo "-- case 1: in-window pass (first-flow ~2 s, done ~4 s)"
out=$(DEMO_CMD="$work/fake_ok" bash scripts/demo_timing.sh) ||
    fail "script exited non-zero on an in-window run"
ff=$(echo "$out" | sed -n 's/^first-flow: *\([0-9][0-9]*\)s.*/\1/p')
[ -n "$ff" ] || fail "no first-flow line in the receipt"
{ [ "$ff" -ge 2 ] && [ "$ff" -lt 4 ]; } ||
    fail "measured first-flow ${ff}s outside [2,4) — the harness is not stamping in the direct pipe reader (F9)"
echo "$out" | grep -q '^result: *PASS$' || fail "no PASS result line"
echo "   ok: exit 0, first-flow=${ff}s in [2,4)"

echo "-- case 2: late first-flow goes red"
set +e
out=$(DEMO_CMD="$work/fake_ok" DEMO_GATE_FIRST_FLOW=1 bash scripts/demo_timing.sh 2>&1)
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "expected exit 1 on a first-flow breach, got $rc"
echo "$out" | grep -q 'first-flow .*exceeds gate' || fail "no first-flow breach message"
echo "   ok: exit 1, breach named"

echo "-- case 3: late total goes red"
set +e
out=$(DEMO_CMD="$work/fake_ok" DEMO_GATE_TOTAL=3 bash scripts/demo_timing.sh 2>&1)
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "expected exit 1 on a total breach, got $rc"
echo "$out" | grep -q 'total .*exceeds gate' || fail "no total breach message"
echo "$out" | grep -q 'first-flow .*exceeds gate' && fail "first-flow wrongly breached too"
echo "   ok: exit 1, breach named"

echo "-- case 4: hang goes red fast with the distinct exit"
if command -v timeout >/dev/null 2>&1 || command -v gtimeout >/dev/null 2>&1 || command -v perl >/dev/null 2>&1; then
    set +e
    out=$(DEMO_CMD="$work/fake_hang" DEMO_TIMEOUT_SECS=3 bash scripts/demo_timing.sh 2>&1)
    rc=$?
    set -e
    [ "$rc" -eq 3 ] || fail "expected the distinct hang exit 3, got $rc"
    echo "$out" | grep -q 'TIMED OUT' || fail "no timed-out receipt line"
    echo "   ok: exit 3, timed-out line present"
else
    echo "   SKIP: no timeout/gtimeout/perl on this host — hang guard not armed (receipted degradation)"
fi

echo "ALL demo_timing.sh GATE CASES GREEN"
