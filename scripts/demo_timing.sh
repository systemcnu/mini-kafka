#!/usr/bin/env bash
# External demo clock (DD-21, D-SL3-4): measures `go run ./cmd/demo -ci`
# from OUTSIDE the process. Sequence: GOTOOLCHAIN=local preflight BEFORE
# t0 (F6) -> fresh temp GOCACHE/GOMODCACHE (never the caller's real
# caches) -> t0 -> run under a hang guard (~200 s), timestamping each line
# in the DIRECT pipe reader (`while read` + `date +%s`, no filter stages
# in between — F9) -> gate first-flow <= 60 s and total <= 180 s -> assert
# the transcript carries the same ordered markers the smoke test pins
# (F5/CF1: the gated surface IS the tested surface) -> emit the receipt.
# Shell here is CI/local harness, never the visitor path (D8 carve-out).
#
# Hang guard (pinned degradation chain, receipted): GNU `timeout -k`,
# else gtimeout, else a perl alarm+fork fallback (macOS ships no timeout),
# else unguarded — whichever applies is named in the receipt.
#
# Exit codes: 0 pass · 1 gate breach · 2 toolchain preflight failure ·
# 3 hang (guard fired) · 4 demo exited non-zero · 5 missing #event marker ·
# 6 marker order broken.
#
# Test seams (fake-demo suite, scripts/demo_timing_test.sh — harness only):
#   DEMO_CMD              executable to run instead of `go run ./cmd/demo -ci`
#   DEMO_GATE_FIRST_FLOW  first-flow gate in seconds (default 60)
#   DEMO_GATE_TOTAL       total gate in seconds (default 180)
#   DEMO_TIMEOUT_SECS     hang-guard budget in seconds (default 200)
#   DEMO_TIMING_IMAGE_REF workflow-pinned container ref, echoed verbatim
#                         into the receipt (D-SL3-5)
set -euo pipefail
cd "$(dirname "$0")/.."

gate_first=${DEMO_GATE_FIRST_FLOW:-60}
gate_total=${DEMO_GATE_TOTAL:-180}
hang_secs=${DEMO_TIMEOUT_SECS:-200}

work=$(mktemp -d "${TMPDIR:-/tmp}/demo-timing.XXXXXX")
cleanup() { chmod -R u+w "$work" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

# GOTOOLCHAIN=local + preflight BEFORE t0 (F6): one go.mod patch-bump must
# fail loudly here, not silently absorb a toolchain download into the gate.
export GOTOOLCHAIN=local
export GOCACHE="$work/gocache" GOMODCACHE="$work/gomodcache"
mkdir -p "$GOCACHE" "$GOMODCACHE"
if ! preflight=$(go list -m 2>&1); then
    echo "preflight FAILED: the local Go toolchain cannot satisfy go.mod under GOTOOLCHAIN=local:" >&2
    echo "$preflight" >&2
    exit 2
fi

# Hang guard (F7): a hung demo must fail fast with a distinct exit, not at
# CI's 6-hour default.
if command -v timeout >/dev/null 2>&1; then
    hang_guard=(timeout -k 10 "$hang_secs")
    hang_note="timeout -k 10 ${hang_secs} (GNU coreutils)"
elif command -v gtimeout >/dev/null 2>&1; then
    hang_guard=(gtimeout -k 10 "$hang_secs")
    hang_note="gtimeout -k 10 ${hang_secs} (coreutils)"
elif command -v perl >/dev/null 2>&1; then
    # The child leads its own process group so the guard can kill the
    # whole tree (a lone TERM to `go run`/bash leaves grandchildren
    # holding the pipe open and the reader parked forever).
    hang_guard=(perl -e '
        my $secs = shift @ARGV;
        my $pid = fork() // die "fork: $!";
        if ($pid == 0) { setpgrp(0, 0); exec @ARGV or exit 127 }
        $SIG{ALRM} = sub { kill "TERM", -$pid; sleep 5; kill "KILL", -$pid; waitpid($pid, 0); exit 124 };
        alarm $secs;
        waitpid($pid, 0);
        my $st = $?;
        exit(($st >> 8) || (($st & 127) ? 128 + ($st & 127) : 0));
    ' "$hang_secs")
    hang_note="perl alarm+fork fallback, ${hang_secs}s (no timeout/gtimeout on this host)"
else
    hang_guard=()
    hang_note="UNAVAILABLE — hang guard not armed (receipted degradation)"
fi

demo_cmd=(go run ./cmd/demo -ci)
if [ -n "${DEMO_CMD:-}" ]; then
    demo_cmd=("$DEMO_CMD")
fi

transcript="$work/transcript"
: > "$transcript"

t0=$(date +%s)
# The `while read` loop IS the timestamper: it reads the demo's stdout
# directly (an interposed awk/sed would block-buffer and inflate every
# measurement, F9) and stamps each line with 1 s resolution.
set +e
${hang_guard[@]+"${hang_guard[@]}"} "${demo_cmd[@]}" | while IFS= read -r line || [ -n "$line" ]; do
    now=$(date +%s)
    printf '%s\n' "$line" >>"$transcript"
    printf '[%4ds] %s\n' "$((now - t0))" "$line"
    case $line in
    '#event first-flow') echo "$now" >"$work/t_first" ;;
    '#event done') echo "$now" >"$work/t_done" ;;
    esac
done
status=${PIPESTATUS[0]}
set -e

if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
    echo "RESULT: TIMED OUT — demo hung past ${hang_secs}s (${hang_note})"
    exit 3
fi
if [ "$status" -ne 0 ]; then
    echo "RESULT: FAIL — demo exited non-zero ($status)"
    exit 4
fi
if [ ! -f "$work/t_first" ]; then
    echo "RESULT: FAIL — '#event first-flow' never appeared"
    exit 5
fi
if [ ! -f "$work/t_done" ]; then
    echo "RESULT: FAIL — '#event done' never appeared"
    exit 5
fi
first_flow=$(($(cat "$work/t_first") - t0))
total=$(($(cat "$work/t_done") - t0))

# Marker-order assertion (F5/CF1): the gated transcript must carry the
# SAME ordered markers the smoke test pins — both act headers, both
# ownership lines before first-flow, the takeover line anchored AFTER the
# act-two header (the act-one owns-all-4 transient must not satisfy it),
# and both events.
# `|| true`: under pipefail a missing marker makes grep's 1 the pipeline
# status, and set -e would kill the script silently instead of reaching
# the named order_fail exit.
first_match() { grep -n -F -x -- "$1" "$transcript" | head -1 | cut -d: -f1 || true; }
first_prefix_match() { grep -n -- "$1" "$transcript" | head -1 | cut -d: -f1 || true; }
i_act1=$(first_match '— act one —')
i_own1=$(first_prefix_match '^consumer-1 owns partitions ')
i_own2=$(first_prefix_match '^consumer-2 owns partitions ')
i_ff=$(first_match '#event first-flow')
i_act2=$(first_match '— act two —')
i_done=$(first_match '#event done')
i_tko=""
if [ -n "$i_act2" ]; then
    i_tko=$(grep -n -F -x -- 'consumer-1 owns partitions 0,1,2,3' "$transcript" |
        awk -F: -v min="$i_act2" '$1 > min { print $1; exit }' || true)
fi
order_fail() {
    echo "RESULT: FAIL — transcript markers missing or out of order: $1"
    exit 6
}
[ -n "$i_act1" ] || order_fail "no act one header"
[ -n "$i_ff" ] || order_fail "no first-flow marker line"
[ -n "$i_own1" ] && [ "$i_own1" -gt "$i_act1" ] && [ "$i_own1" -lt "$i_ff" ] || order_fail "consumer-1 ownership not between act one and first-flow"
[ -n "$i_own2" ] && [ "$i_own2" -gt "$i_act1" ] && [ "$i_own2" -lt "$i_ff" ] || order_fail "consumer-2 ownership not between act one and first-flow"
[ -n "$i_act2" ] && [ "$i_act2" -gt "$i_ff" ] || order_fail "act two header missing or before first-flow"
[ -n "$i_tko" ] || order_fail "no takeover line (consumer-1 owns all 4) after the act-two header"
[ -n "$i_done" ] && [ "$i_done" -gt "$i_tko" ] || order_fail "done marker missing or before the takeover"

breach=""
[ "$first_flow" -le "$gate_first" ] || breach="first-flow ${first_flow}s exceeds gate ${gate_first}s"
[ "$total" -le "$gate_total" ] || breach="${breach:+$breach; }total ${total}s exceeds gate ${gate_total}s"

os_id=$(uname -srm)
if [ -r /etc/os-release ]; then
    os_id="$os_id · $(. /etc/os-release && echo "$PRETTY_NAME")"
fi
echo
echo "== demo-timing receipt (DD-21 / D-SL3-4) =="
echo "commit:      $(git rev-parse HEAD 2>/dev/null || echo "${GITHUB_SHA:-unknown}")"
echo "date-utc:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "go:          $(go version)"
echo "os:          $os_id"
echo "image:       ${DEMO_TIMING_IMAGE_REF:-(none — not the pinned-container run)}"
echo "hang-guard:  $hang_note"
echo "first-flow:  ${first_flow}s (gate ${gate_first}s)"
echo "total:       ${total}s (gate ${gate_total}s)"
echo "markers:     pinned order verified (act one · ownership x2 · first-flow · act two · takeover · done)"
if [ -n "$breach" ]; then
    echo "result:      FAIL — $breach"
    exit 1
fi
echo "result:      PASS"
