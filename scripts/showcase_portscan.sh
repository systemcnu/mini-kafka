#!/usr/bin/env bash
# showcase_portscan.sh — SHOW-3's external scan procedure (D-SL7-8, G-SL7-2).
#
# Usage:
#   scripts/showcase_portscan.sh <hostname> [--ports "<space list>"] [--expected <file>]
#
# Probes each port with `nc -z -G 3 -w 3` (-G bounds the CONNECT: a filtered port that
# silently drops packets — Render's edge posture — would otherwise hang past -w),
# NOTE (witnessed at the first live scan, 2026-08-01): Render's SHARED EDGE answers
# TCP on 8080 with a static 403 "Blocked" page for the hostname — an edge listener,
# not this service (port 80 gives the proper 301-to-HTTPS; the container exposes only
# $PORT). The expected file records the edge's real posture: 443/80/8080 open.
# in argument order: `port NNN: open` / `port NNN: closed`. The output is
# compared against the expected file; ANY deviation prints the diff and
# exits non-zero.
#
# Per-deploy procedure (run after EVERY deploy and at slice exit, from a
# host OUTSIDE Render):
#   1. scripts/showcase_portscan.sh <service-hostname>   → output into
#      docs/receipts/sl7-portscan.txt (the receipt).
#   2. The two live route-posture probes ride the same receipt:
#        curl -X POST https://<service-hostname>/feed   → expect 405
#        curl https://<service-hostname>/nope           → expect 404
#      (the DEPLOYED binary's method/path posture witnessed, not just the
#      local one's.)
#
# Expected live pattern (scripts/showcase_portscan.expected): 443 open and
# 80 open — port 80 is Render's redirect-to-HTTPS edge, expected — and
# every other listed port closed.
#
# Honesty note (D-SL7-8): the scan witnesses that RENDER'S EDGE exposes
# exactly 443/80 — the closed broker ports would read closed even if the
# in-container bind were wrong, because the edge never routes them; the
# loopback bind's actual proof is D-SL7-1's config-literal unit test, and
# the scan exists so the exposure claim is WITNESSED per deploy, not
# deduced (G-SL7-2).
#
# Self-test (no live service needed): pin two high localhost ports — one
# with a listener, one without — against showcase_portscan.selftest.expected:
#   python3 -m http.server 39471 --bind 127.0.0.1 &
#   scripts/showcase_portscan.sh 127.0.0.1 --ports "39471 39472" \
#     --expected scripts/showcase_portscan.selftest.expected
# proving open-detected-open, closed-detected-closed, and (with the
# listener killed) deviation → non-zero.
set -euo pipefail

DEFAULT_PORTS="443 80 7621 7620 7622 7623 7624 7625 7626 7627 7628 7629 7630 8080 9092 3000 5432 6379"

usage() {
	echo "usage: $0 <hostname> [--ports \"<space list>\"] [--expected <file>]" >&2
	exit 2
}

[ $# -ge 1 ] || usage
host="$1"
shift
ports="$DEFAULT_PORTS"
expected="$(dirname "$0")/showcase_portscan.expected"
while [ $# -gt 0 ]; do
	case "$1" in
	--ports)
		[ $# -ge 2 ] || usage
		ports="$2"
		shift 2
		;;
	--expected)
		[ $# -ge 2 ] || usage
		expected="$2"
		shift 2
		;;
	*) usage ;;
	esac
done

[ -r "$expected" ] || {
	echo "$0: expected file not readable: $expected" >&2
	exit 2
}

out=""
read -r -a port_list <<<"$ports"
for port in "${port_list[@]}"; do
	if nc -z -G 3 -w 3 "$host" "$port" >/dev/null 2>&1; then
		line="port $port: open"
	else
		line="port $port: closed"
	fi
	echo "$line"
	out="${out}${line}"$'\n'
done

if ! diff <(printf '%s' "$out") "$expected"; then
	echo "DEVIATION from $expected — do not trust this deploy's exposure claim until explained" >&2
	exit 1
fi
