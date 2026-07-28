#!/usr/bin/env bash
# NFR-1 gate: every dependency of the runtime packages must be Go stdlib.
# go list -deps prints the full transitive closure; anything non-Standard
# that is not this module itself fails the audit (golang.org/x/* counts as
# external).
set -euo pipefail
cd "$(dirname "$0")/.."

nonstd=$(go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... |
	sed '/^$/d' | grep -v -E '^mini-kafka(/|$)' || true)

if [ -n "$nonstd" ]; then
	echo "non-stdlib dependencies found:"
	echo "$nonstd"
	exit 1
fi
echo "stdlib audit OK"
