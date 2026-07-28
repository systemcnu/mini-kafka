#!/usr/bin/env bash
# NFR-1 gate: every dependency of the runtime packages must be Go stdlib.
# go list -deps prints the full transitive closure; anything non-Standard
# that is not this module itself fails the audit (golang.org/x/* counts as
# external).
set -euo pipefail
cd "$(dirname "$0")/.."

module=$(go list -m)
nonstd=$(go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... |
	sed '/^$/d' | grep -v -F -e "$module" || true)

if [ -n "$nonstd" ]; then
	echo "non-stdlib dependencies found:"
	echo "$nonstd"
	exit 1
fi
echo "stdlib audit OK"
