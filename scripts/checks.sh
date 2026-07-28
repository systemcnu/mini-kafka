#!/usr/bin/env bash
# Local CI-equivalent battery (D-SL0-9): every CI command also runs here,
# plus a GOOS=linux cross-compile as the local Linux proxy.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== build"
go build ./...

echo "== vet"
go vet ./...

echo "== gofmt check"
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
	echo "gofmt needed on:"
	echo "$unformatted"
	exit 1
fi

echo "== stdlib audit"
bash scripts/stdlib_audit.sh

echo "== test"
go test ./... -count=1

echo "== linux cross-build (local Linux proxy)"
GOOS=linux go build ./...

echo "ALL CHECKS GREEN"
