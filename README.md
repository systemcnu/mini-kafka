# mini-kafka

A single-broker, Kafka-style durable log in pure Go standard library. This is
the **walking skeleton** (slice SL0): one broker process, append-only
per-partition logs with fsync-backed acks, a small binary protocol over TCP,
and a `mk` CLI to produce and consume.

## Run it

```sh
go build -o bin/minikafka ./cmd/minikafka
go build -o bin/mk ./cmd/mk

./bin/minikafka &                              # listens on 127.0.0.1:7621, data in ./data
./bin/mk create-topic -t demo -p 1
./bin/mk produce -t demo -p 0 -m "hello"       # prints the assigned offset
./bin/mk consume -t demo -p 0 -o 0             # prints offset<TAB>payload, exits at tail
```

`mk consume -f` long-polls for new records instead of exiting at the tail.
`minikafka` stops gracefully on SIGINT/SIGTERM.

## What it guarantees today

- A produce is acked only after the record is written, fsynced, and the
  durable frontier is atomically advanced. Consumers are only ever served
  fsync-covered records.
- Offsets are contiguous ordinals per partition and survive restart.
- macOS caveat: `fsync` on macOS may not flush the drive cache
  (`F_FULLFSYNC` exists but is much slower and has no Linux equivalent);
  durability claims are qualified by that platform limit.

## Limitations (plainly)

- No consumer groups yet — raw single-partition fetch only (groups are the
  next slices).
- Crash-fault recovery code exists (boot scan, refuse-on-acked-damage,
  truncate unacked tail) but is **not yet proven against scripted faults or
  kill -9 harnesses** — that proof work is slice SL1.
- Local only: the broker binds 127.0.0.1 by default and there is no auth,
  TLS, or replication. One broker, one machine.
- In-memory offset index grows with message count — fine at demo scale.

## Development

`scripts/checks.sh` runs the full local battery: build, vet, gofmt check,
stdlib audit, tests, and a `GOOS=linux` cross-compile.
