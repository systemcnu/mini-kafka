# mini-kafka

[![ci](https://github.com/systemcnu/mini-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/systemcnu/mini-kafka/actions/workflows/ci.yml)

A single-broker, Kafka-style durable log in pure Go standard library: fsync-backed
acks, crash-safe recovery, and consumer groups with live rebalancing — one process, real TCP.

```sh
git clone https://github.com/systemcnu/mini-kafka && cd mini-kafka && go run ./cmd/demo
```

What you'll see — the whole demo runs in about 3 minutes:

- **Act one** — a broker comes up in-process, a producer streams `msg-<n>` across
  4 partitions at ~20 msg/s, and two group consumers split them 2+2. First messages
  flow within 60 seconds of the command starting even from cold Go caches (the
  budget is `go run`'s compile, not the broker).
- **Act two** — consumer-2 is killed mid-flight (connections dropped, no goodbye);
  consumer-1 takes over all 4 partitions and the stream continues from the last
  committed offsets.
- Both timings are enforced on every push by CI's `demo-timing` job, which measures
  the demo from outside with a shell clock: first flow ≤ 60 s, total ≤ 180 s.

## Run it by hand

```sh
go build -o bin/minikafka ./cmd/minikafka
go build -o bin/mk ./cmd/mk

./bin/minikafka &                              # listens on 127.0.0.1:7621, data in ./data
./bin/mk create-topic -t demo -p 4
./bin/mk produce -t demo -p 0 -m "hello"       # prints the assigned offset
./bin/mk consume -t demo -p 0 -o 0             # prints offset<TAB>payload, exits at tail
./bin/mk consume -t demo -g readers            # group mode: joins, polls, commits per batch
```

`mk consume -f` long-polls for new records instead of exiting at the tail.
`mk consume -g` joins a consumer group: the broker assigns partitions, rebalances
as members come and go, and resumes each member from its group's committed offsets.
`minikafka` stops gracefully on SIGINT/SIGTERM.

## What it guarantees today

- A produce is acked only after the record is written, fsynced, and the
  durable frontier is atomically advanced. Consumers are only ever served
  fsync-covered records.
- Offsets are contiguous ordinals per partition and survive restart.
- Crash recovery is proven, not just written: scripted-fault suites (torn
  records, bad CRCs, short writes, ENOSPC) and a repeated `kill -9` harness
  run in CI; acked data damage refuses the partition loudly, unacked tails
  are truncated.
- Group commits are persisted atomically before the ack; a rejoining member
  resumes from exactly the committed offsets. Dead members (missed
  heartbeats or dropped control connections) are fenced by generation —
  a stale member's commit can never clobber the group.
- macOS caveat: `fsync` on macOS may not flush the drive cache
  (`F_FULLFSYNC` exists but is much slower and has no Linux equivalent);
  durability claims are qualified by that platform limit.

## Limitations (plainly)

- Local only: the broker binds 127.0.0.1 by default and there is no auth,
  TLS, or replication. One broker, one machine.
- In-memory offset index grows with message count — fine at demo scale.
- No retention or compaction: partitions are single append-only files.

## Development

`scripts/checks.sh` runs the full local battery: build, vet, gofmt check,
stdlib audit, tests, and a `GOOS=linux` cross-compile.
`scripts/demo_timing.sh` is the external demo clock CI runs (gates above);
`scripts/demo_timing_test.sh` proves its gates against a fake demo.

An interactive code map — the runtime story, guided journeys (produce,
long-poll fetch, boot recovery, graceful stop), and editor deep-links —
lives at [docs/code-map.html](docs/code-map.html) (open it in a browser).
Refresh it with `python3 scripts/gen_code_map.py`; the generator re-resolves
every source anchor and refuses to write if any is stale.
