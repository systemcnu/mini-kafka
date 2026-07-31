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

## Live showcase

A watch-only, self-driving instance of this broker — producer and group consumer
running against the real TCP surface, watched from a browser:
**https://systemcnu.github.io/mini-kafka/showcase/**

The first load can take about a minute while the free instance wakes (free
instances sleep when idle — the page narrates the wait). Visitors cannot write
to it: the hosted surface serves exactly one page and one read-only JSON feed.

Teardown criterion: if Render's free tier gains a card requirement, starts
charging, or the workspace's free instance hours run out — the service is
deleted, and this link reverts to "not currently hosted".

Run it locally instead: `go run ./cmd/showcase`, then open `127.0.0.1:8080`.

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
- Delivery is at-least-once: duplicates are possible after crashes and
  rebalances, loss of acked data is not.
- durability is platform-qualified: on macOS Go's Sync is F_FULLFSYNC (drive-cache barrier — stronger and slower); on Linux plain fsync (DD-7, corrected)

## Limitations (plainly)

- Local only: the broker binds 127.0.0.1 by default and there is no auth,
  TLS, or replication — pointing `--addr` anywhere else exposes an
  unauthenticated protocol to that network. One broker, one machine.
- In-memory offset index grows with message count — fine at demo scale.
- No retention or compaction: partitions are single append-only files.

## Development

The complete wire contract lives in [docs/PROTOCOL.md](docs/PROTOCOL.md) —
implement your own client from it; its two registry tables are
machine-diffed against `internal/wire` on every test run and in CI.

`scripts/checks.sh` runs the full local battery: build, vet, gofmt check,
stdlib audit, tests, and a `GOOS=linux` cross-compile.
`scripts/demo_timing.sh` is the external demo clock CI runs (gates above);
`scripts/demo_timing_test.sh` proves its gates against a fake demo.

An interactive code map — the runtime story, guided journeys (produce,
long-poll fetch, boot recovery, graceful stop), and editor deep-links —
lives at [docs/code-map.html](docs/code-map.html) (open it in a browser).
Refresh it with `python3 scripts/gen_code_map.py`; the generator re-resolves
every source anchor and refuses to write if any is stale.

<!-- bench:begin -->
## Benchmarks — closed-loop response latency

Machine-rendered from the committed report by `go run ./cmd/bench -render-readme <report>`;
a repo test re-renders and byte-compares, so a hand-edited number is a build failure.

| iteration | msgs/s | MB/s | ack p50 ms | ack p99 ms | e2e p50 ms | e2e p99 ms | e2e samples | produce errors | duplicates |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 98.3 | 0.10 | 72.26 | 150.64 | 72.31 | 152.22 | 987 | 0 | 0 |
| 2 | 38.5 | 0.04 | 131.13 | 3661.89 | 132.87 | 3662.77 | 388 | 0 | 0 |
| 3 | 57.3 | 0.06 | 137.22 | 177.41 | 139.44 | 180.63 | 584 | 0 | 0 |

Spread across iterations (min / max / mean):

- msgs/s: 38.5 / 98.3 / 64.7
- ack p99 ms: 150.64 / 3661.89 / 1329.98
- e2e p99 ms: 152.22 / 3662.77 / 1331.87

Setup:

- hardware: Apple M3 Max (Mac15,9), 128 GB RAM, macOS 26.5.2
- OS/arch: darwin/arm64
- Go: go1.24.0
- GOMAXPROCS: 16
- commit: a5796a59d19f
- storage: Apple internal NVMe SSD (APFS)
- fsync mode: fsync via Go os.File.Sync (macOS: F_FULLFSYNC, a full drive-cache barrier; Linux: fsync)
- group-commit window: 5 ms
- load model: closed-loop, C=8 sync producers, in-flight 1/conn
- message size: 1024 bytes
- partitions: 4
- run: 3 iterations × 10.0 s
- warm-up: 2.0 s (measured, discarded)
- percentile method: nearest-rank on sorted samples

Caveats:

- closed-loop load understates the queueing tails an open-loop arrival process would show
- ack latency includes the broker's 5 ms group-commit window
- durability is platform-qualified: on macOS Go's Sync is F_FULLFSYNC (drive-cache barrier — stronger and slower); on Linux plain fsync (DD-7, corrected)
- no capacity claims: fixed closed-loop response numbers, not a maximum

Source report: `benchmarks/reports/2026-07-30-a5796a59d19f.json`
<!-- bench:end -->
