# DESIGN — mini-kafka

**Status: DRAFT v0.1 (2026-07-24). Not locked.**
**Upstream: SPEC LOCKED v1.0, sha256 `2b002d99cf9248021ca1ca0bf7cf228ce22cecf5328e70a4827759de3de023c9` — verified at preflight.**

## 1. Shape

One Go module, stdlib-only on every runtime path (NFR-1). One broker process; clients speak a small synchronous binary protocol over TCP. All durability lives in per-partition append-only files plus tiny atomically-replaced metadata files. The demo, benchmark, and (conditional) showcase are just other `cmd/` programs using the same shipped client over real loopback TCP.

```
mini-kafka/
  go.mod                    module; Go version pinned here (floor: DD-18)
  cmd/minikafka/            broker binary
  cmd/mk/                   CLI: create-topic · topics · produce · consume (group or raw) — subcommand style, stdlib flag
  cmd/demo/                 DEMO-1/2: one command, two acts
  cmd/bench/                BENCH-1..3 harness + README renderer
  cmd/showcase/             SHOW-1..4 (conditional; still stdlib)
  client/                   public Go client library (PROT-2)
  wire/                     public: framing, message types, error codes — the registries PROTOCOL.md is audited against
  internal/storage/         partition logs, frontier, recovery
  internal/group/           coordinator: membership, generations, fencing, commits
  internal/broker/          TCP server, request dispatch, caps enforcement
  docs/PROTOCOL.md          hand-written; CI diffs its tables against wire registries (PROT-1)
  .github/workflows/ci.yml  the whole proof battery (OPS-2)
```

Public API surface = `client` + `wire`. Everything else is `internal/`.

## 2. Storage engine (LOG-1..5, TOP-1..2, PROD-2/3)

- **DD-1 (one file per partition + boot-scan index).** Each partition is a single append-only file `data/<topic>/<p>/log`. Offsets are ordinals (0,1,2…; LOG-2). An in-memory index (offset → byte position) is built by a full scan at boot and appended at runtime. Index memory grows with message count — accepted at demo scale, stated in README beside R1. No segments: retention is out (U6), and segment machinery is exactly the gold-plating a mini build should skip.
- **DD-2 (record format).** `[u32 len][u32 crc32c][payload]`, big-endian, CRC over the payload. Max payload 1 MiB (DD-14). A record is valid iff its length fits the file and the CRC matches (LOG-4's detector).
- **DD-3 (durable frontier).** Per partition, a tiny `frontier` file holds the byte length of the log covered by the last completed fsync. **Ack ordering — the load-bearing invariant: append → fsync(log) → write+fsync(frontier) → ack.** Boot recovery scans the log: an invalid record at byte position ≥ frontier is a torn unacked tail → truncate there (allowed: never acked). An invalid record < frontier is damage to acked data → the partition is refused loudly (served as error, named at startup), never silently truncated (LOG-4). A stale-low frontier is safe by construction: records past it were never acked, so truncating them breaks no promise.
- **DD-4 (group commit).** A per-partition flusher batches appends and fsyncs when the oldest waiter is 5 ms old or 64 KiB is pending, whichever first (numbers: DD-14). Producers block until their covering fsync + frontier update complete, then get their ack (PROD-2). The fsync call goes through a `storage.Syncer` interface — the injectable seam LOG-1's check (a) requires: the test's recorder asserts no ack precedes its covering sync.
- **DD-5 (fsync honesty).** Plain `File.Sync()` on both OSes. macOS caveat (fsync may not flush the drive cache; `F_FULLFSYNC` exists but is drastically slower and Linux has no equivalent): documented in PROTOCOL.md + README, and the benchmark label's "fsync mode" field says `fsync` explicitly (BENCH-2). We do not pretend to power-loss guarantees beyond what fsync gives (§1b of SPEC).
- **DD-6 (write failure).** Any append/fsync error (ENOSPC included): truncate the file back to the frontier, mark the partition write-rejecting, keep serving reads (LOG-5). Produces get `ERR_WRITE_FAILED` (PROT-3). A retry loop re-probes writability every 10 s. Boot after freeing space needs nothing special — recovery is the same scan.
- **DD-7 (topic metadata).** `data/<topic>/meta.json` (name, partitions, createdAt), written tmp → fsync → rename → dir-fsync (atomic; TOP-1). Partition count is read from meta.json only — no resize code path exists (TOP-2). Duplicate create → `ERR_TOPIC_EXISTS`.

## 3. Group coordinator (GRP-1..5, CONS-2/3)

- **DD-8 (membership + liveness).** In-memory group table: members (broker-issued memberID, TCP conn, lastHeartbeat), generation counter, assignment. Client heartbeats every 500 ms; session timeout 2 s (D7; numbers DD-14). Death = missed deadline or conn drop, whichever first.
- **DD-9 (eager rebalance with a join window).** On join/leave/death detection: bump generation, signal `REJOIN` in every heartbeat response, wait up to a 1 s join window for live members to re-join, then compute **range assignment** (sorted members × contiguous partition ranges) and return it in JoinGroup responses. Detection → new assignment ≤ 3 s worst-case (window + compute; GRP-2's bound). Worst partition-unowned time = session 2 s + 3 s = 5 s — inside the demo's narrated act two.
- **DD-10 (fencing).** Group fetches and commits carry (memberID, generation). Unknown member or stale generation → `ERR_STALE_GENERATION`, no state change (GRP-5). After a broker restart all group state except commits is gone; every pre-crash memberID is unknown → automatically fenced; clients rejoin cleanly.
- **DD-11 (commit storage).** Committed offsets (= next-to-read, D13) live in `data/_groups/<group>.json`: {topic, offsets{partition: next}}. Written tmp → fsync → rename → dir-fsync **before** the commit ack (CONS-3). Commits are low-rate (per batch, D4), so per-commit atomic-replace is fine and crash-safe by construction: readers see old or new, never torn.
- **DD-12 (join carries state).** JoinGroup response = (memberID, generation, assigned partitions, committed next-offset per partition). No separate offset-fetch round; a fresh group (no file) gets offset 0 per partition (D14 earliest).

## 4. Protocol & client (PROT-1..3, CONS-1, D1, D16)

- **DD-13 (synchronous frames, one in flight).** `[u32 frameLen][u8 version][u8 msgType][payload]`, hand-rolled with `encoding/binary`, big-endian; length-prefixed strings/bytes inside. One request in flight per connection — concurrency comes from connections, not pipelining (kills a whole class of ordering bugs; bench opens N connections). Message types: Produce, Fetch, CreateTopic, ListTopics, JoinGroup, Heartbeat, CommitOffsets, LeaveGroup + their responses + Error{code u16, msg}. Long-poll: Fetch{…, maxWaitMs, maxBytes} parks server-side until data or timeout, returns empty batch at the tail (CONS-1).
- **DD-14 (numbers — the authoritative caps & timing table, NFR-2 + GRP-2).**
  | knob | value | | knob | value |
  |---|---|---|---|---|
  | max payload | 1 MiB | | heartbeat interval | 500 ms |
  | max frame | 1 MiB + 4 KiB | | session timeout | 2 s |
  | fetch maxBytes cap / default | 4 MiB / 1 MiB | | rebalance join window | 1 s |
  | fetch maxWait cap / default | 30 s / 5 s | | detection→assignment bound | 3 s |
  | connections | 256 | | fsync group window | 5 ms / 64 KiB |
  | in-flight per conn | 1 | | write-retry probe | 10 s |
  | topics / partitions per topic | 64 / 16 | | topic & group name length | ≤ 128 bytes |
  | members per group | 32 | | | |
  Every cap rejects with its own PROT-3 error code. Design rule from SPEC NFR-2: any new unbounded input added later must land in this table first.
- **DD-15 (error registry).** `wire/errors.go`: stable u16 codes — UNKNOWN_TOPIC, TOPIC_EXISTS, BAD_PARTITION, MSG_TOO_LARGE, FRAME_TOO_LARGE, MALFORMED, STALE_GENERATION, WRITE_FAILED, CAP_EXCEEDED(one per DD-14 cap where distinct), plus messages. PROTOCOL.md's error table and message-type table are diffed against the `wire` registries by a CI test (PROT-1's audit).
- **DD-16 (client).** `client` package: `Producer` (sync Produce → offset), `Consumer` (raw fetch loop), `GroupConsumer` (join, heartbeat goroutine, fetch assigned, commit; on REJOIN/STALE re-joins and reissues). CLI `mk` = thin subcommand wrapper over `client` (PROT-2's "no privileged path" — demo and bench import the same package).

## 5. Demo, bench, showcase (DEMO-1..2, BENCH-1..3, SHOW-1..4)

- **DD-17 (demo topology).** `go run ./cmd/demo`: starts the broker **in-process** on a loopback port, then producers and two group consumers as goroutines connecting via the shipped client over real TCP (the wire path is the product; only process boundaries are simulated). Act one: create topic (4 partitions), produce, both consumers narrate consumption — first flow line is timestamped against command start (DEMO-1's clock). Act two: consumer 2's connections are hard-dropped and its heartbeats stop (indistinguishable from SIGKILL to the broker); the rebalance and takeover are narrated (DEMO-2). Machine-readable `#timing first-flow=<ms> total=<ms>` lines close each act.
- **DD-18 (60-second gate).** CI job "demo-timing": pinned `golang:<ver>` container, fresh checkout, cold module/build cache, `go run ./cmd/demo -ci`; the job fails if first-flow > 60 000 ms or total > 180 000 ms. The passing run's timing lines are committed as `docs/receipts/demo-timing.txt`. macOS approximation: the native-smoke job runs the same gate without the cold-cache guarantee.
- **DD-19 (bench harness).** `go run ./cmd/bench`: broker in-process (same honesty as demo), closed-loop load — C=8 producer connections, 1 KiB messages, 10 s per iteration after 2 s warm-up, ≥3 iterations; produce→ack latency from each producer's clock; end-to-end latency via a group consumer in the same process (one clock, no skew); percentiles by sort (stdlib). Output: `benchmarks/reports/<utc-date>-<git-commit>.json` carrying every BENCH-2 field (hardware string is a required flag for the committed reference report; runtime fills OS/arch/CPUs/GOMAXPROCS/Go version), plus per-iteration numbers and spread (BENCH-1's "reproducible"). `go run ./cmd/bench -render-readme` regenerates the README's numbers section verbatim from a named report; a CI grep-test asserts the README section matches the committed report (BENCH-3).
- **DD-20 (showcase — conditional, unchanged decision A4).** `cmd/showcase`: one process — broker in-process bound to 127.0.0.1 (SHOW-3 by construction, NFR-4), self-feeder goroutine producing ~2 msg/s of demo traffic, `net/http` server on `$PORT` serving one page + an SSE stream of recent messages (stdlib only, so NFR-1 holds even here; polling fallback). Disk bound (SHOW-4): feeder pauses above 200 MiB data-dir usage; platform disk is ephemeral anyway. Platform: Render free web service (no card — SHOW-2), `render.yaml` committed; sleeps/cold-start reality is already accepted (R6). **Feasibility is re-verified live in the showcase's own slice before any build effort; if Render's terms changed, R4 fires and the slice ships the documented "later" instead.**

## 6. Broker runtime & guards (NFR-2..4, OPS-1)

- **DD-21 (listener).** Default `127.0.0.1:7621` (unassigned port; NFR-4 loopback default). `--addr` must be passed explicitly to bind anything else; the README warning sits on that flag. `--data` defaults to `./data`. Connection cap, per-conn read deadline tied to heartbeat liveness, frame-length pre-check before allocation (NFR-2 enforcement point).
- **DD-22 (concurrency model).** One goroutine per connection (read loop → dispatch); per-partition append serialization through the storage flusher; coordinator state behind one mutex (single node, 256 conns — contention is not a design problem at this size; the crash-walk, not lock-splitting, is where correctness lives). Long-poll parks on per-partition condition variables woken by appends.
- **DD-23 (builds).** Each binary: `CGO_ENABLED=0 go build ./cmd/<name>` (OPS-1; pure-Go net resolver — no cgo paths on either OS). CI: pinned Go version + pinned `ubuntu-*`/`macos-*` runner images; jobs = unit+integration tests · `go vet` + staticcheck (pinned version, dev-tooling exemption NFR-1) · `go list -deps` stdlib audit over `client`, `wire`, `cmd/...` (golang.org/x/* counts external) · PROTOCOL.md registry diff · native build+smoke both OSes · demo-timing gate (DD-18).

## 7. Crash-walk (killed mid-X, what breaks?)

| Killed mid… | On restart |
|---|---|
| append, pre-fsync | invalid/short record ≥ frontier → truncated; nothing was acked (DD-3) |
| post-fsync(log), pre-frontier | records CRC-valid but ≥ stale frontier → truncated; **they were never acked** (ack strictly follows frontier fsync) — allowed by LOG-4 |
| post-frontier, pre-ack | data + frontier durable, client saw no ack → client may retry → duplicate (R2, at-least-once — correct) |
| commit write | tmp+rename = old or new commit file, never torn (DD-11); worst case = re-delivery from older committed offset (R2) |
| rebalance | group state is memory-only; all members' conns die with the broker, every old memberID is unknown on restart → fenced; clients rejoin from committed offsets (DD-10/12) |
| topic create | meta.json atomic rename: topic exists fully or not at all (DD-7) |

## 8. Coverage map (every SPEC ID → mechanism; audited by command)

| SPEC ID | Mechanism | | SPEC ID | Mechanism |
|---|---|---|---|---|
| LOG-1 | DD-3/DD-4 + Syncer seam | | PROT-1 | DD-15 registry diff + docs/PROTOCOL.md |
| LOG-2 | DD-1 ordinal offsets | | PROT-2 | DD-16 client + mk CLI |
| LOG-3 | DD-1 single append file, index order | | PROT-3 | DD-15 error registry |
| LOG-4 | DD-2 CRC + DD-3 frontier rule | | DEMO-1 | DD-17 clock + DD-18 CI gate |
| LOG-5 | DD-6 truncate-to-frontier, read-only degrade | | DEMO-2 | DD-17 act two |
| TOP-1 | DD-7 meta.json atomic create | | BENCH-1 | DD-19 harness |
| TOP-2 | DD-7 no resize path | | BENCH-2 | DD-19 report fields |
| PROD-1 | DD-13 Produce/resp offset | | BENCH-3 | DD-19 render-readme + CI grep |
| PROD-2 | DD-3 ack ordering + DD-4 | | SHOW-1 | DD-20 SSE watch-only page |
| PROD-3 | DD-14 payload cap + DD-15 code | | SHOW-2 | DD-20 Render no-card + teardown note |
| CONS-1 | DD-13 long-poll fetch | | SHOW-3 | DD-20/DD-21 loopback broker |
| CONS-2 | DD-11/DD-12 commit = next | | SHOW-4 | DD-20 feeder disk cap |
| CONS-3 | DD-11 fsync-before-ack commits | | OPS-1 | DD-23 CGO=0 builds + native smoke |
| GRP-1 | DD-9 range assignment per generation | | OPS-2 | DD-23 pinned CI battery |
| GRP-2 | DD-8/DD-9 bounds (DD-14 numbers) | | OPS-3 | README top-screen rule + LICENSE (MIT, SPEC A2) |
| GRP-3 | DD-10..12 resume from committed | | NFR-1 | DD-23 go-list audit; DD-20 stdlib showcase |
| GRP-4 | DD-11 per-group files; DD-12 earliest | | NFR-2 | DD-14 table + DD-21 enforcement |
| GRP-5 | DD-10 generation fencing | | NFR-3 | DD-23 vet+staticcheck; package docs |
| | | | NFR-4 | DD-21 loopback default |

Nothing is deferred; all 37 IDs have mechanisms above.

## 9. Test seams (so the SPEC's checks are actually writable)

- `storage.Syncer` interface (DD-4) — LOG-1(a)'s ack-vs-fsync ordering recorder.
- `storage` fault injection: a `File` interface allowing scripted short-writes/ENOSPC (LOG-4's four cases, LOG-5, scenario J).
- Coordinator clock is injectable (heartbeat/session/window timing tests without real sleeps; GRP-2's measured bound).
- Demo/bench take a `-listen 127.0.0.1:0` ephemeral port — parallel tests never collide.
