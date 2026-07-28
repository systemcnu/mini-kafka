# DESIGN — mini-kafka

**Status: LOCKED v1.0.1 (erratum E1 2026-07-28, accepted by Sri at the SL0 exit gate: crash-walk row 2 wording aligned to DD-4's normative algorithm — no behavior change). DD rows frozen.**
**Upstream: SPEC LOCKED v1.0.1, sha256 `50237dd479caf8bbf7268d92304699efaaf0ed9834646d483bf3d018e313dcfe` — verified at preflight.**
Locked by Sri at the gate 2026-07-28 after the 3-question quiz (two clean, one re-ask answered correctly). Zero decisions were open; all DD rows accepted by silence.
v0.1 → v0.2: 37 lie-hunt findings integrated (4 seats — coverage 8, buildability 9, simplicity 10, Codex platform-reality 10). Biggest changes: split control/fetch connections, read visibility capped at the durable frontier, atomic checksummed frontier, multi-partition fetch, immediate rebalance (join window deleted), externally-measured demo clock, name validation, showcase loading shim.

## 1. Shape

One Go module, stdlib-only on every runtime path (NFR-1). One broker process; clients speak a small synchronous binary protocol over TCP. All durability lives in per-partition append-only files plus tiny atomically-replaced metadata files. The demo, benchmark, and (conditional) showcase are `cmd/` programs using the same shipped client over real loopback TCP.

```
mini-kafka/
  go.mod                    module; Go toolchain version pinned here
  cmd/minikafka/            broker binary
  cmd/mk/                   CLI: create-topic · topics · produce · consume (group or raw)
  cmd/demo/                 DEMO-1/2: one command, two acts
  cmd/bench/                BENCH-1..3 harness + README renderer
  cmd/showcase/             SHOW-1..4 (conditional; still stdlib)
  client/                   the ONLY public package (PROT-2); re-exports error codes
  internal/wire/            framing, message types, error codes — the registries PROTOCOL.md is audited against
  internal/storage/         partition logs, frontier, recovery, flusher
  internal/group/           coordinator: membership, generations, fencing, commits
  internal/broker/          TCP server, dispatch, caps enforcement
  docs/PROTOCOL.md          hand-written; CI diffs its tables against wire registries (PROT-1)
  .github/workflows/ci.yml  the proof battery (OPS-2)
```

- **DD-1 (public surface = `client` only).** `wire` lives under `internal/` — external users never import it; `client` re-exports the error codes in its API. PROTOCOL.md serves readers writing their own clients from scratch (PROT-1), not importers.

## 2. Storage engine (LOG-1..5, TOP-1..2, PROD-2/3)

- **DD-2 (one file per partition + boot-scan index).** Partition = single append-only file `data/<topic>/<p>/log`. Offsets are ordinals (LOG-2). In-memory index (offset → byte position) built by full boot scan, appended at runtime. Index memory grows with message count — accepted at demo scale, stated in README beside R1. No segments: retention is out, and segment machinery is gold-plating here.
- **DD-3 (record format).** `[u32 len][u32 crc32c][payload]`, big-endian, CRC over payload. A record is valid iff its full byte range fits the file and the CRC matches.
- **DD-4 (durable frontier — atomic and checksummed).** Per partition, `frontier` holds `[u64 length][u32 crc32c]` (length = bytes of log covered by the last completed fsync; must land exactly on a record boundary). Every update uses the atomicWrite recipe (DD-9) — the live file is never torn. **Ack ordering, the load-bearing invariant: append → fsync(log) → atomicWrite(frontier) → ack.** Boot recovery parses the log from 0:
  - every record whose byte range intersects `[0, frontier)` must be valid and the parse must consume exactly `frontier` bytes — anything else (including a record straddling the boundary) is damage to acked data → **refuse the partition loudly** (LOG-4);
  - past the frontier: parse until the first invalid record, truncate to the end of the last valid record — that data was never acked (a stale-low frontier only widens this safe zone);
  - `frontier` unreadable, CRC-bad, beyond file length, or not on a record boundary → refuse loudly (atomicWrite makes a benign torn frontier impossible, so an invalid one is real corruption);
  - empty log + missing frontier = fresh partition → initialize to 0; non-empty log + missing frontier → refuse.
- **DD-5 (read visibility = the frontier).** **Fetch never serves past the durable frontier.** Consumers only ever see fsync-covered records. This one rule makes crash-truncation invisible to readers (no consumed-then-reassigned offsets), removes the truncate-vs-read race, and defines "committable": a consumer can only have read — hence only commit — durable offsets. Long-poll wakeups fire on frontier advance (post-fsync in the flusher), not on raw append.
- **DD-6 (group-commit flusher).** Per-partition flusher batches appends and fsyncs when the oldest waiter is 5 ms old (time-only trigger — a byte trigger would be dead machinery at our message sizes). Producers block until their covering fsync + frontier update complete, then get acks (PROD-2). The fsync call goes through a `storage.Syncer` interface — the injectable seam for LOG-1's check (a): a recorder asserts no ack precedes its covering sync.
- **DD-7 (fsync honesty).** Plain `File.Sync()` on both OSes. macOS caveat (fsync may not flush the drive cache; `F_FULLFSYNC` exists, is much slower, and Linux has no equivalent): documented in README + PROTOCOL.md; the benchmark label's "fsync mode" field says `fsync` (BENCH-2). All "old or new, never torn" claims in this design are qualified by this platform limit.
- **DD-8 (write failure — sticky degrade).** Any append/fsync error (ENOSPC included): attempt truncate-back-to-frontier + fsync; the partition becomes write-rejecting **until restart** (no runtime re-probe — restart's recovery scan is the only re-admission path). Produces get `ERR_WRITE_FAILED`. Reads continue and are already frontier-capped (DD-5), so even a failed truncate never exposes a torn record. If the recovery-relevant state itself can't be verified, the partition is refused, not guessed at. (LOG-5; Scenario J.)
- **DD-9 (atomicWrite recipe + topic creation order).** atomicWrite(path, bytes): create temp **in the destination directory** → write → fsync(temp) → rename → fsync(directory). Topic creation: make partition dirs, create empty `log` + `frontier` files, fsync files, fsync each new dir and the topic dir, then write `meta.json` (name, partitions, createdAt) via atomicWrite **last** — meta.json's presence is the topic's existence. Boot removes any topic dir lacking meta.json (aborted create). Duplicate create → `ERR_TOPIC_EXISTS`; partition count is read from meta.json only, no resize path exists (TOP-1/2).

## 3. Group coordinator (GRP-1..5, CONS-2/3)

- **DD-10 (membership + liveness on a dedicated control connection).** A group member = one **control connection** (JoinGroup, Heartbeat every 500 ms, CommitOffsets, LeaveGroup). Liveness is judged ONLY on the control connection: missed 2 s session deadline or control-conn drop = death. Fetch connections (DD-19) carry no liveness meaning — a parked long-poll can never get a member declared dead.
- **DD-11 (immediate rebalance — no join window).** On any membership event (join, leave, death): bump generation, recompute **range assignment** (sorted members × contiguous partition ranges) from the current live set immediately, and signal `REJOIN` in heartbeat responses; members re-JoinGroup to receive the new assignment. Rapid events produce a few extra harmless generations (≤32 members — assignment is a sort). Detection → new assignment bound: 1 s. Worst partition-unowned time = 2 s session + 1 s = 3 s (GRP-2; comfortably inside the demo's narrated act two). A fresh group's first member is assigned immediately. Generation bumps do NOT wake parked fetches — fencing is enforced at serve time (DD-12).
- **DD-12 (fencing at serve time).** Group fetches and commits carry (memberID, generation), validated **when the request is served** (a parked fetch that wakes after a rebalance gets `ERR_STALE_GENERATION`). Unknown member or stale generation → error, zero state change (GRP-5). After broker restart, group state except commits is gone; every pre-crash memberID is unknown → auto-fenced; clients rejoin cleanly from committed offsets.
- **DD-13 (commit storage).** Committed offsets (= next-to-read, SPEC D13) in `data/_groups/<group>.json` {topic, offsets{partition: next}}, written via atomicWrite **before** the commit ack (CONS-3). Low-rate (per batch, D4); crash mid-commit = old or new file, never torn (qualified per DD-7).
- **DD-14 (join carries state).** JoinGroup response = (memberID, generation, assigned partitions, committed next-offset per partition). Fresh group → offset 0 per partition (D14 earliest). No separate offset-fetch round.

## 4. Protocol & client (PROT-1..3, CONS-1, D1, D16)

- **DD-15 (framing + multi-partition fetch).** `[u32 frameLen][u8 version][u8 msgType][payload]`, hand-rolled put/get with `encoding/binary` primitives (no reflection), big-endian, length-prefixed strings/bytes, decoded into bounded buffers sized by the frame caps. One request in flight per connection — concurrency = connections. Types: Produce, **Fetch{topic, entries:[{partition, offset}], maxWaitMs, maxBytes}** (parks until ANY listed partition has durable data or timeout; raw consumers send one entry — this is what lets one member serve all its owned partitions on one fetch conn), CreateTopic, ListTopics, JoinGroup, Heartbeat, CommitOffsets, LeaveGroup, + responses + Error{code u16, msg}. Frame caps are asymmetric: request ≤ 1 MiB + 4 KiB; response ≤ 4 MiB + 64 KiB (so a max-maxBytes fetch response is legal — caps in DD-16).
- **DD-16 (numbers).** Two classes, split deliberately:
  **Input caps** (each rejects with its own error code + has a rejection test): payload ≤ 1 MiB · request frame ≤ 1 MiB + 4 KiB · response frame ≤ 4 MiB + 64 KiB · fetch maxBytes ≤ 4 MiB (default 1 MiB) · fetch maxWait ≤ 30 s (default 5 s) · fetch entries ≤ 16 · connections ≤ 256 · topics ≤ 64 · partitions/topic ≤ 16 · groups ≤ 64 · members/group ≤ 32 · topic & group name ≤ 128 bytes (format: DD-18).
  **Internal timings** (documented, no error codes): heartbeat 500 ms · session timeout 2 s · detection→assignment 1 s · fsync window 5 ms · idle-connection reclaim 5 min.
  Design rule from SPEC NFR-2: any new unbounded input must land in the caps list first. (Group count was exactly such a hole in v0.1 — now capped.)
- **DD-17 (error registry).** `internal/wire/errors.go`, stable u16 codes: UNKNOWN_TOPIC, TOPIC_EXISTS, BAD_PARTITION, INVALID_NAME, MSG_TOO_LARGE, FRAME_TOO_LARGE, FETCH_TOO_WIDE, CAP_EXCEEDED, STALE_GENERATION, UNKNOWN_MEMBER, WRITE_FAILED, MALFORMED. PROTOCOL.md's message-type and error tables are diffed against the registries by a CI test (PROT-1's audit). `client` re-exports the codes (DD-1).
- **DD-18 (name validation).** Topic and group names must match `^[a-z0-9][a-z0-9._-]{0,127}$` — validated in the protocol layer before any filesystem path is formed; `..`, path separators, empty and NUL are structurally impossible. Violation → `ERR_INVALID_NAME` (closes Scenario H's hostile-name hole).
- **DD-19 (client connection architecture).** `client` package: `Producer` (one conn, sync Produce → offset), `Consumer` (one conn, raw fetch loop), `GroupConsumer` (control conn per DD-10 + **one fetch conn** carrying multi-partition long-poll fetches for all owned partitions + commit via control conn; on REJOIN/STALE it re-joins and reissues fetches). Read/write deadlines wrap only actual wire I/O — never a server-side park. CLI `mk` = thin subcommands over `client` (PROT-2's no-privileged-path: demo and bench import the same package).

## 5. Demo, bench, showcase (DEMO-1..2, BENCH-1..3, SHOW-1..4)

- **DD-20 (demo topology).** `go run ./cmd/demo`: broker in-process on an ephemeral loopback port, data dir from `os.MkdirTemp` (a second run collides with nothing), producers + two GroupConsumers as goroutines over real TCP via the shipped client. Act one: create topic (4 partitions), produce, both consumers narrate. Act two: consumer 2's connections are hard-dropped and its heartbeats stop (SIGKILL-equivalent to the broker); takeover narrated (DEMO-2). The demo prints `#event first-flow` / `#event done` marker lines.
- **DD-21 (60-second gate — measured from outside).** A process cannot see its own `go run` compile time, so the clock is external: the CI job "demo-timing" (pinned `golang:<ver>` container, fresh checkout, cold GOCACHE/GOMODCACHE) starts a shell timer, launches `go run ./cmd/demo -ci`, and records wall-clock at each `#event` line as it streams. Gate: external first-flow ≤ 60 s, total ≤ 180 s; receipt (external times + resolved image digest + commit) committed as `docs/receipts/demo-timing.txt`. Shell here is CI harness, not the visitor path — D8 binds only what a visitor types. macOS leg per the SPEC's own split: CI macOS does build + short smoke only; the macOS timing evidence is a documented local clean-cache run (GOCACHE/GOMODCACHE pointed at fresh temp dirs — no nuking Sri's real caches), receipt committed alongside.
- **DD-22 (bench harness).** `go run ./cmd/bench`: broker in-process, MkdirTemp data dir, closed-loop load — C=8 producer conns, 1 KiB messages, 10 s/iteration after 2 s warm-up, ≥3 iterations; produce→ack latency at each producer; end-to-end latency via one in-process group consumer (one clock, no skew); percentiles by sort. **Honesty labels (BENCH-2 + Codex):** the report is titled "closed-loop response latency", carries achieved throughput, GC pause totals (`runtime.ReadMemStats`), and a stated caveat that closed-loop load understates queueing tails; no capacity claims anywhere. Output: `benchmarks/reports/<utc-date>-<commit>.json` with every BENCH-2 field (hardware string = required flag for reference reports). `-render-readme` regenerates the README numbers section verbatim from a named report; a CI test asserts the README section matches the committed report (BENCH-3).
- **DD-23 (showcase — conditional, SPEC A4).** `cmd/showcase`: one process — broker in-process on 127.0.0.1 (SHOW-3 by construction), self-feeder ~2 msg/s, stdlib `net/http` on `0.0.0.0:$PORT` serving the page + a 10 s-poll JSON feed (polling is also what keeps a watched instance awake on Render — awake exactly while watched; SSE optional garnish, it does not count as inbound activity there). **Entry point = a GitHub Pages static page** (free, always-up): shows "waking the showcase (~1 min)", polls the Render URL, swaps in when live — that is SHOW-1's loading state, buildable even though the sleeping process is the web server. Disk bound: feeder pauses above a threshold (default 200 MiB, verified against the real instance's allowance before ship) + ephemeral disk resets (SHOW-4). **SHOW-2 evidence on record: Render free web services require no payment method (render.com/docs/free, checked 2026-07 during the SPEC lie-hunt). Teardown criterion: if Render's free tier gains a card requirement, starts charging, or the workspace's free instance hours run out — the service is deleted, and the README's showcase link reverts to "not currently hosted".** Port-scan procedure (SHOW-3's check): after each deploy, from any external host, a scripted TCP-connect sweep of the service hostname (the broker port + a sample range) must show only HTTPS answering; script + expected output live in the repo; the showcase slice implements it. Live feasibility is re-verified in that slice before build effort; if reality changed, R4 fires and the slice ships the documented "later".

## 6. Broker runtime & guards (NFR-2..4, OPS-1)

- **DD-24 (listener).** Default `127.0.0.1:7621`; binding anything else requires an explicit `--addr` (NFR-4; README warning sits on the flag). `--data` defaults to `./data` (broker binary only — demo/bench use temp dirs). Enforcement at the edge: frame-length pre-check before allocation, connection cap with a served `ERR_CAP_EXCEEDED` frame before close (accept → write error → close), idle-connection reclaim after 5 min without a request (protects the cap from leaked conns; heartbeating control conns are never idle).
- **DD-25 (concurrency model).** Goroutine per connection (read → dispatch → respond). Per-partition append serialization through the flusher. Coordinator state behind one mutex — never held while parked or during I/O. **Long-poll parking pattern (sync.Cond has no timed wait — not used):** each partition keeps a `chan struct{}` closed on frontier advance and replaced; a fetch loops: check durable tail under read-lock → if empty, capture current channel, release locks, `select { <-ch; <-timer; <-connCtx.Done() }` → recheck (kills the missed-wakeup race; per-partition channels avoid global wake storms); group fetches re-check generation at serve time (DD-12).
- **DD-26 (builds + CI).** Each binary: `CGO_ENABLED=0 go build ./cmd/<name>` (pure-Go resolver, both OSes). CI: pinned Go toolchain; runner labels are mutable images, so every job **records the resolved image/runner version into its receipt** rather than claiming immutability; staticcheck pinned to an explicit release compatible with the toolchain, install cached. Jobs: unit+integration tests · `go vet` + staticcheck · `go list -deps` stdlib audit over `client`, `internal/...`, `cmd/...` (golang.org/x/* = external) · PROTOCOL.md registry diff · native build+smoke on both OSes · demo-timing gate (DD-21). Receipts name the commit they ran on.

## 7. Crash-walk (killed mid-X, what breaks?) — all rows qualified by DD-7's platform limit

| Killed mid… | On restart |
|---|---|
| append, pre-fsync | invalid/short record past frontier → truncated; nothing was acked (DD-4) |
| post-fsync(log), pre-frontier | CRC-valid records past the stale frontier are **kept but hidden** (DD-5 read cap) until a later fsync advances the frontier over them; truncation begins only at the first invalid record (DD-4's normative rule). **Never acked** (ack strictly follows frontier write), so a producer retry may duplicate (R2) |
| frontier atomicWrite | rename is atomic: old or new frontier, never torn (DD-4/DD-9) |
| post-frontier, pre-ack | data + frontier durable, client unacked → client retry → duplicate (R2, correct) |
| commit write | atomicWrite: old or new commit file (DD-13); worst case re-delivery (R2) |
| rebalance | group state memory-only; all conns died; every old memberID unknown → fenced; rejoin from committed offsets (DD-12/14) |
| topic create | meta.json written last via atomicWrite: topic fully exists or boot removes the meta-less dir (DD-9) |
| boot recovery / runtime truncate | recovery is idempotent (re-scan reproduces the same decisions); reads never exceed the frontier regardless (DD-5/DD-8) |

## 8. Coverage map (every SPEC ID → mechanism; audited by command)

| SPEC ID | Mechanism | | SPEC ID | Mechanism |
|---|---|---|---|---|
| LOG-1 | DD-4/DD-6 ack ordering + Syncer seam | | PROT-1 | DD-17 registry diff + docs/PROTOCOL.md |
| LOG-2 | DD-2 ordinal offsets | | PROT-2 | DD-19 client + mk CLI |
| LOG-3 | DD-2 single file, index order | | PROT-3 | DD-17 error registry + DD-18 |
| LOG-4 | DD-3 CRC + DD-4 boundary rules | | DEMO-1 | DD-20/DD-21 external clock + CI gate |
| LOG-5 | DD-8 sticky degrade, frontier-capped reads | | DEMO-2 | DD-20 act two |
| TOP-1 | DD-9 atomic create + DD-18 names | | BENCH-1 | DD-22 harness |
| TOP-2 | DD-9 no resize path | | BENCH-2 | DD-22 labels + caveats |
| PROD-1 | DD-15 Produce/resp offset | | BENCH-3 | DD-22 render-readme + CI test |
| PROD-2 | DD-4/DD-6 ack ordering | | SHOW-1 | DD-23 watch-only page + Pages loading shim |
| PROD-3 | DD-16 payload cap + DD-17 code | | SHOW-2 | DD-23 no-card evidence + teardown criterion |
| CONS-1 | DD-15 multi-partition long-poll | | SHOW-3 | DD-23 loopback broker + scan procedure |
| CONS-2 | DD-13/DD-14 commit = next | | SHOW-4 | DD-23 feeder disk cap + ephemeral reset |
| CONS-3 | DD-13 atomicWrite-before-ack | | OPS-1 | DD-26 CGO=0 builds + native smoke |
| GRP-1 | DD-11 range assignment per generation | | OPS-2 | DD-26 CI battery, receipts record versions |
| GRP-2 | DD-10/DD-11 bounds (3 s worst) | | OPS-3 | README top-screen rule + LICENSE (MIT, SPEC A2) |
| GRP-3 | DD-12..14 resume from committed | | NFR-1 | DD-26 go-list audit; DD-23 stdlib showcase |
| GRP-4 | DD-13 per-group files; DD-14 earliest | | NFR-2 | DD-16 caps + DD-24 edge enforcement |
| GRP-5 | DD-12 serve-time fencing | | NFR-3 | DD-26 vet+staticcheck; package docs |
| | | | NFR-4 | DD-24 loopback default |

Nothing is deferred; all 37 IDs have mechanisms above.

## 9. Test seams (so the SPEC's checks are actually writable)

- `storage.Syncer` (DD-6) — LOG-1(a)'s ack-vs-fsync ordering recorder.
- `storage` file interface with scripted short-writes/ENOSPC/corruption (LOG-4's four cases, LOG-5, scenarios C/J — including the straddling-record case).
- Injectable coordinator clock (GRP-2's measured detection→assignment bound without real sleeps).
- Frontier-advance notification hook (long-poll wake tests without timing races).
- Demo/bench accept `-listen 127.0.0.1:0` and temp data dirs — parallel tests never collide.

## 10. Declined findings (with reasons — nothing silently dropped)

- Codex-8's open-loop/paced benchmark run: **declined as scope** — SPEC BENCH makes no capacity claims and the report now says so explicitly; the closed-loop label + caveat is the honest fix at this size. Revisitable post-v1 without unlocking anything.
