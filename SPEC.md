# SPEC — mini-kafka

**Status: LOCKED v1.0 (2026-07-24). IDs frozen. Changes only via errata cascade.**
Locked by Sri at the gate 2026-07-24 after the 3-question quiz (two re-asks, both then answered on the brief's exact lines) and four gate decisions, resolved in §7.
v0.1 → v0.2: 46 lie-hunt findings integrated (5 seats — ambiguity 10, untestability 10, gaps 9, contradiction/goal-fit 8, Codex feasibility 9); new IDs LOG-5, CONS-3, GRP-5, PROT-3, SHOW-4, NFR-4. v0.2 → v1.0: gate decisions A1–A4 resolved; no requirement text changed.

## 0. Verbatim intent

> "idea: mini-kafka with basic features"

Given by Sri 2026-07-24, immediately after concluding the url-shortener testbed. Intent grilled the same day (8 questions); grill answers are ledger rows U1–U8 and are the user's own choices, not agent assumptions.

## 1. Goals (user-stated)

- **G1 — Portfolio piece.** A public repo whose job is to demonstrate systems-engineering skill to readers. (U1)
- **G2 — Success bar = two things only:** (a) a visitor can clone and see messages flow through the broker within 60 seconds; (b) the repo carries honest, reproducible benchmarks. Sri explicitly did NOT make an architecture writeup or a showcase test suite success criteria. (U2)
- **G3 — Local-first, $0-hard hosting.** Everything runs on the visitor's machine. A hosted watch-only showcase ships ONLY if a genuinely free no-card tier carries it; otherwise the project is local-only and hosted is a documented "later". (U3, U4, U8)

## 1b. Definitions (used throughout; each is a contract)

- **Durable**: written to the log and covered by a completed fsync (batched/group fsync allowed). An acked message or offset commit survives `kill -9` and OS restart, up to platform fsync limits — design documents per-OS caveats (macOS `fsync` vs `F_FULLFSYNC`, drive caches) and benchmark labels state the exact mode. Process-crash survival alone is NOT durability (page cache survives `kill -9`).
- **Committed offset**: the NEXT offset the group will read (Kafka convention). Resumption begins exactly at the committed offset; the message before it is never redelivered by a clean resume.
- **Group**: subscribes to exactly one topic (D15). Assignment happens in numbered **generations** (GRP-5).

## 2. Requirements

Every requirement carries the check that would prove it. "Broker" = the single server process; "client" = the shipped Go library and CLI tools.

### Log & topics

- **LOG-1 (durability).** A message acked to a producer is durable (§1b). *Check — two-part, both required: (a) invariant: via an injectable sync-recorder seam, assert no ack is ever emitted before the fsync covering its record completes; (b) end-to-end: produce N acked, `kill -9` mid-load, restart, all N intact. (The kill -9 test alone proves nothing about fsync — page cache survives process death.)*
- **LOG-2 (offsets).** Every message in a partition gets a broker-assigned offset, contiguous and monotonically increasing from 0, stable across restarts. *Check: after restart, offsets read back 0..N-1, no gaps, no renumbering.*
- **LOG-3 (order).** A consumer reading one partition receives messages in offset order, every time. *Check: sequence-tagged payload test.*
- **LOG-4 (recovery fault model).** Records carry checksums. On startup, recovery scans the tail: a torn or checksum-failing record AFTER the durable boundary (never acked) is truncated silently. Damage AT OR BEFORE the durable boundary (acked data) is detected and refused loudly — the broker names the damaged partition and refuses to serve it as healthy; it never silently drops acked data and never serves a partial or corrupt record. *Check: four separate fault-injection tests — short write · torn unacked tail record · checksum-corrupt tail record · corrupted acked region → startup refuses with a named error.*
- **LOG-5 (write failure).** On disk-full or I/O error in the produce path: the produce is rejected with a clear error (PROT-3), nothing is corrupted, and the broker keeps serving fetches of existing data. After space is freed and a restart, no manual repair is needed. *Check: produce into a quota-limited filesystem → clean rejection, fetches still serve; free space, restart → healthy.*
- **TOP-1 (topics).** Topics are created explicitly by name with a fixed partition count; creating an existing topic is a clear error; topics are listable. No auto-create (D3). *Check: create/list/duplicate-create CLI test.*
- **TOP-2 (fixed partitions).** Partition count is set at creation and never changes. *Check: the protocol's documented message set contains no resize operation (doc audit), and partition count in topic metadata is byte-identical across restarts (test).*

### Produce

- **PROD-1 (produce).** A producer sends an opaque-bytes message to a chosen (topic, partition) — chosen explicitly or round-robin by the client; the broker never routes by key (U6). The ack returns the assigned offset. *Check: roundtrip test asserts returned offset matches consumed offset.*
- **PROD-2 (ack = durable).** An ack is sent only after the record is durable (§1b). Batching/group-commit of fsync is design's call; the guarantee is not. There is exactly one ack semantic — no non-durable fast mode ships (D17, confirmed at lock). *Check: the LOG-1 pair proves it.*
- **PROD-3 (size cap).** The cap applies to payload bytes (frame overhead is on top; both documented). Oversized messages are rejected with the error naming the cap; nothing is written. *Check: oversized produce → PROT-3 error naming limit; partition byte-count unchanged.*

### Consume & groups

- **CONS-1 (long-poll fetch).** A consumer fetches from (topic, partition, offset) with a max-wait: data returns immediately when present, else the fetch waits up to max-wait and returns empty — never an error at or past the tail (D16). *Check: fetch-at-tail returns empty after max-wait · blocked fetch wakes on produce · fetch-from-zero returns data.*
- **CONS-2 (committed offsets).** A group member commits the group's position (§1b: next-to-read) to the broker; a restarted consumer resumes exactly there. *Check: consume half, commit, restart consumer → first message received is exactly the committed offset.*
- **CONS-3 (commit durability).** An acked offset commit is durable (§1b): group positions survive broker `kill -9`. *Check: commit, kill -9 broker, restart → group resumes from the committed offset.*
- **GRP-1 (group assignment).** In every generation, the broker maps each of the topic's partitions to exactly one live member — no overlap, none unowned while the group has members. *Check: 2 members, 4 partitions → assignment table is an exact partition of the set, tagged with its generation.*
- **GRP-2 (rebalance).** On member join, leave, or missed liveness deadline (numbers in design), the broker produces a new generation within a stated bound measured FROM DETECTION. Worst-case partition-unowned time = liveness deadline + rebalance bound; both numbers live in design and must be small enough for the demo's narrated takeover (Scenario A). *Check: instrumented test measures marked-dead → new-generation time against the bound; kill-one-of-two e2e as complement.*
- **GRP-3 (at-least-once).** After any rebalance or consumer restart, delivery resumes from each partition's committed offset; everything at or after it is (re)delivered. Duplicates permitted; loss is not (U6). *Check: crash a member mid-batch before commit → union of messages processed across the group = all produced.*
- **GRP-4 (fan-out).** Independent groups have independent committed offsets: two groups on one topic each receive the full stream. A group with no committed offset starts at the earliest available offset (D14). *Check: second group joins after the first finished → still consumes all N from 0.*
- **GRP-5 (fencing).** Fetches and commits carry their assignment generation; the broker rejects both from a member whose generation is stale. A timed-out-but-still-running member can never advance or rewind the group's position. *Check: pause one member past its deadline, let rebalance complete, resume it → its commit and fetch get the fencing error; group position unchanged by them.*

### Protocol & clients

- **PROT-1 (own protocol, documented).** The broker speaks its own versioned protocol (framing/transport = design). `docs/PROTOCOL.md` documents the framing, every message type, every limit (NFR-2), and every error code (PROT-3). Real Kafka clients are NOT supported (U6). *Check: doc-completeness audit — the doc's message-type and error-code lists match the implementation's registries, command-audited in CI.*
- **PROT-2 (shipped clients).** The repo ships a Go client library plus CLI producer/consumer/admin tools built on it — the same client code the demo and benchmarks use (no privileged path). *Check: demo and bench import the same client package; CLI smoke test.*
- **PROT-3 (error contract).** Every client-visible failure has a stable error code and human-readable message; at minimum: unknown topic · bad partition index · oversized message · malformed or oversized frame · stale generation · write failure (LOG-5). Errors never take the broker down. *Check: one test per listed error asserting the code and that the broker still serves.*

### Demo (success bar, G2a)

- **DEMO-1 (60-second demo).** The demo is Go-native: `go run ./cmd/demo` — no make, shell scripts, or non-Go tools anywhere in the visitor path (D8). The clock starts when the command starts (cloning is excluded — visitor network speed is not ours). Within 60 seconds, messages visibly flow producer → broker → consumers. Baseline: only a supported Go toolchain, empty Go build/module caches. *Check: a scripted clean-container (Linux) run gates the wall-clock — over 60s = FAIL; receipt commits the measured time; macOS approximated by a clean-cache local run.*
- **DEMO-2 (rebalance visible).** The demo's second act kills one consumer and narrates the takeover; the whole demo completes within 3 minutes. *Check: transcript shows the takeover; receipt records total time.*

### Benchmarks (success bar, G2b)

- **BENCH-1 (harness).** `go run ./cmd/bench` measures at minimum: produce throughput (msgs/s and MB/s), produce→ack latency (p50/p99), and end-to-end produce→consume latency (p50/p99, co-located long-poll consumer, one clock). At least 3 iterations; per-iteration numbers and spread in the report — that spread is what "reproducible" means here. *Check: one command emits a machine-written report containing all of the above.*
- **BENCH-2 (honesty floor).** Every published number is labeled with: commit, hardware, OS, Go version, GOMAXPROCS, storage type, message size, batching/in-flight settings, fsync mode, load model (open/closed + concurrency), run duration, warm-up, percentile method, and error/duplicate counts. *Check: the report schema contains every listed field; README numbers are pasted from a committed report.*
- **BENCH-3 (no hand-edited numbers).** README numbers are regenerated by command and traceable to a committed report file. A number appearing in no report is a defect. *Check: grep README numbers against the committed report.*

### Showcase (conditional — every SHOW row is void if no genuinely free no-card tier exists at build time; that outcome is documented as the alternative)

Platform reality on record (Codex, 2026-07): the one credible no-card option is Render's free web-service tier — instances sleep after ~15 idle minutes, cold-start in ~1 minute, disk is ephemeral, restarts may happen anytime. The rows below are written against that reality. Kept conditional at lock (A4).

- **SHOW-1 (watch-only).** A public page visualizes live message flow of a self-driving instance (it feeds itself demo traffic while awake). A sleeping instance cold-starts on visit behind a loading state. Visitors have no write path of any kind. *Check: the deployed surface serves only the read-only page/stream.*
- **SHOW-2 ($0 enforced).** Free tier with NO payment method attached — no card, no cap-based fallback (U8). If the free tier ends or is exceeded: teardown, per a documented criterion. *Check: the platform requires no card (evidence recorded at design); teardown rule documented.*
- **SHOW-3 (broker not reachable).** The broker runs co-located behind the web process, bound to loopback (NFR-4); only the HTTP surface is exposed. *Check: deploy config asserts the loopback bind; a scripted external scan runs at each deploy per a procedure defined in design.*
- **SHOW-4 (bounded showcase).** The showcase bounds its own disk use (feed-rate cap + ephemeral restart-fresh state; mechanism in design). Risk R1's unbounded growth explicitly does NOT extend to the hosted case. *Check: sustained-run test shows disk usage plateaus or resets.*

### Ops & repo

- **OPS-1 (builds + platforms).** Each binary builds with an exact documented command (`go build ./cmd/<name>`), `CGO_ENABLED=0`, static. CI natively builds AND smoke-runs on pinned Linux and macOS runners — cross-compilation alone is not platform proof. *Check: CI matrix with a native smoke run on both.*
- **OPS-2 (CI).** Public GitHub repo (hosted runners are free for public repos — this is why OPS-2 is $0); tests, `go vet` + linter, and both-platform build+smoke green on every push to main. Go version, runner images, actions, and linter version pinned. *Check: green status at HEAD; pins present in the workflow file.*
- **OPS-3 (repo is the deliverable).** README's top screen contains the one demo command; the repo carries an OSI license (MIT, resolved at lock — A2). *Check: README audit; LICENSE file exists.*

### Non-functional

- **NFR-1 (stdlib-only runtime).** Single Go module. The claim covers the broker, client library, and shipped `cmd/` binaries: `go list -deps` over those packages shows standard library only — `golang.org/x/*` counts as external. Test-only and CI tooling are exempt and live outside those paths. *Check: the go-list audit runs in CI on every push.*
- **NFR-2 (bounded resources).** Authoritative cap inventory: message size · frame length · fetch batch size/bytes · fetch max-wait · concurrent connections · in-flight requests per connection · topic count · partitions per topic · members per group · topic-name and group-id length. Numbers in design; any new unbounded input design introduces must be added to this inventory (design-review rule). *Check: each cap has a rejection test; the inventory is cross-checked at the DESIGN gate.*
- **NFR-3 (readable as a portfolio artifact).** Every package has a doc header; exported API documented; `go vet` and the pinned linter clean in CI. *Check: lint gate in CI.*
- **NFR-4 (safe defaults).** The broker binds `127.0.0.1` by default; binding any non-loopback interface requires an explicit flag. This makes R3 true by construction, not by README warning. *Check: default-config broker rejects/never sees non-local connections (bind-address test).*

## 3. Scenarios (acceptance; every one owned by exactly one slice later)

- **A — First contact (the visitor's scenario).** Visitor clones, runs `go run ./cmd/demo` from the README top screen; first messages flow within 60 seconds of the command starting (DEMO-1); the demo then kills a consumer and narrates the takeover; everything done inside 3 minutes (DEMO-2, GRP-2). The demo's producers/consumers are the shipped client over long-poll fetch (PROT-2, CONS-1).
- **B — Restart durability.** Create a topic with a fixed partition count (TOP-1), produce N acked (PROD-1), stop broker normally, restart: all N consumable, in offset order, same offsets, partition count unchanged. (LOG-1, LOG-2, LOG-3, TOP-2)
- **C — Crash durability.** Produce with acks and commits, `kill -9` mid-load, restart: every acked message intact, group positions intact, torn tail truncated, no partial record ever served. (LOG-1, LOG-4, PROD-2, CONS-3)
- **D — Consumer resumes.** A committing consumer restarts and continues exactly from the committed offset (dupes only for uncommitted work). (CONS-2, GRP-3)
- **E — Rebalance under failure.** Two members split four partitions; one is killed; within liveness-deadline + rebalance-bound the survivor owns all four; the full stream is still delivered at-least-once; the dead member's later commits are fenced. (GRP-1, GRP-2, GRP-3, GRP-5)
- **F — Fan-out.** A second group with no committed offsets joins later and independently receives the full stream from offset 0. (GRP-4)
- **G — Benchmark run.** One command, ≥3 iterations, labeled machine-written report via the shipped client; README numbers match a committed report. (BENCH-1, BENCH-2, BENCH-3, PROT-2)
- **H — Hostile inputs.** Oversized message, unknown topic, bad partition index, malformed frame, stale generation: each gets its stable error code; broker stays up; nothing is written. (PROT-3, PROD-3, NFR-2)
- **I — Showcase visit (conditional).** Visitor opens the page (cold start allowed), watches live traffic; external scan shows only the web surface; disk stays bounded; no card on the account. (SHOW-1, SHOW-2, SHOW-3, SHOW-4)
- **J — Disk full.** The log's disk fills: produces fail with the write-failure code, reads keep working, and recovery after freeing space needs no manual repair. (LOG-5, PROT-3)
- **K — Repo & CI.** A push to main natively builds and smoke-runs both platforms, passes vet/lint, the stdlib-only audit, and the protocol-doc completeness audit; the README top screen carries the demo command and a LICENSE file exists. (OPS-1, OPS-2, OPS-3, NFR-1, NFR-3, PROT-1)

## 4. Out of scope (user-stated U6 unless noted)

- Log retention & compaction — local/dev disks grow (risk R1); the hosted showcase is separately bounded (SHOW-4).
- Keyed partition routing (same-key-same-partition ordering).
- Exactly-once delivery / transactions — at-least-once is the contract.
- Kafka wire-protocol compatibility — real Kafka clients will not connect.
- Replication / multi-node / failover (U5 tier choice).
- Any visitor-writable hosted surface (U4).
- Auth/TLS on the broker protocol — local by default (NFR-4 loopback); the one hosted case never exposes the port (SHOW-3).
- Topic deletion (D18, drafter-decided) — the offline remedy is stop-broker + delete data dir; attackable at the gate.
- Architecture writeup and test-suite-as-showcase as success criteria (U2) — docs and tests exist; they are not what success is measured by.

## 5. Decision ledger

### User-stated (from the 2026-07-24 grill — Sri's own choices)

| # | Decision |
|---|----------|
| U1 | Purpose: portfolio piece — demonstrate systems skill to readers. |
| U2 | Success = 60-second demo + honest benchmarks. Architecture writeup and test-rigor-showcase explicitly NOT success criteria. |
| U3 | Runs on the visitor's machine; hosted demo optional, not load-bearing. |
| U4 | Hosted demo, if any, is a watch-only self-driving showcase — visitors cannot write. |
| U5 | Feature tier: core pub/sub log + consumer groups with rebalancing. |
| U6 | Explicitly OUT: retention/compaction, keyed routing, exactly-once/transactions, Kafka wire-protocol compat. |
| U7 | Language: Go. |
| U8 | Hosting cost: $0 hard — free tier only, else local-only and hosted is a documented later. |

### Drafter decisions (each attackable; silence at lock = accepted)

| # | Decision | Why | Consequence if wrong |
|---|----------|-----|----------------------|
| D1 | Own minimal protocol over TCP, framing = design's call, documented in `docs/PROTOCOL.md`. | Kafka-ish realism, zero deps. | HTTP/JSON would be simpler but less systems-y. |
| D2 | Pull-based consumption, like Kafka. | Simpler backpressure; matches the miniature. | Push changes the whole consumer/group design. |
| D3 | Explicit topic creation; no auto-create. | Predictable demo, clear errors. | One extra CLI step. |
| D4 | At-least-once with explicit commit; CLI tools auto-commit per batch for ergonomics. | Matches U6; makes GRP-3 provable. | Auto-commit-only hides the interesting semantics. |
| D5 | Stdlib-only runtime (NFR-1) as a stated portfolio claim. | Strong, checkable signal; stdlib suffices (percentiles = sort; showcase = net/http SSE/poll). | If a real need appears the claim narrows — ledgered, not silent. |
| D6 | Payloads are opaque bytes; broker never inspects content. | Simplicity. | None at this tier. |
| D7 | Liveness = client heartbeats + session timeout; enforcement = generation fencing (GRP-5). | Standard mechanism; fencing is what makes the guarantees honest. | Design may tune mechanisms; the guarantees are fixed. |
| D8 | Visitor path is Go-native: `go run ./cmd/demo`, `go run ./cmd/bench` — no make/bash/curl anywhere a visitor goes. Prebuilt binaries a bonus. | "Only Go installed" must be literally true, cross-platform. | If non-Go visitors matter, releases become a required path. |
| D9 | Single broker process, single node (restates U5/U6). | Tier choice. | None; replication is out. |
| D10 | Benchmarks: committed reference report from Sri's stated hardware + harness for visitors to reproduce locally. | Honest and free. | Server-grade numbers would need rented hardware — new scope. |
| D11 | GitHub hosts; repo public early (it IS the deliverable). New account → sr7544068@gmail.com per standing rule; existing-account choice is Sri's at the gate. | Portfolio needs a public home. | Publishing later just shifts OPS-2/3 timing. |
| D12 | Name "mini-kafka" — RESOLVED at lock (A1): Sri keeps it, trademark note acknowledged; now user-stated. | Naming a public artifact is the user's call. | Rename is cheap pre-publication, annoying after. |
| D13 | Committed offset = NEXT offset to read (Kafka convention), stated in §1b and the protocol doc. | Kills a silent off-by-one that tests can't catch. | None — any consistent convention works, but it must be one. |
| D14 | A group with no committed offset starts at the earliest offset. | Fan-out demo (Scenario F) works by contract, not luck. | Kafka defaults to latest; earliest is friendlier for a demo. |
| D15 | One group subscribes to exactly one topic. | Halves protocol and rebalance surface at zero demo cost. | Multi-topic groups are a Kafka feature readers might probe. |
| D16 | Fetch is long-poll with client max-wait. | No busy-polling in the demo; honest e2e latency measurable. | Slightly bigger protocol surface than immediate-return. |
| D17 | Exactly one ack semantic ships: durable. No labeled unsafe fast mode. CONFIRMED at lock (A3). | Honesty over impressiveness (R5); one semantic = one proof. | Benchmarks can't show a "fast mode" comparison line. |
| D18 | Topic deletion OUT (offline remedy documented). | Retention already out; delete adds API+recovery surface. | In-band cleanup impossible; disk remedy is manual. |
| D19 | License: MIT — RESOLVED at lock (A2). | Unlicensed public code is all-rights-reserved — defeats G1. | — |

## 6. Accepted risks

- **R1 — Local/dev disks grow without bound** (retention out, U6). Fine at demo scale; README states it. Does NOT cover the showcase — SHOW-4 bounds that separately.
- **R2 — Duplicates are possible** (at-least-once, U6). Stated in docs.
- **R3 — The broker protocol is unauthenticated.** Mitigated by construction: loopback-only default (NFR-4); the hosted case never exposes the port (SHOW-3). README still carries the warning.
- **R4 — The showcase may never ship** (U8). Accepted; the project is complete without it (U3).
- **R5 — Benchmark numbers are modest.** A single-node fsync-honest Go broker won't post Kafka-headline numbers; honesty (BENCH-2) is chosen over impressiveness — and only the durable mode exists to benchmark (D17).
- **R6 — The showcase sleeps.** On the free tier the instance sleeps when idle; a visitor's first load waits ~1 minute behind a loading state. Accepted consequence of U8.

## 7. Gate decisions (resolved at lock 2026-07-24 — Sri's own choices, user-stated)

- **A1 — Public name: "mini-kafka" stays.** Sri chose to keep it over the recommended original name; the trademark note (D12) was presented and acknowledged.
- **A2 — License: MIT.** (D19 resolved.)
- **A3 — Benchmark modes: durable only.** (D17 confirmed — one ack semantic, one proof.)
- **A4 — Showcase: kept conditional** against the recorded platform reality (sleeps when idle, ~1-minute wake, ephemeral disk); final feasibility is re-checked at DESIGN, and R4 stands.

All other ledger rows (D1–D19) were accepted by silence at this lock.
