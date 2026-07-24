# SPEC — mini-kafka (working name; see ledger D12)

**Status: DRAFT v0.1 (2026-07-24). Not locked. Nothing downstream may build on this.**

## 0. Verbatim intent

> "idea: mini-kafka with basic features"

Given by Sri 2026-07-24, immediately after concluding the url-shortener testbed. Intent grilled the same day (8 questions); grill answers are ledger rows U1–U8 and are the user's own choices, not agent assumptions.

## 1. Goals (user-stated)

- **G1 — Portfolio piece.** A public repo whose job is to demonstrate systems-engineering skill to readers. (U1)
- **G2 — Success bar = two things only:** (a) a visitor can clone and see messages flow through the broker within 60 seconds; (b) the repo carries honest, reproducible benchmarks. Sri explicitly did NOT make an architecture writeup or a showcase test suite success criteria. (U2)
- **G3 — Local-first, $0-hard hosting.** Everything runs on the visitor's machine. A hosted watch-only showcase ships ONLY if a genuinely free tier carries it; otherwise the project is local-only and hosted is a documented "later". (U3, U4, U8)

## 2. Requirements

Every requirement carries the check that would prove it. "Broker" = the single server process; "client" = the shipped Go library and CLI tools.

### Log & topics

- **LOG-1 (durability).** A message acknowledged to a producer survives broker restart, including abrupt kill (`kill -9`). *Check: produce N with acks, kill -9 broker, restart, consume all N intact.*
- **LOG-2 (offsets).** Every message in a partition gets a broker-assigned offset, contiguous and monotonically increasing from 0, stable across restarts. *Check: after restart, offsets read back 0..N-1 with no gaps or renumbering.*
- **LOG-3 (order).** A consumer reading one partition receives messages in offset order, every time. *Check: sequence-tagged payload test.*
- **LOG-4 (torn-tail recovery).** A crash mid-append never yields a corrupt or partial message to any consumer: on restart the broker detects and truncates a torn tail before serving. Unacknowledged data may be lost; acknowledged data may not. *Check: fault-injection test that truncates/corrupts the log tail mid-record, restart, verify all acked messages intact and no partial record served.*
- **TOP-1 (topics).** Topics are created explicitly by name with a fixed partition count; creating an existing topic is a clear error; topics are listable. No auto-create (D3). *Check: create/list/duplicate-create CLI test.*
- **TOP-2 (fixed partitions).** Partition count is set at topic creation and never changes. *Check: no resize API exists; attempting it is a protocol error.*

### Produce

- **PROD-1 (produce).** A producer sends an opaque-bytes message to a chosen (topic, partition) — chosen explicitly or round-robin by the client; the broker never routes by key (U6). The ack returns the assigned offset. *Check: roundtrip test asserts returned offset matches consumed offset.*
- **PROD-2 (ack = durable).** An ack is sent only after the message is durably on disk (fsync policy is a design decision; the guarantee is not). *Check: same as LOG-1 — the kill -9 test is the proof.*
- **PROD-3 (size cap).** Messages over the size cap (number in design; default order 1 MiB) are rejected with a clear error naming the cap; nothing is written. *Check: oversized produce → error naming limit; topic byte-count unchanged.*

### Consume & groups

- **CONS-1 (pull fetch).** A consumer fetches messages from (topic, partition, offset) — pull-based, batch-sized (D2). Fetching past the end returns empty/valid, not an error. *Check: fetch-at-tail and fetch-from-zero tests.*
- **CONS-2 (committed offsets).** A consumer (within a group, GRP-1) can commit its position to the broker and, after restarting, resumes from the last committed offset. *Check: consume half, commit, restart consumer, verify resumption point.*
- **GRP-1 (group assignment).** Consumers joining the same group share a topic's partitions: each partition is assigned to exactly one live member at a time. *Check: 2 consumers, 4 partitions → assignment is a partition of the partition set; no overlap, none unowned.*
- **GRP-2 (rebalance).** When a member joins, leaves, or dies (misses its liveness deadline — numbers in design), the broker reassigns partitions; no partition stays unowned longer than the stated bound. *Check: kill one of two group members mid-stream; the survivor takes over its partitions within the bound.*
- **GRP-3 (at-least-once).** Across a rebalance, every message from the last committed offset onward is redelivered to the new owner. Duplicates are permitted; loss is not (U6: exactly-once is out). *Check: crash a consumer mid-batch before commit; union of processed messages across the group = all produced messages.*
- **GRP-4 (fan-out).** Independent groups have independent committed offsets: two groups on one topic each receive the full stream. *Check: two groups both consume all N.*

### Protocol & clients

- **PROT-1 (own protocol, documented).** The broker speaks its own network protocol (design decides framing/transport), versioned and documented in the repo well enough that a reader could write a client from the doc alone. Real Kafka clients are explicitly NOT supported (U6). *Check: doc exists; a protocol-only conformance test exercises every message type against the doc.*
- **PROT-2 (shipped clients).** The repo ships a Go client library plus CLI producer/consumer/admin tools built on it — the same client code the demo and benchmarks use (no privileged path). *Check: demo and bench import the same client package; CLI smoke test.*

### Demo (success bar, G2a)

- **DEMO-1 (60-second demo).** From a fresh clone on a machine with only the Go toolchain installed (D8): one command starts the broker plus demo producer(s)/consumer(s) and messages visibly flow, within 60 seconds. *Check: timed clean-machine (or clean-checkout) script run; the receipt records the wall-clock.*
- **DEMO-2 (rebalance visible).** The demo demonstrates the headline feature: it visibly shows a consumer group rebalancing (e.g., a member is killed and the survivor takes over, narrated in output). *Check: demo transcript shows the takeover.*

### Benchmarks (success bar, G2b)

- **BENCH-1 (harness).** One command runs a reproducible benchmark measuring at minimum: produce throughput (msgs/s and MB/s) and end-to-end latency (p50/p99), against the durability settings the demo uses. *Check: `make bench` (or equivalent) emits a machine-written report.*
- **BENCH-2 (honesty).** Every published number is labeled with commit, hardware, OS, message size, and durability mode; if multiple modes are benchmarked, each is labeled — no unlabeled best-case numbers. *Check: report format includes all fields; README numbers are pasted from a committed report, not typed.*
- **BENCH-3 (no hand-edited numbers).** Benchmark numbers in the README are regenerated by command and traceable to a committed report file. A number that appears nowhere in a report is a defect. *Check: grep README numbers against the committed report.*

### Showcase (conditional — U3/U4/U8; every SHOW requirement is void if no free tier fits, and that outcome is documented instead)

- **SHOW-1 (watch-only).** A public web page visualizes live message flow of a self-driving broker instance (the instance feeds itself demo traffic). Visitors watch; no write path of any kind is exposed to them. *Check: the deployed surface serves only the read-only page/stream; no produce endpoint is reachable.*
- **SHOW-2 ($0 enforced).** The showcase runs only on a genuinely free tier with no payment method attached, or with a hard $0 cap. If the free tier ends or is exceeded: it comes down rather than costing money. *Check: account has no billable payment path; documented teardown criterion.*
- **SHOW-3 (broker port not public).** The broker's own network protocol port is never internet-reachable in the showcase deployment — only the web page/stream is. Otherwise anyone could write to it (bypassing SHOW-1). *Check: external port scan of the deployment shows only the web surface.*

### Ops & repo

- **OPS-1 (single-binary build).** `go build` produces the broker and CLI tools as static binaries; works on macOS and Linux. *Check: CI builds both platforms.*
- **OPS-2 (CI).** A public GitHub repo with free-tier CI: tests, vet/lint, and both-platform builds green on every push to main. *Check: CI badge/status on HEAD.*
- **OPS-3 (repo is the deliverable).** The public repo with a README that gets a visitor to the demo in one screen is itself a requirement — this is a portfolio piece (G1). *Check: README top screen contains the one demo command.*

### Non-functional

- **NFR-1 (stdlib-only broker).** The broker and client library use only the Go standard library — zero third-party runtime dependencies. This is a stated portfolio claim (D5). Dev/CI tooling is exempt. *Check: `go.mod` for broker/client modules has no external requires (command-audited in CI).*
- **NFR-2 (bounded resources).** Every unbounded input has a cap: message size (PROD-3), fetch batch size/bytes, concurrent connections, topic and partition counts. Numbers in design; caps exist here. *Check: each cap has a rejection test.*
- **NFR-3 (readable as a portfolio artifact).** Every package has a doc header; exported API is documented; `go vet` and the chosen linter are clean in CI. *Check: lint gate in CI.*

## 3. Scenarios (acceptance; every one owned by exactly one slice later)

- **A — First contact (the visitor's scenario).** Visitor clones the repo, runs the one command from the README top screen, and within 60 seconds watches messages flow producer → broker → consumers, including a narrated rebalance. (DEMO-1, DEMO-2)
- **B — Restart durability.** Produce N acked messages, stop the broker normally, restart: all N consumable, same offsets. (LOG-1, LOG-2)
- **C — Crash durability.** Produce with acks, `kill -9` the broker mid-load, restart: every acked message intact, torn tail truncated, no partial message ever served. (LOG-1, LOG-4, PROD-2)
- **D — Consumer resumes.** A committing consumer restarts and continues from its committed offset without loss (dupes allowed). (CONS-2, GRP-3)
- **E — Rebalance under failure.** Two group members split four partitions; one is killed; within the bound the survivor owns all four and the full stream is still delivered at-least-once. (GRP-1, GRP-2, GRP-3)
- **F — Fan-out.** A second group starts from offset 0 and independently receives the full stream already consumed by the first. (GRP-4)
- **G — Benchmark run.** One command produces a labeled machine-written report; README numbers match a committed report. (BENCH-1..3)
- **H — Hostile inputs.** Oversized message, unknown topic, bad partition index, malformed frame: each gets a clear protocol error; broker stays up; nothing is written. (PROD-3, NFR-2, PROT-1)
- **I — Showcase visit (conditional).** A visitor opens the public page and watches live traffic; port scan shows no writable surface. (SHOW-1..3)

## 4. Out of scope (user-stated, U6 unless noted)

- Log retention & compaction — the disk grows; accepted at demo scale (risk R1).
- Keyed partition routing (same-key-same-partition ordering).
- Exactly-once delivery / transactions — at-least-once is the contract.
- Kafka wire-protocol compatibility — real Kafka clients will not connect.
- Replication / multi-node / failover (U5 tier choice).
- Any visitor-writable hosted surface (U4).
- Auth/TLS on the broker protocol — it is a local-machine demo, never to be internet-exposed (SHOW-3 enforces the one hosted case; risk R3).
- Architecture writeup and test-suite-as-showcase as success criteria (U2) — docs and tests still exist; they are not what success is measured by.

## 5. Decision ledger

### User-stated (from the 2026-07-24 grill — Sri's own words/choices)

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
| D1 | Own minimal protocol over TCP, length-prefixed binary framing (final framing is design's call), documented in `docs/PROTOCOL.md`. | Kafka-ish realism with zero deps; a documented protocol is itself portfolio material. | If design finds binary framing disproportionate, HTTP/JSON would be simpler but reads as less systems-y. |
| D2 | Pull-based consumption (consumer fetches), like Kafka — not broker push. | Simpler backpressure; matches the thing being miniatured. | Push would change the whole consumer/group design. |
| D3 | Explicit topic creation; no auto-create-on-produce. | Predictable demo, clear errors, less magic. | Auto-create is friendlier for ad-hoc poking; one extra CLI step without it. |
| D4 | At-least-once with explicit offset commit (client decides when); CLI tools auto-commit per batch for demo ergonomics. | Matches U6 (no exactly-once); explicit commit makes GRP-3 provable. | Auto-commit-only would be simpler but hides the interesting semantics. |
| D5 | Stdlib-only broker & client (NFR-1) as a stated portfolio claim. | "Zero dependencies" is a strong, checkable signal; Go stdlib genuinely suffices. | If a real need appears (e.g., a metrics lib for showcase), the claim narrows to "broker core" — ledgered, not silent. |
| D6 | Message payloads are opaque bytes; broker never inspects content. | Simplicity; content-awareness is scope creep. | None foreseen at this tier. |
| D7 | Group liveness via client heartbeats with a session timeout (numbers in design). | The standard mechanism; needed for GRP-2's bound. | Design may choose lease-based instead; the GRP-2 guarantee is what's fixed. |
| D8 | Demo assumes the visitor has a Go toolchain (version floor in design); prebuilt release binaries are a bonus, not the demo path. | Target audience is engineers; `go build` of a stdlib-only repo is seconds. | If Sri wants non-Go visitors covered, releases become required and DEMO-1 gains a second path. |
| D9 | Single broker process, single node — concurrency inside, no clustering (restates U5/U6 as an architecture constraint). | Tier choice. | None; replication is explicitly out. |
| D10 | Benchmarks: a committed reference report from Sri's stated hardware + the harness so any visitor reproduces locally (BENCH-1..3). | Honest and cheap; no rented benchmark rigs. | If numbers must impress on server hardware, that's new scope + cost. |
| D11 | GitHub is the host; repo public from early on (it is the deliverable, OPS-3). If a new GitHub account is created it registers under sr7544068@gmail.com per standing rule; if Sri has an existing account they want it under, that's their call at the gate. | The portfolio needs a public home. | Publishing later instead is fine; flips OPS-2/OPS-3 timing. |
| D12 | Working name "mini-kafka". Flag: "Kafka" is an Apache trademark; naming a public portfolio repo `mini-kafka` is common practice but the safer form is an original name + "a Kafka-style log/broker" in the description. Sri decides the public name at the gate. | Naming a public artifact is the user's decision. | Rename later is cheap pre-publication, annoying after. |

## 6. Accepted risks

- **R1 — Disk grows without bound** (retention out, U6). Fine at demo scale; documented in README so a reader doesn't mistake it for an oversight.
- **R2 — Duplicates are possible** (at-least-once, U6). Stated in docs; consumers of a demo topic tolerate it.
- **R3 — The broker protocol is unauthenticated** (out of scope §4). Safe because it is local-only; the single hosted case never exposes the port (SHOW-3). README carries a "do not expose this port to the internet" warning.
- **R4 — The showcase may never ship** (U8: free tier or nothing). Accepted by design; the project is complete without it (U3).
- **R5 — Benchmark numbers are modest.** A single-node fsync-honest Go broker will not post Kafka-headline numbers; honesty (BENCH-2) is chosen over impressiveness. Accepted under G2b's "honest" framing.
