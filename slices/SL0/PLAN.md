# SL0 Implementation Plan

**Derives from: slices/SL0/DESIGN.md FINALIZED 2026-07-28.** Any design change patches this plan in the same change. Contracts live in the design (D-SL0-*) and project DESIGN (DD-*); this plan owns where code lives and build order.

## Codebase map (one responsibility per file)

```
go.mod                         module mini-kafka (G1), go 1.24
cmd/minikafka/main.go          broker entry: flags, signal handling, start/stop (D-SL0-6/7)
cmd/mk/main.go                 CLI subcommand dispatch + the four subcommands (D-SL0-7)
internal/wire/frame.go         frame read/write, bounded decoder, caps (D-SL0-2)
internal/wire/messages.go      typed encode/decode per message body (D-SL0-3)
internal/wire/errors.go        code registry (values pinned, D-SL0-8) + Error helpers
internal/wire/names.go         name validation (DD-18)
internal/storage/fs.go         FS seam + osFS + WriteFileAtomic recipe (D-SL0-4)
internal/storage/partition.go  one partition: append, flusher, frontier, notify channel, read (D-SL0-5)
internal/storage/recovery.go   DD-4 boot scan, all three branches
internal/storage/store.go      topics registry, meta.json, create/list, partition lifecycle (DD-9)
internal/storage/syncer.go     Syncer interface + real impl (blocking implementations legal)
internal/broker/server.go      listener, conn cap guard, conn loop, dispatch table, drain (D-SL0-6, DD-24)
internal/broker/handlers.go    per-message handlers: validate → storage → respond
client/client.go               Producer + Consumer over one conn (DD-19 subset)
scripts/checks.sh              local CI-equivalent battery (D-SL0-9)
scripts/stdlib_audit.sh        go list -deps gate (NFR-1)
.github/workflows/ci.yml       the four CI jobs (D-SL0-9)
LICENSE · README.md            MIT · truthful top screen
```

**Where do I look for X?** wire format → wire/frame.go+messages.go · error codes → wire/errors.go · ack ordering → storage/partition.go · crash recovery → storage/recovery.go · why a produce was rejected → broker/handlers.go · shutdown → broker/server.go + cmd/minikafka.

**Orchestration rule:** only `internal/broker` coordinates wire↔storage; `storage` never imports `wire`; `client` imports `wire` only (via internal is legal in-module); `cmd/*` import broker/client, never storage directly. A violation is visible in imports.

## Entry points

- `go run ./cmd/minikafka [--addr] [--data]` — the broker, runs until SIGINT/SIGTERM → graceful stop.
- `go run ./cmd/mk <create-topic|topics|produce|consume> …` — the four client commands.
- `go test ./...` — everything; `scripts/checks.sh` — the full local battery.

## The end-to-end trace (narrate-able after one read)

`mk produce` → `client.Producer.Produce(topic, partition, payload)` → `wire.WriteFrame(conn, Produce{…})` → broker `server.connLoop` reads frame (deadline around the read only) → `handlers.handleProduce`: name/caps validation → `store.Partition(topic, p)` → `partition.Append(payload)` enqueues a waiter → flusher batch: `file.Write` → `syncer.Sync` → frontier `WriteFileAtomic` + notify-swap (under write lock) → waiter released with offset → `ProduceResp{offset}` framed back → client returns offset → `mk` prints it. Fetch mirrors it: `handleFetch` → `partition.ReadFrom(offset, maxBytes)` under read lock (frontier-capped) → if empty, capture notify chan (same lock), park on `{notify, timer, stopping, connCtx}` → wake → re-read → `FetchResp`.

## Build order (each row done when DEMONSTRATED)

| # | Builds | Done when |
|---|--------|-----------|
| 1 | go.mod, LICENSE, README stub, scripts/, ci.yml skeleton | `scripts/checks.sh` runs (vet+build green on empty packages) |
| 2 | wire: frame, errors, names (+tests red→green) | frame roundtrip + bounded-decode + INVALID_NAME table tests pass |
| 3 | wire: messages (+tests) | encode/decode roundtrip per type passes |
| 4 | storage: fs.go seam + osFS (+atomicWrite test) | WriteFileAtomic post-state test passes |
| 5 | storage: partition append/flusher/frontier/notify + syncer | ack-ordering recorder test (LOG-1a) passes — seen red first via a syncer that acks early sabotage |
| 6 | storage: recovery (3 branches) + store (meta.json, create/list) | happy-path recovery + TOP-1/TOP-2 tests pass |
| 7 | broker: server + handlers | integration: real TCP produce/fetch roundtrip test (PROD-1, LOG-2, LOG-3) passes |
| 8 | long-poll park/wake + graceful stop | wake test (AdvanceHook + gate) passes; drain test: parked fetch returns empty on SIGTERM |
| 9 | client + mk | `mk` four commands work against a live broker (scripted) |
| 10 | live-cap rejection tests + NFR-4 test + conn guard | exit-checklist item 4 fully green |
| 11 | scenario B demo + transcript receipt | docs/receipts/sl0-scenario-b.txt committed |
| 12 | sabotage check, README truth pass, checks.sh full green | exit items 1–8 all receipted |

Steps 2–4 could parallelize; everything else is dependency-ordered.

## Modification recipes (what later slices touch)

- **SL1 (faults):** new scripted `FS`/`File` fakes in storage tests + kill -9 harness under `internal/harness` or test files — zero production edits expected except LOG-5 truncate-back inside partition.go's error path.
- **SL2 (groups):** new wire types 9+ in messages.go (additive), codes 12–13 in errors.go (additive), new `internal/group` package, broker handlers additions, `client.GroupConsumer` — partition.go untouched except multi-chan wake reuse.
- **SL4 (caps polish):** served-error-frame on conn cap + idle reclaim in server.go only.

## Pitfalls (named so they can be checked)

- The flusher must assign offsets at APPEND (queue) time, not flush time, or concurrent producers' ProduceResp offsets can misorder vs LOG-2's contiguity test.
- `net.Conn` deadlines: set before each frame read, CLEAR before handler work — a deadline left armed kills parked fetches (DESIGN DD-19's lesson applied server-side).
- `WriteFileAtomic` on macOS: rename within the same dir; temp file must be created in the destination dir (Codex DESIGN finding).
- Windows is out of scope everywhere (spec: macOS/Linux) — no `runtime.GOOS` special-casing.
- The conn-cap guard must decrement on ALL exit paths (defer), or the cap wedges the broker after 256 total conns ever.
