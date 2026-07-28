# SL1 Implementation Plan

**Derives from: slices/SL1/DESIGN.md FINALIZED 2026-07-28.** Any design change patches this plan in the same change. Contracts live in the slice design (D-SL1-*) and project DESIGN (DD-*); this plan owns where code lives and build order. Scale: ~2 production edits (partition.go degrade path, broker test-constructor), 1 new tiny package, ~6 new/extended test files, 3 receipts.

## Codebase map (delta only — SL0's map still holds)

```
internal/storage/storagetest/fault.go   NEW: FaultFS/FaultFile/FaultSyncer wrapping osFS (D-SL1-1/2);
                                        test-only by placement — imported ONLY from _test.go files
internal/storage/partition.go           EDIT: degrade() — D-SL1-3's scoped truncate-back (prod change #1)
internal/broker/server.go               EDIT: test constructor over injectable FS/Syncer (prod change #2, D-SL1-4)
internal/storage/recovery_test.go       EXTEND: the recovery matrix (short-write · CRC-corrupt tail ·
                                        acked-corrupt · straddle · frontier CRC-bad/beyond-length · double-boot)
internal/storage/degrade_test.go        NEW: the degrade suite (write/short-write/sync/frontier faults,
                                        truncate-back + its failure cases, restart-heals)
internal/storage/store_fault_test.go    NEW: topic-create mid-fault abort (D-SL1-1 class a)
internal/broker/writefailed_test.go     NEW: wire-level code 11 + broker-still-serves (PROT-3 row)
internal/e2e/crash_test.go              NEW: the kill -9 harness (D-SL1-5/6)
docs/receipts/sl1-red-green.txt         machine-written red→green record per new test file
docs/receipts/sl1-kill9-run.txt         one harness run's output
docs/receipts/sl1-scenario-j.txt        the live disk-full transcript (D-SL1-7)
```

**Where do I look for X?** fault scripting → storagetest/fault.go · why truncate-back is scoped → slices/SL1/DESIGN.md D-SL1-3 · staged-corruption helpers → recovery_test.go · why a partition rejects writes → partition.go degrade path · harness mechanics → e2e/crash_test.go.

**Orchestration rule (adds one line to SL0's):** `storagetest` may be imported only by `_test.go` files — an import from production code is a review-visible defect. Everything else unchanged: only `internal/broker` coordinates wire↔storage.

## Entry points (delta)

- `go test ./...` now also builds `cmd/minikafka` once inside `internal/e2e` and runs the kill cycles (≤20 s budget).
- Scenario J is manual + receipted, not a test: commands inline in the receipt header (D-SL1-7).

## The trace that matters this slice (degrade path, function by function)

`flusher` batch → `flush`: `file.Write` or `syncer.Sync` FAILS → `markFailed` (sticky, under mu) → `truncateBack()`: `fsys.Truncate(log, frontier)` + fresh-handle `Sync` (best-effort; frontier value is provably still on disk on these branches) → `failAll` → each waiter's `Append` returns `ErrWriteRejected` → `handlers.handleProduce` maps it → `wire.Errf(CodeWriteFailed=11)` → client surfaces the typed code. Frontier-`WriteFileAtomic` failure takes the same path MINUS truncateBack (D-SL1-3). Reads: untouched throughout — `readLocked` is frontier-capped and never sees the torn range.

## Build order (each row done when DEMONSTRATED)

| # | Builds | Done when |
|---|--------|-----------|
| 1 | `storagetest` package + its own contract self-test | FaultFS post-state test green: scripted `WriteFileAtomic` failure leaves target parseable as old OR new, never torn; short-write leaves file really short |
| 2 | Recovery matrix tests (staged bytes, no fakes needed) | all matrix rows green; each refusal asserts the partition path in the message; red witnessed per file (receipt) |
| 3 | `degrade()` in partition.go + degrade suite | truncate-back tests red against SL0 baseline (no truncate exists) → green after the edit; frontier-write case asserts NO truncate; repair-fails cases safe; restart-heals green |
| 4 | Broker test-constructor + wire code-11 test | writefailed_test green: produce → code 11, then a fetch on the SAME broker serves |
| 5 | Topic-create mid-fault abort test | store_fault_test green: no half-topic listed, dir absent or removed on next boot |
| 6 | kill -9 harness | 3 kill/restart cycles green locally; run output saved as the receipt |
| 7 | Receipts + scenario J live + sabotage (D-SL1-8) + code map + BRIEF + STATUS | exit checklist items 1–9 all receipted |

Steps 2, 3 (test-side), 4, 5 could parallelize after 1; the harness (6) is independent of all of them.

## Modification recipes (what later slices touch)

- **SL2:** the harness grows commit journaling + "group positions intact" assertions (scenario C's second half) — additive to crash_test.go; `storagetest` faults aim at `data/_groups/*.json` writes with zero new fake machinery.
- **SL4:** nothing here — caps polish never touches storage or the fakes.

## Pitfalls (named so they can be checked)

- **FaultFS must wrap the Files it returns** (OpenAppend → FaultFile) or `File.Write`/`File.Sync` scripts silently never fire and every fault test is vacuously green. The step-1 self-test exists to catch exactly this.
- The broker maps ANY storage error on produce to code 11 (handlers.go catch-all) — the wire test must script a FLUSH-path fault; tripping a validation error would pass for the wrong reason.
- Harness process hygiene: parse the listening line from **stderr**; after SIGKILL, `Wait()` to reap before restarting; re-parse the port EVERY cycle (`:0` re-binds); same data dir across cycles is the whole point.
- Journal an ack ONLY after `Produce` returns its offset in the producer goroutine — journal-at-send fabricates acks and the one-directional assertion (D-SL1-5) stops being sound.
- Fetched ⊇ journaled, never equality — kept-but-hidden records surface after the NEXT cycle's flush; asserting equality is the flake the review killed.
- A failed (write-rejecting) partition still owns a live flusher and open file — every degrade test must `Close()` it or `-race` runs leak goroutines into later tests.
- Path-based `Truncate` while the O_APPEND handle stays open is safe ONLY because the flusher is the sole writer and it is the goroutine doing the repair — don't "fix" it to close/reopen, and don't call it anywhere but the flusher.
- `storagetest` is a real package: it needs the doc header, and vet/staticcheck/stdlib-audit all run on it — `syscall` is stdlib, fine.
- Scenario J: create the topic and baseline records BEFORE the filler (D-SL1-7) or the demo dies on the wrong error; record the image size in the receipt.
