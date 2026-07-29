# SL3 Implementation Plan

**Derives from: slices/SL3/DESIGN.md FINALIZED 2026-07-29.** Any design change patches this plan in the same change. Contracts live in D-SL3-* and DD-20/21; this plan owns where code lives and build order. Scale: 1 new cmd (~250 lines), 2 client additions (~40 lines), 1 shell harness, 1 CI job, README surgery, 3 test files/receipts.

## Codebase map (delta only)

```
cmd/demo/main.go            NEW: two-act narrated demo (D-SL3-1/3) — broker in-process,
                            topic demo×4, 1 producer + 2 GroupConsumers over real TCP,
                            #event markers, SIGTERM cleanup, -ci and -flow flags
cmd/demo/demo_test.go       NEW: black-box smoke test (built binary, default + -ci,
                            TMPDIR-owned, ordered-marker assertions) (§4)
client/client.go            EDIT: GroupConsumer.Abandon() (pinned sequence, shared
                            closeOnce) + Assignment() []uint32 (D-SL3-2)
client/group_test.go        EXTEND: Abandon unit (non-nil Commit error + survivor owns all)
scripts/demo_timing.sh      NEW: the external clock (D-SL3-4 — GOTOOLCHAIN=local preflight,
                            temp caches, timeout -k 200, direct-pipe timestamper, gates,
                            marker assertions, receipt block)
scripts/demo_timing_test.sh NEW: fake-demo gate tests (pass-in-window / late-red / hang-red)
.github/workflows/ci.yml    EDIT: demo-timing job, container pinned BY DIGEST (D-SL3-5)
README.md                   EDIT: top screen + truth corrections (D-SL3-7)
docs/receipts/              sl3-scenario-a.txt · demo-timing-macos.txt · demo-timing.txt (from CI)
```

**Where do I look for X?** demo narration rules → cmd/demo/main.go top comment (cites D-SL3-3) · why Abandon is lock-free → client.go Abandon comment (D-SL3-2) · gate mechanics → scripts/demo_timing.sh header · why the container is digest-pinned → ci.yml comment (D-SL3-5).

**Orchestration rule:** `cmd/demo` imports `internal/broker` + `client` (same as `cmd/minikafka` precedent) — never storage/group/wire directly.

## Build order (each row done when DEMONSTRATED)

| # | Builds | Done when |
|---|--------|-----------|
| 1 | client: Assignment() + Abandon() (+ red-first unit tests) | Abandon test green: Commit errors non-nil, survivor owns all 4, no goroutine leak under -race; Assignment snapshot correct across a rebalance |
| 2 | cmd/demo (both acts, deterministic startup order, markers, flags, SIGTERM) | smoke test green: ordered markers both modes, byte-identical narration, TMPDIR empty after |
| 3 | scripts/demo_timing.sh + its fake-demo tests | all four gate cases proven (in-window pass with first-flow ∈ [2,4) · late-first-flow red · late-total red · hang red via timeout) |
| 4 | ci.yml demo-timing job (digest-pinned container) | job YAML validates; local run of the script against the real demo passes gates + marker assertions |
| 5 | README top screen + truth corrections | D-SL3-7's list all applied; no remaining "walking skeleton"/"no consumer groups" text (grep) |
| 6 | receipts prep | local `bash scripts/demo_timing.sh` receipt saved (dev-machine, labeled as such); checks.sh fully green |

Steps 1→2 strictly ordered; 3 parallel with 2; 4-6 after.

## Pitfalls (named so they can be checked)

- **Abandon takes NO mutex** — a concurrent net.Conn.Close is the unblock mechanism for the parked Poll; grabbing fetchMu first deadlocks the kill behind a 7 s park (the F3 catch). The heartbeat goroutine must exit via hbStop AFTER the conns close, all inside the shared closeOnce.
- The demo must wait for the SETTLED 2-member assignment before starting the producer (D-SL3-3) or the transcript is nondeterministic and the smoke test flakes — wait on both consumers' Assignment() lengths being 2, not on sleeps.
- `#event` lines: own line, exact text `#event first-flow` / `#event done`, written to os.Stdout directly (Go's File writes flush per-write; do NOT wrap stdout in bufio).
- The takeover assertion anchors AFTER the act-two header — the act-one transient (consumer-1 briefly owns all 4 before consumer-2 joins) will satisfy a naive owns-all matcher (F4).
- demo_timing.sh: the `while read` loop IS the timestamper — no awk/sed between the demo and it (F9); `date +%s` per line; TZ-independent arithmetic.
- `timeout -k` flags differ between GNU coreutils (CI container) and macOS: macOS has no `timeout` by default — the script must degrade honestly (macOS leg: run without timeout, note it in the receipt) or use a portable Go/perl fallback; pick ONE and receipt it.
- GOTOOLCHAIN=local + version preflight BEFORE t0 (F6); fresh GOCACHE/GOMODCACHE per run; NEVER touch the caller's real caches.
- ci.yml: get the golang:1.24 digest by `docker manifest inspect` (or from Docker Hub) at authoring time; pin `container: golang:1.24@sha256:…`; echo the pinned ref into the receipt (F10).
- README: keep the limitations section (truthful ones), fix only the false lines (D-SL3-7's list); the SL5/SL6 passes own the rest.
- The smoke test builds the binary once (sync.Once or TestMain), runs it twice; runtime budget ≤ 30 s with the -flow override at ~2 s.
