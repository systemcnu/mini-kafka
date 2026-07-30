# SL5 Slice Design — Benchmarks

**Status: DRAFT (2026-07-30) — awaiting 2-seat independent review.**
**Derives from: SPEC v1.0.1 · DESIGN v1.0.1 · SLICES v1.0.1 (hashes in STATUS.md).** Scope authority: SLICES §2/SL5. Mechanisms: DD-22 (bench harness), DD-7 (fsync honesty), DD-19 (client architecture), referenced never restated.
**Done =** scenario G real: one command runs ≥3 labeled iterations through the shipped client and emits a machine-written report with every honesty field; the README's numbers section is rendered from a committed report by command, and a CI-run test makes a hand-edited number a build failure.

## 1. Spec contract table

| ID | Treatment | Notes |
|---|---|---|
| BENCH-1 | Real | `cmd/bench`: produce throughput (msgs/s, MB/s), produce→ack p50/p99, end-to-end p50/p99 via one co-located group consumer (one process = one clock), ≥3 iterations, per-iteration numbers + spread in the report. |
| BENCH-2 | Real | Report schema carries the FULL listed label set (§3) + DD-22's framing ("closed-loop response latency", achieved-throughput wording, GC pause totals, closed-loop caveat) + DD-7's fsync-mode/platform caveat. Hardware is a required flag — the harness refuses to run without it (a report missing its hardware label is a defect by construction). |
| BENCH-3 | Real | `-render-readme` regenerates the README section verbatim from a named committed report; a repo test re-renders and byte-compares — runs in CI's existing test job on every push. |
| PROT-2 (reuse) | Proof-by-construction | The harness imports only `client` (+ `internal/broker` to host, same as `cmd/demo`'s precedent) — no privileged path. |
| Scenario G | **Owned here** | Exit demo: the real run with default parameters, receipted; the committed reference report is that run's output. |

## 2. Slice-local decisions

| # | Decision |
|---|---|
| D-SL5-1 | **Harness topology & measurement (DD-22 realized).** One process: broker in-process on `127.0.0.1:0`, data dir `os.MkdirTemp`, topic `bench` × 4 partitions. Closed loop: C=8 `client.Producer`s, one goroutine each, producer *i* → partition *i* mod 4, 1 KiB payloads, sync produce (in-flight = 1 per conn by construction — that IS the closed loop). Produce→ack latency measured around each Produce call. End-to-end: ONE `client.GroupConsumer` polling throughout; each payload embeds its produce wall-clock (`time.Now().UnixNano()` at send); e2e latency = receipt time − embedded time — same process, so no clock skew (wall-clock steps during a 10 s window are accepted noise, stated in the report's method note). Iteration structure: 2 s warm-up (measured but discarded, stated) then ≥3 × 10 s measured iterations, all in one broker lifetime (steady state, no restarts). Percentiles: nearest-rank on the full sorted per-iteration sample — named in the report ("percentile_method"). |
| D-SL5-2 | **The report is the single source of truth; the schema is the BENCH-2 checklist.** `benchmarks/reports/<utc-date>-<commit>.json`, machine-written, one struct: title ("closed-loop response latency"), commit, timestamp, hardware (flag), os/arch, go version, GOMAXPROCS, storage note (flag, default "local SSD (unverified)"), fsync mode ("fsync" + DD-7 caveat text), load model ("closed-loop, C=8 sync producers, in-flight 1/conn"), message size, partitions, warm-up, per-iteration rows (duration, msgs acked, msgs/s, MB/s, ack p50/p99, e2e p50/p99, e2e samples, GC pause total Δ, GC count Δ, produce errors, duplicates), cross-iteration spread (min/max/mean of msgs/s and both p99s), caveats block (closed-loop understates queueing tails · fsync platform limit · no capacity claims). Duplicates counted by embedded sequence IDs at the consumer (redial re-delivery is at-least-once and must be counted, not hidden); errors = failed Produce calls. |
| D-SL5-3 | **Flags.** `-hardware "<string>"` REQUIRED (refuse to run without it — every report is labeled at birth). `-storage "<string>"` optional with the honest default above. Test seams like the demo's `-flow` precedent: `-iters` (default 3), `-duration` (10 s), `-warmup` (2 s), `-c` (8) — visitors and the reference run take defaults; the smoke test shortens them. `-out <dir>` (default `benchmarks/reports`). `-render-readme <report.json>` renders and rewrites the README section instead of running a benchmark. |
| D-SL5-4 | **README section + the BENCH-3 gate.** The README gains a marker-delimited section (`<!-- bench:begin -->` / `<!-- bench:end -->`) containing: the headline table (throughput, ack p50/p99, e2e p50/p99, spread), the label block (hardware, OS, Go, commit, fsync mode, load model), the closed-loop + fsync caveats, and the source report's path — ALL rendered verbatim from the named report by `-render-readme`; rendering is a pure function of the report file. The gate: `cmd/bench/render_test.go` parses the README markers, extracts the named report path, re-renders from that committed report, and byte-compares — a hand-edited number, a stale section after a new reference report, or a README pointing at a missing report all go red in CI's existing test job. |
| D-SL5-5 | **What the bench does NOT do or claim.** No open-loop mode, no capacity/"max throughput" claims anywhere (R5 accepted: numbers are modest and honest) · CI never RUNS the benchmark (shared-runner numbers would launder dishonest hardware labels into the repo; CI only verifies README↔report consistency) · no comparisons to other systems · the group consumer's commit cadence is not benchmarked (commits happen at natural Poll cadence; e2e latency is the measured claim). |
| D-SL5-6 | **Reference run = scenario G = the committed artifacts.** The integrator (not the builder) runs `go run ./cmd/bench -hardware "<real machine string>"` with defaults on Sri's actual machine, commits the emitted report as the reference, runs `-render-readme` against it, and captures the transcript as `docs/receipts/sl5-scenario-g.txt`. The README section names that exact report file — BENCH-3's traceability is a file path, not a promise. |
| D-SL5-7 | **What SL5 touches.** NEW: `cmd/bench` (+ its tests), `benchmarks/reports/` (one committed reference report), the README bench section, `docs/receipts/sl5-*`. NO broker, client, wire, storage, group, or CI-workflow changes (the BENCH-3 test rides the existing test job). |

## 3. Known gaps accepted

- **G-SL5-1** — hardware and storage labels are operator-stated flags, not detected: honest self-description is the design (BENCH-2 wants the label present and truthful; detection can't verify "SSD" reliably anyway). The default storage string says "(unverified)" so a lazy run is labeled as such.
- **G-SL5-2** — wall-clock e2e: a clock step mid-iteration lands in the numbers; same-process measurement makes this rare and small, and the method note states it. Monotonic-clock plumbing through payloads is not worth the complexity for a 10 s window.
- **G-SL5-3** — the consumer measures e2e only for records it receives during the window; if it lags the producers, latency inflates HONESTLY (the lag is real) and the per-iteration e2e sample count exposes it.
- **G-SL5-4** — the committed reference report goes stale by design between refreshes (same convention as the demo-timing receipts): it names its commit; the BENCH-3 test pins README↔report consistency, not report↔HEAD freshness.

## 4. Test plan mapped to claims

- **Smoke test** (`cmd/bench/bench_test.go`, black-box like the demo's): run the built binary with short seams (`-iters 2 -duration 1s -warmup 200ms -c 2`) and a required `-hardware` string into a temp `-out`; assert: exit 0 · a report file exists · it unmarshals · EVERY BENCH-2 schema field non-zero/present (the checklist as assertions) · iterations == 2 with sane values (msgs acked > 0, p50 ≤ p99, e2e samples > 0) · spread block present · caveats block carries the closed-loop and fsync texts · refusing to run without `-hardware` (distinct error, non-zero exit).
- **Render test** (`render_test.go`): golden-path — render a fixture report, assert the section contains its numbers and the report path; the CI gate — parse the REAL README's markers, load the named committed report, re-render, byte-compare (this is the test that makes hand-edits fail CI).
- **Determinism**: rendering the same report twice is byte-identical (pure function).
- **Live legs (exit, integrator):** scenario G reference run with defaults on stated hardware → committed report + README render + transcript receipt · exit sabotage: hand-edit one README number → render test red → restore via `-render-readme` → green.

## 5. Validate — exit checklist (all demonstrated, not asserted)

1. Red-before-green for the smoke and render tests (against a bench that doesn't yet emit the field/section under test).
2. Full suite + `scripts/checks.sh` race-green by command (bench code passes staticcheck/vet/gofmt like everything else).
3. Scenario G: the reference run's transcript → `docs/receipts/sl5-scenario-g.txt`; report committed under `benchmarks/reports/`; README section rendered from it by command.
4. Exit sabotage (integrator's hands): one README number hand-edited → the BENCH-3 test goes red → restored by re-render → green.
5. Push → CI green (five jobs; the new tests ride the test job).
6. Code map + BRIEF (baked diagram) + STATUS/LAB-STATE + commits.
7. Gate notes for Sri: none anticipated beyond the reference-run hardware string being Sri's real machine description (integrator-stated, per D-SL5-6).
