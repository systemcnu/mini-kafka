# SL5 Implementation Plan

**Derives from: slices/SL5/DESIGN.md FINALIZED 2026-07-30.** Any design change patches this plan in the same change. Contracts live in D-SL5-1..8 and DD-22/DD-7; this plan owns where code lives and build order. Scale: 1 new cmd (~350 lines across main/report/render), 2 test files, README markers, 1 committed fixture; NO production-package changes.

## Codebase map (delta only)

```
cmd/bench/main.go         NEW: flag parsing (D-SL5-3), VCS provenance gate (D-SL5-8),
                          broker+topic setup, producer/consumer orchestration,
                          iteration clock, report assembly + write (D-SL5-1/2)
cmd/bench/report.go       NEW: the Report struct (THE BENCH-2 checklist, D-SL5-2),
                          percentile + spread math, JSON writing
cmd/bench/render.go       NEW: -render-readme — pure report→section function +
                          marker splice into README (D-SL5-4)
cmd/bench/bench_test.go   NEW: black-box smoke (built binary, short seams; two-class
                          field assertions; refusal paths) (§4)
cmd/bench/render_test.go  NEW: fixture render leg + determinism + the real-README
                          bootstrap-ruled gate leg (D-SL5-4)
cmd/bench/testdata/       NEW: fixture report for the render leg
README.md                 EDIT (builder): nothing — markers land ONLY with the
                          integrator's reference render (bootstrap rule); builder
                          touches no README line
benchmarks/reports/       integrator's reference report (exit); builder commits NONE
docs/receipts/            sl5-red-green.txt (builder) · sl5-scenario-g.txt (integrator)
```

**Where do I look for X?** why the bench refuses without VCS info → main.go provenance comment (D-SL5-8) · what every report field means → report.go struct doc comments (one line each, BENCH-2 names) · why duplicates are excluded from samples → the dedupe comment (D-SL5-2/F7) · the gate's bootstrap rule → render_test.go top comment (D-SL5-4).

**Orchestration rule:** `cmd/bench` imports `internal/broker` + `client` only (demo precedent). The renderer is a pure function `render(Report) string` — no I/O — so determinism is testable without touching disk.

## Build order (each row done when DEMONSTRATED)

| # | Builds | Done when |
|---|--------|-----------|
| 1 | SKELETON commit (§5.1): flags + Report struct with all fields + stub run emitting an empty-but-well-formed report + render stub emitting markers only | smoke + render tests written and RED against it (fields empty/absent → both classes of assertion fail; refusal paths fail; fixture leg fails); reds receipted per file into sl5-red-green.txt |
| 2 | provenance gate (D-SL5-8) + `-hardware` refusal | both refusal subtests green: no `-hardware` → distinct error; no VCS revision → error naming `-buildvcs=true`; dirty tree stamps `-dirty` |
| 3 | the harness (D-SL5-1): producers, consumer, iteration clock, dedupe, commit-per-iteration, percentiles, spread, GC deltas | smoke green: all must-be-positive fields real, raw-JSON presence for zero-class, errors==0 && duplicates==0, p50 ≤ p99, iterations == 2 |
| 4 | report writing (D-SL5-2): filename `<utc-date>-<commit>.json`, caveats block | smoke asserts filename shape + every caveat text present |
| 5 | renderer (D-SL5-4): section content incl. FULL inline label set + counts + caveats + path | fixture leg green; determinism green; real-README leg SKIPS with the named bootstrap reason (no committed report yet) |
| 6 | full suite + checks.sh | race-green by command; count reported by command |

1 strictly first; 2–5 ordered as listed (each consumes the prior); 6 last. The builder commits NO report and NO README change — the gate leg must end the builder phase in its named SKIP state (that skip line is itself receipt-worthy: capture it).

## Pitfalls (named so they can be checked)

- **Payload layout:** 1 KiB payload = 8-byte producerID + 8-byte seq + 8-byte sendUnixNano + padding to 1024. The consumer parses ONLY the first 24 bytes; padding is opaque. Message size label = 1024 (the payload the broker caps, not the frame).
- **Dedupe map growth:** ~40 s × ~10k msg/s ≈ 400k IDs — a `map[[2]uint64]struct{}` is fine (~tens of MB); do NOT clear it between iterations (a redelivery crosses boundaries by definition).
- **Iteration clock:** one goroutine owns phase transitions and publishes the index atomically (warm-up = index 0, discarded). Producers/consumer read it per sample: ack bucketed at ack-completion read, e2e at receipt read (D-SL5-1/F8). Don't reconcile totals across buckets — per-iteration rows stand alone.
- **ReadMemStats is stop-the-world:** call it ONLY at iteration boundaries (never inside the measured window); deltas between boundary snapshots.
- **Consumer loop:** Poll with a short maxWait (e.g. 250 ms) so iteration boundaries aren't held hostage by a parked fetch; Commit at the boundary, outside the sampled path (D-SL5-1/F6).
- **The producers must STOP producing before the final boundary snapshot** (closed loop: signal, then join) — an ack completing after the last boundary belongs to no bucket; drop it, don't panic.
- **Render purity:** render(Report) builds the whole section string (fixed field order, `strconv.FormatFloat(_, 'f', 1/2, 64)` — never `%v` — so byte-identity is platform-stable); the README splice is a separate tiny function (read, replace between markers or append section at a pinned anchor if markers absent — ONLY under -render-readme; tests splice into temp copies).
- **Path resolution (F9):** render_test resolves the repo root as `../../` from the package dir; the README's report path is repo-root-relative; the renderer's `-readme` default assumes cwd = repo root and says so in its flag help.
- **Gate-leg bootstrap (D-SL5-4):** skip condition = `benchmarks/reports/*.json` glob empty. NOT "markers absent" — reports-present-but-markers-gone must FAIL.
- **The smoke test builds the binary once** (TestMain, demo precedent) with `go build` (embeds VCS) — but the VCS-refusal subtest needs a binary WITHOUT vcs info: build it with `-buildvcs=false` into the temp dir for that one subtest.
- **staticcheck is in the gate** (checks.sh, pinned 2025.1.1): error-last returns, no unused symbols; gofmt clean.
- **Wall-clock only for e2e** (time.Now().UnixNano() both ends, same process); do NOT try monotonic plumbing (G-SL5-2 accepted).
