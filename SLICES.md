# SLICES — mini-kafka

**Status: DRAFT v0.1 (2026-07-28). Not locked.**
**Upstream: SPEC LOCKED v1.0 `2b002d99cf9248021ca1ca0bf7cf228ce22cecf5328e70a4827759de3de023c9` · DESIGN LOCKED v1.0 `47bef8647c97c2b810f31ab8770d59cfc7b425030462a10402f3d7cceacfc72a` — both verified at preflight.**

## 1. The cut

Eight slices, serial build order (SL7 is schedule-independent after SL2 but deliberately last — SD-7). Every slice ends in a runnable end-to-end demo; slice 0 is the walking skeleton proving the architecture. Day counts are solo-work estimates; total = **15.5 days** (command-summed).

| # | Slice | Ends with (the runnable demo) | Owns scenario | Days |
|---|-------|-------------------------------|---------------|------|
| SL0 | Walking skeleton | Create topic, produce, consume over real TCP via `mk`; stop broker normally, restart, consume again — same offsets | B | 2.5 |
| SL1 | Crash & disk hardening | Live `kill -9` mid-load, restart, all acked messages intact; disk-full demo: writes refused, reads fine | C, J | 2 |
| SL2 | Consumer groups | Two `mk consume --group` processes split 4 partitions; kill one, survivor takes over; second group re-reads from 0 | D, E, F | 3 |
| SL3 | Hostile inputs & caps | A battery of bad inputs (oversized, bad names, unknown topic, malformed frames, stale generation) each gets its named error; broker stays up | H | 1.5 |
| SL4 | The demo (two acts) | `go run ./cmd/demo` — the actual 60-second success bar, externally timed in CI, receipt committed | A | 1.5 |
| SL5 | Benchmarks | `go run ./cmd/bench` → labeled report; README numbers rendered from a committed report; CI enforces the match | G | 1.5 |
| SL6 | Repo & CI completion | Full CI battery green on the public repo: stdlib audit, PROTOCOL.md registry diff, macOS smoke, lint; LICENSE; README top screen | K | 1.5 |
| SL7 | Showcase (conditional) | Live page watched through the Pages loading shim — or, if the feasibility check fails, the documented "later" | I | 2 |

Ownership audit rule: every scenario A–K appears exactly once above (K's letter belongs to SL6; audit by command at draft and at lock).

## 2. Slice detail

### SL0 — Walking skeleton (2.5d)
**Goal:** the architecture proven end to end — client → TCP → broker → durable log → fetch back — with the real durability spine, not a stub.
**Builds:** module layout (DD-1); wire framing + Produce/Fetch(multi-entry form)/CreateTopic/ListTopics + error frames (DD-15 subset); storage: record format, CRC, frontier + atomicWrite, 5 ms flusher, read-cap-at-frontier, basic boot recovery (DD-2..6, DD-9); `Producer`/`Consumer` client + `mk` create-topic/topics/produce/consume-raw (DD-19 subset); Syncer seam + ack-ordering test (LOG-1a); CI skeleton: Linux build + tests on every push (DD-26 subset).
**Checks due:** LOG-1, LOG-2, LOG-3, PROD-1, PROD-2, CONS-1 (raw path), TOP-1, TOP-2.
**Exit demo:** scenario B live, via `mk` against a real broker process.

### SL1 — Crash & disk hardening (2d)
**Goal:** every crash and storage-failure promise proven with fault injection, not luck.
**Builds:** storage file seam with scripted short-writes/ENOSPC/corruption (§9); LOG-4's four recovery cases + the straddling-record case; frontier-unreadable/boundary rules; `kill -9` e2e harness; sticky write-reject degrade + truncate-back (DD-8); topic-create crash cleanup (DD-9); crash-walk table turned into tests.
**Checks due:** LOG-4, LOG-5.
**Exit demo:** scenario C live (`kill -9` mid-load, recover, verify) + scenario J (quota-limited dir fills, writes refused with the named error, reads keep serving).

### SL2 — Consumer groups (3d)
**Goal:** the headline feature whole: groups, immediate rebalance, fencing, durable commits.
**Builds:** coordinator (DD-10..14): control conn, heartbeats/session timeout, immediate rebalance + range assignment, generations, serve-time fencing, commit storage via atomicWrite, join-carries-state; `GroupConsumer` client with control+fetch conns (DD-19); injectable coordinator clock + GRP-2 bound test; long-poll wake on frontier advance across owned partitions.
**Checks due:** CONS-2, CONS-3, GRP-1, GRP-2, GRP-3, GRP-4, GRP-5.
**Exit demo:** scenarios D, E, F live via two `mk consume --group` processes (kill one mid-stream; watch fencing reject its late commit in the log output).

### SL3 — Hostile inputs & caps (1.5d)
**Goal:** every input cap and error contract real, now that the full message set exists (stale-generation errors need SL2 — SD-4).
**Builds:** name validation (DD-18); the complete input-cap list with per-cap rejection tests (DD-16); full error registry incl. INVALID_NAME/CAP_EXCEEDED/FETCH_TOO_WIDE (DD-17); conn cap with served error frame, idle reclaim (DD-24); malformed-frame fuzz-ish table tests.
**Checks due:** PROD-3, PROT-3, NFR-2, NFR-4 (bind-address test lands here with the listener finalization).
**Exit demo:** scenario H live — a scripted battery of hostile inputs against a running broker, each answered by its named code, broker still serving.

### SL4 — The demo (1.5d)
**Goal:** the success bar itself (G2a), honestly measured.
**Builds:** `cmd/demo` two acts with narrated output + `#event` markers, MkdirTemp data dir (DD-20); CI demo-timing job with the external clock, cold caches, receipt with resolved image + commit (DD-21); macOS clean-cache local-run procedure + receipt; README top screen carries the one command (OPS-3 partial).
**Checks due:** DEMO-1, DEMO-2.
**Exit demo:** scenario A — run it, then watch CI run it colder and commit the receipt.

### SL5 — Benchmarks (1.5d)
**Goal:** the other success bar (G2b): honest, labeled, reproducible numbers.
**Builds:** `cmd/bench` closed-loop harness, ≥3 iterations, spread, GC stats, full BENCH-2 label set, "closed-loop response latency" framing + caveat (DD-22); report files; `-render-readme`; CI test asserting README ↔ committed report; Sri's reference run on stated hardware.
**Checks due:** BENCH-1, BENCH-2, BENCH-3.
**Exit demo:** scenario G — one command to a labeled report; README numbers traceable to it.

### SL6 — Repo & CI completion (1.5d)
**Goal:** the repo IS the portfolio deliverable — finish every proof CI owes.
**Builds:** `docs/PROTOCOL.md` (registry now final) + CI registry-diff test (DD-17); `go list -deps` stdlib audit job; staticcheck (pinned) + vet gate; macOS native build+smoke job; receipts record resolved runner versions (DD-26); LICENSE (MIT, SPEC A2); README full pass (R1/R2/R3 statements, macOS fsync caveat, port warning); package doc headers audit (NFR-3).
**Checks due:** PROT-1, PROT-2 (final audit both), OPS-1, OPS-2, OPS-3, NFR-1, NFR-3.
**Exit demo:** scenario K — a push to the public repo runs the whole battery green.

### SL7 — Showcase (conditional, 2d)
**Goal:** the watch-only live page — or its honest absence.
**Builds:** **step 1 is the live feasibility check** (Render free tier still card-less, instance hours sufficient, disk allowance vs threshold — DD-23); kill criterion: any failure → write the documented "later" (R4) and the slice ends at 0.5d. If green: `cmd/showcase` (feeder, poll feed, page), GitHub Pages loading shim, `render.yaml`, deploy, scripted port scan, teardown criterion in README.
**Checks due:** SHOW-1, SHOW-2, SHOW-3, SHOW-4.
**Exit demo:** scenario I — or the committed feasibility record and "later" note.

## 3. Process per slice (from the stage-loop BUILD stage)

Each slice: design+plan under the slice skills → red-before-green build → sabotage check at exit → machine-written receipts → `slices/<SL>/BRIEF.md` (plain language, baked diagram) → code map regenerated → commit checkpoints → Sri gates the slice exit.

## 4. Decision ledger (slice-level)

| # | Decision | Why | Consequence if wrong |
|---|----------|-----|----------------------|
| SD-1 | Eight slices, serial; SL7 alone is schedule-independent (needs only SL2's client surface) but is deliberately scheduled last. | Serial keeps every slice on locked predecessors; the conditional slice shouldn't block the success bars. | If Sri wants the showcase earlier it can run any time after SL2. |
| SD-2 | The durability spine (frontier, flusher, read-cap) is in SL0, not deferred to SL1. | The skeleton must prove the REAL architecture — retrofitting durability under a fake ack path would rework SL0. | SL0 is 2.5d instead of a 1d toy. |
| SD-3 | Crash/fault hardening (SL1) immediately after SL0, before any features. | Every later slice builds on storage; hardening it early means groups/demo/bench never sit on sand. | — |
| SD-4 | Groups (SL2) before hostile-inputs (SL3), so scenario H completes in ONE slice — stale-generation errors don't exist until groups do. | Scenario ownership stays exactly-once. | H's fuzz battery waits ~3 days longer. |
| SD-5 | Requirement checks land with the slice that builds the mechanism ("Checks due" lists); scenarios are owned exactly once (§1 table). | Two audit surfaces: nothing double-owned, nothing orphaned. | — |
| SD-6 | Sizing: solo-with-agent estimates; total 15.5d command-summed; SL2 is the biggest at 3d. | Honest sizing beats optimistic sizing. | Estimates are estimates; gates catch drift. |
| SD-7 | SL7 opens with a kill-criterion feasibility check before any build effort. | A4 kept the showcase conditional; the check spends 0.5d to avoid 1.5d of maybe-wasted build. | — |
| SD-8 | PROTOCOL.md is authored in SL6, after SL2/SL3 freeze the registry; until then wire doc-comments carry the contract. | Writing the doc twice is waste; the CI diff test needs a final registry. | Early protocol readers (none expected pre-publication) would wait. |
| SD-9 | Publication timing and GitHub account are Sri's gate decisions (see §5) — the plan defaults to publish-at-SL0-exit per DESIGN D11 ("public early"). | Only Sri can act on accounts (standing rule: a new account registers under sr7544068@gmail.com). | Publishing later just delays K's full green. |

## 5. Open at this gate (Sri decides)

- **B1 — Publish when?** At SL0 exit with LICENSE + minimal README, iterating in public (recommended — matches locked DESIGN D11 "public early"; an honest commit history is itself portfolio material) — or after SL4 when the 60-second demo works.
- **B2 — Which GitHub account?** Your existing account, or a new one (which per your standing rule would register under sr7544068@gmail.com). The repo's public home is where the portfolio lives — your call, and only you can perform the publish step either way.
