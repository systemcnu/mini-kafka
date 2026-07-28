# SLICES — mini-kafka

**Status: LOCKED v1.0 (2026-07-28). Slice cut frozen. Changes only via errata cascade.**
Locked by Sri at the gate 2026-07-28 after the 2-question quiz (both answered correctly, no re-asks) and two gate decisions, resolved in §5.
**Upstream: SPEC LOCKED v1.0 `2b002d99cf9248021ca1ca0bf7cf228ce22cecf5328e70a4827759de3de023c9` · DESIGN LOCKED v1.0 `47bef8647c97c2b810f31ab8770d59cfc7b425030462a10402f3d7cceacfc72a` — both verified at preflight.**
v0.1 → v0.2: 33 lie-hunt findings integrated (4 seats — coverage 7, verticality 6, sizing 10, Codex cut-order 10). Restructured: broker + safety edges + seams + LICENSE/README + CI baseline all moved into SL0; scenario C re-owned by SL2; demo moved ahead of hostile-input completeness; showcase feasibility check pulled early; sizing re-estimated honestly (15.5d → 24.5d, process overhead now budgeted).

## 1. The cut

Eight slices, serial build order. Every slice ends in a runnable end-to-end demo; slice 0 is the walking skeleton proving the real architecture. **Day counts include each slice's exit process** (brief, code-map regen, sabotage check, gate prep — ~0.5d each; SD-6). Total = **24.5 days** (command-summed). The prior 15.5d was build-time-only underestimation — called out by the sizing seat and corrected, not massaged.

| # | Slice | Ends with (the runnable demo) | Owns scenario | Days |
|---|-------|-------------------------------|---------------|------|
| SL0 | Walking skeleton | Create topic, produce, consume over real TCP via `mk` against the real broker binary; stop normally, restart, consume again — same offsets. Repo published (if B1 = early): LICENSE + truthful README, CI baseline green | B | 5 |
| SL1 | Crash & disk hardening | Live `kill -9` mid-load, restart, acked messages intact; disk-full: writes refused with the named error, reads fine | J | 3 |
| SL2 | Consumer groups | Two `mk consume --group` processes split 4 partitions; one SIGSTOPped past its deadline, survivor takes over, the stalled one resumes and its late commit is visibly fenced; kill -9 broker → group positions survive; second group re-reads from 0 | C, D, E, F | 4.5 |
| SL3 | The demo (two acts) | `go run ./cmd/demo` — the 60-second success bar, externally timed in CI, receipt committed | A | 2.5 |
| SL4 | Hostile inputs & caps audit | Scripted battery: oversized message, unknown topic, **bad partition index**, bad names, malformed frames, stale generation — each answered by its named code; broker stays up; cap inventory + registry completeness audited | H | 2 |
| SL5 | Benchmarks | `go run ./cmd/bench` → labeled report; README numbers rendered from a committed report; CI enforces the match | G | 2 |
| SL6 | Protocol doc & repo completion | PROTOCOL.md a stranger could implement from + CI registry diff; full README pass; final PROT/OPS/NFR audits green on the public repo | K | 2.5 |
| SL7 | Showcase build (conditional) | Live page watched through the Pages loading shim — or the documented "later". **Its 0.5d feasibility check runs early, at SL0 exit** (SD-7) | I | 3 |

Ownership audit rule: every scenario A–K appears exactly once above (audited by command at draft and at lock).

## 2. Slice detail

### SL0 — Walking skeleton (5d)
**Goal:** the real architecture end to end — client → TCP → real broker binary → durable log → back — with the safety edges and test seams that everything later depends on. Priced as what it is: the concurrency core of the system (SD-2).
**Builds:** module layout (DD-1) · wire framing with the bounded decoder + error envelope and **registry skeleton** (codes added per slice thereafter — SD-10) (DD-15/17 subset) · **`cmd/minikafka` + `internal/broker`**: TCP server, dispatch with cancellable per-conn contexts and connection-role primitives (ready for SL2's control/fetch split), loopback-default listener + `--addr`/`--data`, graceful stop (DD-24/25) · **name validation before any path is formed** (DD-18) · storage spine **behind the file-operations seam from day one** (records, CRC, frontier + atomicWrite, 5 ms flusher, read-cap, boot recovery; DD-2, DD-3, DD-4, DD-5, DD-6, DD-9; the seam's real implementation here, scripted fakes in SL1 — SD-3) · Produce / Fetch (multi-entry form, single-partition use) / CreateTopic / ListTopics · single-partition long-poll park + wake on frontier advance (DD-25 pattern + §9 hook) · `Producer`/`Consumer` client + `mk` create-topic/topics/produce/consume-raw (DD-19 subset) · Syncer seam + ack-ordering test (LOG-1a) · **LICENSE (MIT) + minimal truthful README** (limitations stated — makes B1's early-publish default actually executable) · **CI baseline: build, tests, `go vet`, staticcheck (pinned), stdlib audit, macOS native build+smoke** (DD-26 baseline — the cheap continuous constraints run from day one; only demo-timing and the registry diff wait for their artifacts).
**Checks due:** LOG-2, LOG-3, PROD-1, TOP-1, CONS-1 (incl. blocked-fetch-wakes-on-produce), NFR-4. **Partial (SD-5):** LOG-1/PROD-2 half (a) — the sync-recorder ordering invariant — proven here; half (b), kill -9, completes at SL1. TOP-2's stability test here; its doc-audit half completes at SL6.
**Exit demo:** scenario B live via `mk` against the broker binary. SL7's feasibility check (0.5d, from its budget) runs at this exit and its result is committed.

### SL1 — Crash & disk hardening (3d)
**Goal:** every crash and storage-failure promise proven with scripted faults, not luck.
**Builds:** scripted fault fakes on the existing file seam (short writes, ENOSPC, corruption, rename/dir-fsync failure) · LOG-4's four recovery cases + the straddling-record case · frontier unreadable/boundary refusal rules · the `kill -9` e2e harness (real broker process, kill points under load) · sticky write-reject degrade + truncate-back, `ERR_WRITE_FAILED` added to the registry (DD-8) · topic-create crash cleanup (DD-9) · crash-walk table → tests.
**Checks due:** LOG-4, LOG-5, and LOG-1/PROD-2 half (b) — closing their partial from SL0.
**Exit demo:** scenario J live (quota-limited dir fills → named error, reads keep serving) + the kill -9 recovery run witnessed.

### SL2 — Consumer groups (4.5d)
**Goal:** the headline feature whole — and the biggest slice in the plan, priced accordingly.
**Builds:** coordinator (DD-10, DD-11, DD-12, DD-13, DD-14): control conn, heartbeats/session deadlines, immediate rebalance + range assignment, generations, serve-time fencing (`ERR_STALE_GENERATION`, `ERR_UNKNOWN_MEMBER` added), durable commits via atomicWrite, join-carries-state · group message types into wire + PROTOCOL doc-comments · `GroupConsumer` dual-conn client with REJOIN/STALE rejoin-and-reissue · multi-partition long-poll wake across owned partitions · injectable coordinator clock + GRP-2 bound test.
**Checks due:** CONS-2, CONS-3, GRP-1, GRP-2, GRP-3, GRP-4, GRP-5.
**Exit demo:** scenarios C (full — kill -9 broker with commits in flight, positions survive; the harness exists from SL1), D, E, F. E's fencing is made visible by procedure (SD-11): SIGSTOP one member past its deadline, watch takeover, SIGCONT it, watch its late commit rejected in the logs.

### SL3 — The demo (2.5d)
**Goal:** success bar G2a, honestly measured — pulled ahead of hostile-input completeness because it is the best living integration test of everything SL2 built (SD-4).
**Builds:** `cmd/demo` two acts, narrated, `#event` markers, MkdirTemp (DD-20) · CI demo-timing job: external shell clock, cold caches, gate at 60 s/180 s, receipt with resolved image + commit (DD-21) · macOS clean-cache local procedure + receipt · README top screen carries the one command.
**Checks due:** DEMO-1, DEMO-2.
**Exit demo:** scenario A — run it, watch CI run it colder, commit the receipt.

### SL4 — Hostile inputs & caps audit (2d)
**Goal:** every input cap and error real, and the registry audited complete (SD-10's completeness pass).
**Builds:** the full input-cap inventory with per-cap rejection tests (DD-16) — including **bad partition index** — plus `ERR_INVALID_NAME`/`CAP_EXCEEDED`/`FETCH_TOO_WIDE` coverage tests (DD-17) · conn cap with served error frame + idle reclaim with its clock seam (DD-24) · malformed-frame table tests · registry completeness audit against every mechanism shipped so far.
**Checks due:** PROD-3, PROT-3, NFR-2.
**Exit demo:** scenario H live — the scripted battery, every input answered by its named code, broker still serving.

### SL5 — Benchmarks (2d)
**Goal:** success bar G2b: honest, labeled, reproducible numbers.
**Builds:** `cmd/bench` closed-loop harness, ≥3 iterations, spread, GC stats, full BENCH-2 label set incl. the fsync-mode/platform caveat (DD-7) + "closed-loop response latency" framing (DD-22) · report files + `-render-readme` · CI README↔report test · Sri's reference run on stated hardware.
**Checks due:** BENCH-1, BENCH-2, BENCH-3.
**Exit demo:** scenario G.

### SL6 — Protocol doc & repo completion (2.5d)
**Goal:** the repo finished as the portfolio deliverable.
**Builds:** `docs/PROTOCOL.md` — budgeted at a full day; it must let a stranger implement a client (PROT-1) · CI registry-diff test · TOP-2's doc-audit half (no resize operation documented) · receipts record resolved runner versions (DD-26 completion) · full README pass (R1/R2/R3 statements, macOS fsync caveat per DD-7, port warning) · package doc-header audit (NFR-3) · final PROT-2/OPS-1/OPS-2/OPS-3/NFR-1 audits.
**Checks due:** PROT-1, PROT-2, OPS-1, OPS-2, OPS-3, NFR-1, NFR-3, and TOP-2's deferred half.
**Exit demo:** scenario K — a push runs the whole battery green on the public repo.

### SL7 — Showcase build (conditional, 3d total)
**Goal:** the watch-only live page — or its honest absence. **Timing (SD-7): the 0.5d feasibility check runs at SL0 exit** (external dependency, volatile terms — scope certainty early); the 2.5d build stays last and needs the repo public (B1) plus SL2's client surface.
**Builds (only if the check passed):** everything DD-23 specifies — `cmd/showcase` (feeder, poll feed, page), GitHub Pages loading shim, `render.yaml`, deploy, scripted port scan, teardown criterion in README. If the check failed: the committed feasibility record + the documented "later" (R4), and this slice ends at what the check already spent.
**Checks due:** SHOW-1, SHOW-2, SHOW-3, SHOW-4.
**Exit demo:** scenario I — or the honest absence, documented.

## 3. Process per slice (from the stage-loop BUILD stage)

Each slice: design+plan under the slice skills → red-before-green build → sabotage check at exit → machine-written receipts → `slices/<SL>/BRIEF.md` (plain language, baked diagram) → code map regenerated → commit checkpoints → Sri gates the slice exit. **This overhead is inside each slice's day count (SD-6).**

## 4. Decision ledger (slice-level)

| # | Decision | Why | Consequence if wrong |
|---|----------|-----|----------------------|
| SD-1 | Eight slices, serial. SL7's build is last and additionally depends on publication (B1); only its feasibility check is early. | Serial keeps every slice on completed predecessors; the v0.1 "SL7 independent after SL2" claim was overstated and is withdrawn. | — |
| SD-2 | SL0 contains the real broker, the safety edges (bounded decoder, name validation, loopback default), the durability spine, and both test seams — priced at 5d. | Four review seats showed v0.1's SL0 had no broker in its build list and deferred edge safety to a slice after the recommended publication point. A skeleton missing its skeleton is not a skeleton. | SL0 is the biggest early slice; that is the honest shape of this system. |
| SD-3 | The file-operations seam is built in SL0 (real implementation); SL1 adds only scripted fakes. | Retrofitting a seam through gated storage code is rework by construction. | — |
| SD-4 | Demo (SL3) before hostile-input completeness (SL4). | Cross-family finding: the demo is the best living integration test of SL2's machinery, and the success bar shouldn't queue behind completeness work; SL0 already owns the safety-critical edges. | H's full battery lands ~2.5d later than in v0.1. |
| SD-5 | Partial-proof convention: a check split across slices is listed at BOTH ends with explicit halves (LOG-1/PROD-2 a@SL0 b@SL1; TOP-2 test@SL0 doc-audit@SL6). STATUS tracks open halves. | v0.1 marked checks "due" where they could only half-run — a proven-on-half-evidence hole two seats caught. | — |
| SD-6 | Sizing: 24.5d command-summed, each slice including ~0.5d exit process. The v0.1 15.5d was build-only and uniformly under (sizing seat: every error pointed the same direction). | Honest sizing beats optimistic sizing — actually practiced this time. | Estimates still carry ±1d each; gates catch drift. |
| SD-7 | SL7's feasibility check executes at SL0 exit; its build stays last. | External-dependency volatility argues for early scope certainty; the conditional showcase must never block the success bars. | 0.5d spent early even if Sri later drops the showcase. |
| SD-8 | Error-code convention: registry skeleton + envelope in SL0; each slice adds its mechanism's codes; SL4 audits completeness; PROTOCOL.md (SL6) documents the final set. | Retroactive standardization of SL2's codes was the alternative — worse. | — |
| SD-9 | PROTOCOL.md authored in SL6 at a full day's budget; wire doc-comments carry the contract until then. | The registry is final only after SL4; a stranger-implementable doc is real writing, not a checkbox. | Early protocol readers (none expected) wait. |
| SD-10 | Publication default: SL0 exit, executable as written (LICENSE + truthful README are SL0 build items). Timing and account remain Sri's gate decisions (§5). | v0.1's recommended default wasn't executable from its own build lists — an all-rights-reserved "portfolio" publication. | If Sri picks B1-late, SL0's publish step just moves. |
| SD-11 | Scenario E's fencing is demonstrated by SIGSTOP-past-deadline / SIGCONT-late-commit-rejected, scripted in the exit demo. | A SIGKILLed consumer emits no late commit — v0.1's demo claimed an observation it could not produce. | — |

## 5. Open at this gate (Sri decides)

- **B1 — Publish when?** At SL0 exit with LICENSE + truthful README, iterating in public (recommended — matches locked DESIGN D11 "public early"; honest commit history is itself portfolio material) — or after SL3 when the 60-second demo works.
- **B2 — Which GitHub account?** Your existing account, or a new one (which per your standing rule would register under sr7544068@gmail.com). Only you can perform the publish step either way.
