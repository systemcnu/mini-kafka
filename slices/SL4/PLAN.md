# SL4 Implementation Plan

**Derives from: slices/SL4/DESIGN.md FINALIZED 2026-07-29.** Any design change patches this plan in the same change. Contracts live in D-SL4-* and DD-16/17/18/24; this plan owns where code lives and build order. Scale: ~4 small production edits (server.go conn-cap frame + idle deadlines, wire AllCodes, client Poll redial trigger, doc comments), the rest is tests: 1 new battery file, extensions to caps/group tests, 1 new wire audit test.

## Codebase map (delta only)

```
internal/wire/errors.go        EDIT: AllCodes() []Code adjacent to the const block (D-SL4-1)
internal/wire/errors_test.go   NEW (or extend): go/parser leg — every declared Code constant
                               ∈ AllCodes(); no dupes, non-empty (D-SL4-1 leg 1)
internal/broker/server.go      EDIT: (a) acceptLoop over-cap path → wg-tracked writer goroutine,
                               1 s write deadline, CAP_EXCEEDED frame, close (D-SL4-2);
                               (b) Config.IdleTimeout (0 → 5 min) + SetReadDeadline before each
                               ReadFrame + SetWriteDeadline before each response write (D-SL4-3);
                               idle-deadline expiry closes silently (distinguish via net.Error)
internal/broker/registry_test.go  NEW: the battery (D-SL4-1 leg 2, §4) — TestScenarioH; cap
                               inventory audit table as the top comment block (§5)
internal/broker/caps_test.go   EXTEND: conn-cap test now reads the served frame (D-SL4-2);
                               idle-reclaim tests (all 5 rows of §4); group caps over the wire
                               (D-SL4-5); fetch-side bad-partition row (CF4)
client/client.go               EDIT: GroupConsumer fetch-conn redial — add EOF/reset trigger to
                               the existing timeout-redial path, nothing else (D-SL4-7(4));
                               doc comments on Producer/Consumer re idle reclaim (G-SL4-1)
client/group_test.go           EXTEND: paused-Poll redial test (§4)
docs/receipts/                 sl4-red-green.txt · sl4-scenario-h.txt (verifier captures H)
```

**Where do I look for X?** why AllCodes is parsed from source → errors_test.go top comment (D-SL4-1) · battery structure/instance map → registry_test.go top comment · why the over-cap writer is wg-tracked → server.go acceptLoop comment (D-SL4-2) · why deadlines never kill parks → server.go serveConn comment (D-SL4-3) · why Poll redials on EOF → client.go comment (D-SL4-7(4)).

**Orchestration rule:** registry_test.go lives in package `broker` (needs newWithFS for the FaultFS instance); raw hostile bytes go over real TCP conns, never handler calls.

## Build order (each row done when DEMONSTRATED)

| # | Builds | Done when |
|---|--------|-----------|
| 1 | SEAMS ONLY commit (§6.1): Config.IdleTimeout field (unenforced) + AllCodes() + parser test scaffold | new seam-dependent tests COMPILE and run RED; red transcript captured per test file into sl4-red-green.txt; conn-cap frame test additionally red against the pre-seam serve path (EOF, no frame) |
| 2 | wire audit leg green: AllCodes + parser test | errors_test green; sabotage probe: a scratch `CodeProbe Code = 14` constant (not committed) makes the parser leg red |
| 3 | conn-cap served frame (D-SL4-2) | upgraded conn-cap test green: 257th conn reads CAP_EXCEEDED then EOF; under-cap conns unaffected; -race clean incl. Stop while an over-cap writer is in flight |
| 4 | idle reclaim (D-SL4-3) | all 5 idle rows green: idle-reclaimed · heartbeater survives · cap protection · park survives IdleTimeout < maxWait · stalled-reader (several-hundred-KB response, red when write deadline removed — capture that red too) |
| 5 | client Poll redial + doc comments (D-SL4-7(4)) | paused-Poll test green: membership never lapses, next Poll serves records after reclaim |
| 6 | malformed table + group caps + fetch bad-partition (D-SL4-4/5, CF4) | every row green; fill-count asserted before each over-cap attempt; same-conn/new-conn survival split per row |
| 7 | the battery (D-SL4-1 leg 2) | TestScenarioH green: union of elicitations == AllCodes() across the three instances (shared OSFS · scoped FaultFS · throwaway Stop-hammer); broker-still-serving after every non-terminal elicitation |
| 8 | full suite + checks.sh | race-green by command; count reported by command |

1 strictly first (the red evidence); 2–5 in any order after; 6–7 after 3–4 (they exercise the new edges); 8 last.

## Pitfalls (named so they can be checked)

- **The idle deadline is re-armed at the TOP of each serve-loop iteration** — never cleared mid-dispatch, never armed inside dispatch. A park (≤30 s) happens after the frame is read; no conn reads occur during it; the stale absolute deadline is harmless and replaced on the next iteration. Do NOT SetReadDeadline(time.Time{}) after read — pointless churn.
- **Idle expiry must be told apart from hostile bytes:** deadline expiry surfaces as a net.Error with Timeout()==true from ReadFrame's io layer — close silently (no frame). Do not let it fall into the "serve MALFORMED" path.
- **The over-cap writer goroutine:** wg.Add BEFORE `go`, inside acceptLoop (safe: Stop waits acceptDone → wg.Wait). The conn it owns is NOT in s.conns — its close is the goroutine's own job on every path.
- **Stalled-reader test sizing:** the response must exceed loopback send+receive socket buffers (several hundred KB — build it from a fetch of many ~1 KiB records or one near-max payload); a small response slips into kernel buffers and the read deadline masks a missing write deadline (F3).
- **FaultFS scoping in the battery:** suffix must be the target topic's own log (`<topic>/0/log`), broker built via newWithFS from the start; an unscoped "log" suffix sticky-degrades whatever writes next (F4).
- **SHUTTING_DOWN hammer:** collect responses until EOF, assert ≥1 code-10 frame across the run — never exactly-once, never frame-then-clean-close ordering (the drain window races conn force-close, F2).
- **Group-cap fills:** raw-wire conns never heartbeat; the 2 s session window is unseamable at broker level — assert the fill count immediately before the over-cap attempt so a sweep shows as a loud fixture failure, not a flaky pass (F9). One conn can hold joins in many distinct groups — verify at the coordinator seam before using it for the 64-group fill.
- **STALE_GENERATION elicitation needs a second conn** — same-conn re-joins are re-Joins by design and cannot go stale; join member A (conn 1), join member B (conn 2) to bump the generation, then commit from A with the old generation.
- **Malformed table's ListTopics row is the special case** (any non-empty body → MALFORMED); do not manufacture a fake "truncated" cell (F6).
- **Poll redial:** extend the EXISTING redial branch's trigger condition only (io.EOF / ECONNRESET on the fetch conn); do not touch join/generation handling; the heartbeat goroutine is already independent of Poll cadence.
- Existing tests assume no deadlines: the 5 min defaults are far above every test's lifetime, but greps for long-held idle conns (shutdown tests hold conns open) are due diligence before assuming.
