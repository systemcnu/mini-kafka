# SL4 Slice Design — Hostile inputs & caps audit

**Status: DRAFT (2026-07-29) — awaiting 2-seat independent review.**
**Derives from: SPEC v1.0.1 · DESIGN v1.0.1 · SLICES v1.0.1 (hashes in STATUS.md).** Scope authority: SLICES §2/SL4. Mechanisms: DD-16 (cap inventory), DD-17 (error registry), DD-18 (names), DD-24 (edge enforcement), referenced never restated.
**Done =** scenario H real: a scripted battery throws every hostile input class at a live broker — every one answered by its named stable code, the broker still serving after each — and the registry is audited complete: every code elicited, every cap backed by a rejection test, mechanically re-checked on every future `go test`.

## 1. Spec contract table

| ID | Treatment | Notes |
|---|---|---|
| PROT-3 | Real | The battery = one live-wire elicitation per registry code (all 13), each asserting the code AND that the broker still serves. Supersedes nothing: existing per-cap tests stay; the battery is the completeness layer on top. |
| PROD-3 | Real (since SL0; re-proven here) | Oversized produce → MSG_TOO_LARGE naming the cap, partition bytes unchanged — existing test cited in the audit table; battery re-elicits the code. |
| NFR-2 | Real | DD-16's 12 input caps each mapped to a named rejection test in the audit table (§5); the two SL4-built caps (conn-cap frame, idle reclaim) close the last gaps. In-flight/conn = 1 is structural (§3). |
| Scenario H | **Owned here** | Exit demo: the battery run live with transcript receipt. |
| PROT-1 (registry diff) | Deferred → SL6 | But `wire.AllCodes()` built here is the hook PROT-1's diff will consume (D-SL4-1). |

## 2. Slice-local decisions

| # | Decision |
|---|---|
| D-SL4-1 | **The completeness audit is a test that cannot go stale, not a checklist.** `internal/wire` gains `AllCodes() []Code` — the registry self-describes, maintained adjacent to the constants, with a wire test asserting the list is dense over 1..max (a new constant without an AllCodes entry fails immediately). The battery (`internal/broker/registry_test.go`, real TCP on `127.0.0.1:0`) walks AllCodes(): each code elicited from a LIVE broker by a genuinely hostile input (not a unit call), ticked off a map; the final assertion is `elicited == AllCodes()`. Consequence: any future slice adding a code without teaching the battery to elicit it goes red — SD-8's "SL4 audits completeness" becomes a standing property, and SL6's PROTOCOL.md diff (PROT-1) gets its machine-readable registry for free. |
| D-SL4-2 | **Conn cap now serves its frame (DD-24's "accept → write error → close", replacing SL0's silent close).** In acceptLoop, an over-cap accept spawns a short goroutine: `SetWriteDeadline(now+1s)` → write the CAP_EXCEEDED error frame → close. The goroutine + deadline are load-bearing: a hostile client that never reads would otherwise wedge acceptLoop itself (writing synchronously from the accept thread hands the attacker the listener). Over-cap conns never enter the registry, hold no slot, and are invisible to the coordinator. Existing TestConnectionCapGuard upgraded: the 257th conn now READS the CAP_EXCEEDED frame, then EOF. |
| D-SL4-3 | **Idle reclaim (DD-24): read deadline armed before each frame-wait; seam = `Config.IdleTimeout` (0 → 5 min default).** `SetReadDeadline(now+idle)` immediately before each ReadFrame — so a conn is reclaimed after 5 min without a COMPLETE request, which also reclaims a slow-loris stalling mid-frame (the deadline spans the whole frame read). Expiry → close, NO error frame (the peer is by definition not talking; writing at a dead peer can stall — gap G-SL4-1). The deadline never kills a parked fetch: parks happen in dispatch, AFTER the frame is read; no conn reads occur during a park, and the deadline is re-armed fresh on the next loop iteration — proven by an adversarial test with IdleTimeout < the fetch's maxWait. Response writes get `SetWriteDeadline(now+idle)` too: a client that sends one request and never drains the response is the same leaked conn DD-24's reclaim exists to evict (without this, a stalled WriteFrame parks serveConn forever and the reclaim never fires — the cap-protection claim would be false). Heartbeating control conns are never idle by arithmetic (500 ms beats ≪ 5 min); ordinary consumers re-request within their ≤ 30 s maxWait. The duration seam (not a Clock interface) is deliberate: net.Conn deadlines run on real time — an injected clock cannot drive them; tests use ~200 ms. |
| D-SL4-4 | **Broker-level malformed table — the group-message rows SLICES names, plus every other type.** One table test over real TCP: for EACH message type (Produce, Fetch, CreateTopic, ListTopics, JoinGroup, Heartbeat, CommitOffsets, LeaveGroup, GroupFetch) — truncated body → MALFORMED, trailing bytes → MALFORMED; plus the frame-level rows: below-min length, bad version, unknown type, oversized frame. Non-closing rejections assert the SAME conn then serves a valid request; closing rejections (frame-level: the stream is untrustworthy) assert a NEW conn serves. The wire-layer decode tables (messages_test.go) stay — they prove the decoder; this proves the SERVED frame and the broker's survival, which is what PROT-3's check text demands. |
| D-SL4-5 | **Group caps get their wire-level rejection tests (the audit's found gap).** Coordinator-level tests exist (coordinator_test.go); nothing proves the SERVED frame. Added: 33rd member's JoinGroup → CAP_EXCEEDED frame; 65th group's JoinGroup → CAP_EXCEEDED frame (65 conns, well under the 256 conn cap). Both assert the broker still serves. |
| D-SL4-6 | **Registry deltas on the record, not silently absorbed.** (a) DD-17's locked list names 12 codes; the shipped registry has 13 — SHUTTING_DOWN (10) was SL0's graceful-stop addition under SD-8's each-slice-adds rule; PROTOCOL.md (SL6) documents the final set. (b) SLICES' SL4 row cites "SD-10's completeness pass" where the registry convention is actually SD-8 (SD-10 is the publication default) — a locked cross-reference misnumber, intent unambiguous, surfaced at this gate like SL3's DD-21 wording (no erratum cascade proposed). |
| D-SL4-7 | **What SL4 does NOT touch.** No new codes, no wire-format changes, no client API changes, no mk changes, no coordinator behavior changes. The two behavior changes are DD-24's own text (served frame, idle reclaim) — both marked "SL4's" in SL0-era code comments. Everything else is tests + the AllCodes hook. |

## 3. Known gaps accepted

- **G-SL4-1** — idle-reclaimed conns are closed without an error frame (unlike the conn cap): the peer is absent by definition and a farewell write can stall. A well-behaved client sees EOF/reset and reconnects (conn slot freed is the point).
- **G-SL4-2** — in-flight requests per connection (NFR-2's inventory) has no rejection test because there is nothing to reject: DD-15's serve loop reads the next frame only after the response is written, so the cap (=1) is structural; a pipelining client's second request waits in the kernel buffer. Recorded in the audit table as "structural — by construction", with the serve-loop test cited.
- **G-SL4-3** — response-frame cap is enforced by the receiving side (client's ReadFrame at MaxResponseFrame; wire-level rejection test exists) and respected by construction on the sending side (maxBytes ≤ 4 MiB + bounded overhead ≤ 64 KiB headroom); no broker-side test forces an oversized response into existence. Audit table row says exactly this.
- **G-SL4-4** — PROTOCOL.md and its CI registry diff remain SL6's (SD-9); AllCodes() is built now so that diff has a stable hook.

## 4. Test plan mapped to claims

- **The battery** (`registry_test.go`, scenario H): all 13 codes elicited live — UNKNOWN_TOPIC (produce to absent topic) · TOPIC_EXISTS (duplicate create) · BAD_PARTITION (produce AND fetch at partition ≥ count) · INVALID_NAME (hostile name with `../`) · MSG_TOO_LARGE (1 MiB+1 payload; bytes-unchanged assertion = PROD-3) · FRAME_TOO_LARGE (oversized frame) · MALFORMED (truncated body) · CAP_EXCEEDED (maxWait over cap) · FETCH_TOO_WIDE (17 entries) · SHUTTING_DOWN (request during drain — shutdown_test.go's existing elicitation pattern) · WRITE_FAILED (produce onto a scripted FaultFS write failure via newWithFS) · STALE_GENERATION (commit at generation N−1 after a rebalance) · UNKNOWN_MEMBER (commit from a fabricated member). Completeness: elicited-set == AllCodes(). Broker-still-serving after every non-terminal elicitation.
- **Conn cap frame:** 257th conn reads CAP_EXCEEDED then EOF; existing under-cap conns unaffected (D-SL4-2).
- **Idle reclaim:** idle conn reclaimed at short IdleTimeout · heartbeating GroupConsumer survives the same window (D-SL2-11's exemption, proven not asserted) · cap protection: fill the cap with idle conns, wait one timeout, a NEW conn is admitted and served · adversarial: IdleTimeout < fetch maxWait — the parked fetch completes and the conn survives (the deadline-never-spans-a-park proof) · stalled-reader: a client that never drains its response is reclaimed (write deadline).
- **Malformed table** (D-SL4-4): 9 message types × {truncated, trailing} + 4 frame-level rows, served frame + survival asserted per row.
- **Group caps over the wire** (D-SL4-5).
- **AllCodes density test** (wire package).

## 5. Cap inventory audit table (NFR-2's check, emitted into the slice)

Deliverable artifact: a table in this slice's BRIEF and mirrored as a comment block atop `registry_test.go` — every DD-16 input cap → its enforcing code path → its named rejection test (12 rows: payload · request frame · response frame [G-SL4-3's wording] · fetch maxBytes · fetch maxWait · fetch entries · connections · topics · partitions/topic · groups · members/group · name length) + the structural in-flight row (G-SL4-2). Counts by command at write time, per the no-hand-typed-numbers rule.

## 6. Validate — exit checklist (all demonstrated, not asserted)

1. Red-before-green for every new test (battery run against pre-SL4 broker where applicable: conn-cap frame and idle-reclaim tests MUST fail on the old code; battery's completeness assertion red until all 13 elicitations exist).
2. Full battery + existing suite race-green by command; `scripts/checks.sh` green.
3. Scenario H live: `go test -v -run TestScenarioH ./internal/broker` transcript → `docs/receipts/sl4-scenario-h.txt` (every code named, broker-survival lines visible).
4. Exit sabotage (verifier's hands): disable one enforcement (e.g. the payload cap check) → battery AND the per-cap test go red → restore → green.
5. Push → CI green (all five jobs).
6. Code map + BRIEF (baked diagram) + STATUS/LAB-STATE + commits.
7. Gate notes for Sri: the SD-10→SD-8 cross-reference misnumber (D-SL4-6b) and the 13th code's provenance (D-SL4-6a).
