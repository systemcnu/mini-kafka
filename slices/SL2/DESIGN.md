# SL2 Slice Design — Consumer groups

**Status: DRAFT for review.**
**Derives from: SPEC v1.0.1 · DESIGN v1.0.1 · SLICES v1.0.1 (hashes in STATUS.md).** Scope authority: SLICES §2/SL2. Mechanisms: DESIGN DD-10..14 (coordinator), DD-15/16/17 (wire, caps, codes), DD-19 (client), DD-25 (concurrency), referenced never restated.
**Done =** the headline feature whole: groups with heartbeats, immediate rebalance, generation fencing, durable commits — scenarios C (full), D, E, F all demonstrated, GRP-1..5 + CONS-2/3 checks green.

## 1. Spec contract table

| ID | Treatment | Notes |
|---|---|---|
| GRP-1 | Real | Range assignment per generation (DD-11); exact-partition test (2 members × 4 partitions, tagged with generation). |
| GRP-2 | Real | Injectable coordinator clock (DESIGN §9); measured marked-dead → new-generation ≤ 1 s bound test + kill-one-of-two e2e complement. |
| GRP-3 | Real | Crash a member mid-batch pre-commit → union across group = all produced (at-least-once test). |
| GRP-4 | Real | Second group joins after first finished → full stream from 0 (D14: fresh group starts at 0 — no retention, earliest = 0 always). |
| GRP-5 | Real | Serve-time fencing (DD-12): SIGSTOP-past-deadline test + SD-11 live procedure receipt. Codes 12/13 activate (values pinned since D-SL0-8). |
| CONS-2 | Real | Commit = next-to-read (§1b/D13); consume-half/commit/restart-consumer test resumes exactly there. |
| CONS-3 | Real | atomicWrite-before-ack (DD-13); kill -9 harness grows commit journaling — closes scenario C's group-positions half (SL1's G-SL1-3). |
| CONS-1 (completion) | **Closes SL0's G7** | Multi-entry Fetch served (≤16 entries) + multi-partition long-poll wake — the same serving path GroupFetch uses, minus fencing. |
| Scenario C (full) | **Owned here** | SL1's harness + a committing group member child; one-directional commit assertion (see D-SL2-9). |
| Scenarios D, E, F | **Owned here** | As tests + E additionally as the SD-11 live receipt. |
| PROT-1 (partial) | Doc-comments only | Wire doc-comments for group types now; PROTOCOL.md + registry-diff CI → SL6. |
| PROT-3 (rows 12/13) | Real | Stale-generation + unknown-member rejection tests, broker stays up. |
| mk group subcommands | **Out (deferred → SL3/SL6 polish)** | Demo and scenarios use the `client` package directly (PROT-2's no-privileged-path holds — client IS the public path). |

## 2. Slice-local decisions

| # | Decision |
|---|---|
| D-SL2-1 | **Wire additions (types 9–17, additive only).** 9 JoinGroup `[str group][str topic]` → 10 JoinGroupResp `[str memberID][u64 generation][u32 n]{[u32 partition][u64 nextOffset]}` (join carries state, DD-14). 11 Heartbeat `[str group][str memberID][u64 generation]` → 12 HeartbeatResp `[u8 flags]` (bit0 = REJOIN, DD-11). 13 CommitOffsets `[str group][str memberID][u64 generation][u32 n]{[u32 partition][u64 next]}` → 14 CommitOffsetsResp empty. 15 LeaveGroup `[str group][str memberID]` → 16 LeaveGroupResp empty. 17 **GroupFetch** `[str group][str memberID][u64 generation]` + Fetch's exact entry/maxWait/maxBytes tail → answered by FetchResp (type 4, reused — one decode path). Codes 12 STALE_GENERATION, 13 UNKNOWN_MEMBER activate. |
| D-SL2-2 | **Why GroupFetch is a NEW type (design-tension note, stated openly):** DD-12 requires group fetches to CARRY (memberID, generation); SL0's G7 declared the raw Fetch wire shape final (and shipped it). An additive type honors both; mutating Fetch's body would break SL0's recorded finality for zero gain. DD-15's type list is read as non-exhaustive (it already elides response types). Flag for reviewers: if this reads as a DESIGN erratum instead, say so — but no behavior in DD-15 changes either way. |
| D-SL2-3 | **Coordinator: `internal/group`, one mutex, injectable clock.** State: `groups[name] = {topic, generation, members[id]{lastBeat, assignment}, committed[partition]next, dirty-join flag}`. All transitions under one mutex, never held during I/O or parking (DD-25). `Config{HeartbeatInterval 500ms, SessionTimeout 2s, SweepInterval 100ms, Clock}` — the clock is DESIGN §9's seam; GRP-2's bound test runs on a fake clock, no real sleeps. Liveness (DD-10): control-conn drop = immediate death; sweeper marks dead on missed session deadline. Any membership event → bump generation + recompute range assignment (sorted memberIDs × contiguous ranges) immediately + set REJOIN in subsequent heartbeat responses (DD-11). MemberIDs are coordinator-assigned (`m<seq>`, unique per broker lifetime — restart forgets everything, DD-12's auto-fencing). |
| D-SL2-4 | **`data/_groups/` is RESERVED storage, and Store.Open must not eat it.** DD-13 puts commits in `data/_groups/<group>.json`. Store.Open currently REMOVES any data-dir entry lacking `meta.json` (aborted-create cleanup) — unguarded, every boot would DELETE all group commits. Rule: Open skips the literal name `_groups` (collision-impossible: DD-18 names must start `[a-z0-9]`). A test stages `_groups` + a junk meta-less dir and proves exactly one survives boot. The coordinator owns everything under `_groups/`. |
| D-SL2-5 | **Commit path (CONS-3): atomicWrite BEFORE ack, one file per group.** `{"topic":T,"offsets":{"0":n,...}}` (offsets = next-to-read). Serve-time order: validate fencing under the mutex → merge offsets into a copy → atomicWrite `_groups/<group>.json` (mutex NOT held during the write; a per-group commit lock serializes writers) → install merged map under the mutex → ack. Commits for partitions outside the member's current assignment → STALE_GENERATION (the assignment moved). Loaded lazily: a group's file is read at its first JoinGroup after boot; unreadable/corrupt commit file → refuse the join loudly (MALFORMED with the file named), never guess positions. |
| D-SL2-6 | **Fencing at serve time (DD-12), exactly two errors.** Order of checks for GroupFetch/Commit/Heartbeat: group unknown OR memberID not in live set → UNKNOWN_MEMBER (13) · generation ≠ current → STALE_GENERATION (12). Zero state change on either. A parked GroupFetch re-validates on EVERY wake (park loop re-enters the coordinator check). JoinGroup on an existing group with a different topic → MALFORMED("group <g> is bound to topic <t>") — D15 one-topic-per-group; reviewers: attack this code choice. JoinGroup with unknown topic → UNKNOWN_TOPIC. Caps: groups ≤ 64, members/group ≤ 32 → CAP_EXCEEDED; group name → INVALID_NAME (DD-16/18). |
| D-SL2-7 | **Multi-partition serving = `TryFetch` + goroutine-per-entry park (no reflection, no SL0 refactor).** New storage API: `Partition.TryFetch(offset, maxBytes) (recs, notifyCh, err)` — the read + channel-capture atomically under the read lock (the missed-wakeup pin, D-SL0-5). The broker's fetch loop: TryFetch every entry (respecting maxBytes across the whole response, first-listed-first) → any records: respond · none: park one goroutine per entry on `select{<-notifyCh, <-done}`, first wake wins a shared buffered chan, `close(done)` reaps the rest → loop. Entries ≤ 16 bounds the goroutines; they cannot leak (done is always closed on every exit path). Raw single-entry Fetch keeps its existing `Partition.Fetch` path UNTOUCHED (surgical rule); raw multi-entry (2..16) and GroupFetch share the new loop. GroupFetch re-fences each iteration (D-SL2-6). |
| D-SL2-8 | **Client `GroupConsumer` (DD-19): two conns, rejoin-and-reissue.** Control conn: Join → heartbeat every 500 ms; a mutex serializes heartbeat/commit roundtrips (one-request-in-flight per conn is the SL0 protocol rule). Fetch conn: GroupFetch over ALL owned partitions from per-partition `next` cursors. On REJOIN flag, STALE_GENERATION, or UNKNOWN_MEMBER: re-Join on the control conn, adopt the new assignment + committed cursors, reissue the fetch. API: `JoinGroup(addr, group, topic) (*GroupConsumer, error)` · `Poll(ctx-less, maxWait) ([]PartRecord, error)` (PartRecord = partition + offset + payload) · `Commit() error` (commits current cursors, CONS-2 next-to-read) · `Close()` (LeaveGroup + both conns). Deadlines wrap wire I/O only; the fetch read deadline = maxWait + a grace margin, never a bare park (DD-19). |
| D-SL2-9 | **Scenario C completion in the harness — one-directional, like SL1.** The kill -9 harness adds a group-consumer child thread (same process, shipped client): consume, commit per batch, journal every ACKED commit (partition → next). After each restart: JoinGroup returns recovered committed offsets; assert recovered ≥ journaled per partition (commit acked ⇒ durable ⇒ never lower; higher is legal — a later commit's ack outran the journal). Group positions intact = that assertion, cycle after cycle. |
| D-SL2-10 | **Scenario E live (SD-11) via the re-exec pattern — no new public binaries.** The e2e test binary re-execs itself as a group-member child (env-var switch in TestMain). Procedure test: two member processes on 4 partitions → SIGSTOP one → fake-free real-time wait past 2 s session → survivor owns all 4 (JoinGroup assignment observed) → SIGCONT → the woken member's commit gets STALE_GENERATION or UNKNOWN_MEMBER, logged and asserted, group positions unchanged. The `-v` run of this test IS the SD-11 receipt (`docs/receipts/sl2-scenario-e.txt`). Scenarios D and F are plain client tests (restart-resume exact offset; second group full stream from 0). |
| D-SL2-11 | **Server → coordinator lifecycle glue.** JoinGroup binds memberID to its conn; the server's existing per-conn teardown calls `coord.ConnClosed(connID)` → immediate death + rebalance (DD-10 control-conn-drop). Heartbeats keep control conns busy, so SL4's future idle reclaim can never kill them (DD-24 note, no code here). Graceful stop: coordinator returns SHUTTING_DOWN to group requests during drain — parked group fetches already return empty via the stopping channel (D-SL0-6 unchanged). |

## 3. Crash/failure-point analysis (the rows groups add to DESIGN §7)

| Failure mid… | Outcome |
|---|---|
| commit atomicWrite | Old or new file, never torn (DD-13); unacked → client may re-commit → same value, idempotent (R2-class, correct) |
| broker kill -9 with commits in flight | Acked commits durable (harness-proven); unacked lost → re-delivery from last commit (GRP-3 at-least-once, exactly the spec's promise) |
| member SIGKILL/SIGSTOP | Session deadline (2 s) → death → generation bump → survivor assignment ≤ 1 s later (GRP-2); late riser fenced at serve time (GRP-5) |
| rebalance vs parked fetch | Parked GroupFetch re-fences on wake → STALE_GENERATION → client rejoins (DD-12; no generation-bump wake needed) |
| broker restart | Membership forgotten (all pre-crash memberIDs unknown → fenced); commits reloaded from `_groups/` on first join; boot NEVER deletes `_groups` (D-SL2-4) |
| coordinator sweep vs heartbeat race | Both under the one mutex — a heartbeat at t=deadline either lands before the sweep (member lives) or after (fenced on its next call); no torn state either way |

## 4. Known gaps accepted

- **G-SL2-1** — `mk` gets no group subcommands here (demo/bench never needed them; SL3's demo uses the client; SL6 decides the final CLI surface). Scenario walkthroughs use test binaries and the client API.
- **G-SL2-2** — commit files are per-group JSON, rewritten whole per commit batch: fine at D4's per-batch rate and ≤16 partitions; stated in the file's doc comment. Not a mechanism for high-rate commits, by design (spec D4).
- **G-SL2-3** — GRP-2's 1 s detection→assignment bound is proven on the fake clock + observed e2e; the 500 ms/2 s defaults themselves stay compile-time constants in `group.Config` defaults (no flags — NFR-2's cap discipline: nothing new user-tunable without a cap).
- **G-SL2-4** — the SD-11 receipt's SIGSTOP timing uses real wall-clock (2 s+ sleeps in one e2e test): accepted, it is the REALISM leg; the deterministic leg is the fake-clock bound test.

## 5. Test plan mapped to claims

- **Coordinator unit (fake clock):** GRP-1 exact-partition (2×4, generation-tagged) · GRP-2 bound (advance past deadline → sweep → new generation within 1 s of detection; measured on the fake clock) · join/leave/death each bump generation once · REJOIN flag visible in next heartbeat · caps (65th group, 33rd member) → CAP_EXCEEDED · one-topic-per-group violation → MALFORMED.
- **Fencing (broker-level, real TCP):** stale-generation fetch + commit → 12; unknown member → 13; zero state change proven by re-reading positions; PROT-3 rows: broker serves normal traffic after every rejection.
- **Commit path:** CONS-2 consume-half/commit/restart-consumer resumes exactly · CONS-3 via harness (D-SL2-9) · commit-outside-assignment → STALE_GENERATION · corrupt `_groups/<g>.json` → join refused loudly · `_groups` survives boot while junk dirs are removed (D-SL2-4).
- **Multi-partition wake:** parked multi-entry fetch (raw AND group) wakes on ANY listed partition's frontier advance, not on raw append (extends SL0's wake test through the new loop); goroutine-leak check via ParkedWaiters + `-race`.
- **Scenarios:** C = harness with commits (D-SL2-9) · D = restart-resume test · E = SD-11 re-exec test (+ live `-v` receipt) · F = second-group-from-0 test.
- **Client:** GroupConsumer rejoin-and-reissue on REJOIN and on STALE (kill the assignment under it mid-poll); control-conn serialization race test under `-race`.

## 6. Validate — exit checklist (all demonstrated, not asserted)

1. Red-before-green record for every new test file (receipt).
2. Coordinator unit suite green on the fake clock, including the GRP-2 measured bound.
3. Fencing suite green: codes 12/13 live, zero-state-change proven, broker survives (scenario H's stale-generation row now real).
4. CONS-2/CONS-3/GRP-3/GRP-4 tests green; `_groups` boot-preservation test green.
5. kill -9 harness with commit journaling: 3 cycles green, receipt shows per-cycle commit evidence (`docs/receipts/sl2-kill9-commits.txt`).
6. Scenario E SD-11 live receipt committed (`docs/receipts/sl2-scenario-e.txt`): takeover + fenced late commit visible.
7. CONS-1 fully closed: raw multi-entry fetch served and woken across partitions (G7 lifted — the SL0 CAP_EXCEEDED guard for entries>1 removed, >16 still FETCH_TOO_WIDE).
8. Exit sabotage (verifier's hands): disable serve-time generation check → fencing tests red → restore → green.
9. `scripts/checks.sh` fully green (staticcheck included); CI green on GitHub after push.
10. `slices/SL2/BRIEF.md` (baked diagram) + code map extended (with a completeness pass over new .go files) + STATUS/LAB-STATE + commits.

---
**DRAFT for review** — the contracts reviewers should attack: D-SL2-1's wire shapes, D-SL2-2's new-type-vs-erratum call, D-SL2-4's reservation rule, D-SL2-5's lock/ack ordering, D-SL2-6's error precedence, D-SL2-7's park teardown, D-SL2-9's one-directional commit assertion.
