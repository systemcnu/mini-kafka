# SL2 Implementation Plan

**Derives from: slices/SL2/DESIGN.md FINALIZED 2026-07-29.** Any design change patches this plan in the same change. Contracts live in the slice design (D-SL2-*) and project DESIGN (DD-10..19/25); this plan owns where code lives and build order. Scale: 1 new package (`internal/group`), wire additions (types 9–17, codes 12/13 activate), broker glue, client `GroupConsumer`, `mk consume -g`, ~8 new/extended test files, 3 receipts.

## Codebase map (delta only — SL0/SL1 maps still hold)

```
internal/wire/messages.go       EXTEND: types 9–17 encode/decode + doc-comments (D-SL2-1)
internal/wire/errors.go         codes 12/13 already pinned — no edit expected
internal/group/coordinator.go   NEW: groups/members/generations state machine, one mutex,
                                sweeper, level-triggered REJOIN, injectable Clock (D-SL2-3/3b/6)
internal/group/commits.go       NEW: commit merge-inside-commitLock + atomicWrite + re-fence
                                install; two-phase lazy load; MkdirAll(_groups) (D-SL2-5)
internal/storage/store.go       EDIT: Open skips the reserved "_groups" dir (D-SL2-4)
internal/storage/partition.go   EDIT: TryFetch — read + notify-capture, one RLock section (D-SL2-7)
internal/broker/server.go       EDIT: conn teardown → coord.ConnClosed(connID) (D-SL2-11)
internal/broker/handlers.go     EXTEND: 5 group handlers + the multi-entry fetch loop
                                (goroutine-per-entry park, wake chan buffered len(entries),
                                fresh done per round) + lift entries>1 guard (D-SL2-6/7)
client/client.go                EXTEND: GroupConsumer (dual conn, lazy rejoin, Commit surfaces
                                fencing; PartRecord) (D-SL2-8)
cmd/mk/main.go                  EXTEND: consume -g (join/poll/print/commit-per-batch) (contract row)
internal/group/*_test.go        coordinator unit suite on the fake clock
internal/broker/group_test.go   fencing suite + multi-entry wake over real TCP
client/group_test.go            GroupConsumer rejoin/reissue + CONS-2 + scenarios D/F
internal/e2e/group_e2e_test.go  GRP-3 union + takeover (two members, real broker process)
internal/e2e/crash_test.go      EXTEND: single committing member + commit journal (D-SL2-9)
docs/receipts/sl2-red-green.txt · sl2-kill9-commits.txt · sl2-scenario-e.txt
```

**Where do I look for X?** liveness/rebalance/generations → group/coordinator.go · commit durability → group/commits.go · why a member got 12 vs 13 → coordinator.go fence funcs (D-SL2-6's precedence) · multi-partition park → broker/handlers.go fetch loop · client rejoin policy → client/client.go GroupConsumer · the SD-11 procedure → docs/receipts/sl2-scenario-e.txt header.

**Orchestration rule (adds one line):** `internal/group` imports `internal/storage` (commit files via the FS seam; partition lookups stay in broker) and NEVER `internal/wire` — the broker maps group sentinel errors onto codes, same as storage's. Everything else unchanged.

## Entry points (delta)

- `mk consume -t T -g G [-addr]` — group membership from the CLI (the SLICES demo surface).
- `go test ./internal/group/` — the fake-clock suite; e2e grows two tests.

## The trace that matters this slice (join → rebalance → fenced commit)

`mk consume -g` → `client.JoinGroup` dials control conn → JoinGroup frame → `handlers.handleJoinGroup` → `coord.Join(conn, group, topic)` under mutex: conn already bound to live member? re-Join (same ID, no bump — D-SL2-3b) else new member + generation bump + range reassign → JoinGroupResp(memberID, gen, assignment, committed) → client starts heartbeat loop (500 ms) + fetch conn GroupFetch loop. Member dies → sweeper (100 ms tick, fake-clock-able) sees lastBeat > 2 s → removes member, bumps generation, reassigns → survivor's next heartbeat returns REJOIN bit (level-triggered) → survivor re-Joins, adopts partitions. Dead member SIGCONTs → its commit hits `coord.Commit` → memberID not in live set → UNKNOWN_MEMBER (13) → client surfaces the error (no auto-heal before surfacing, D-SL2-8) → `mk` prints it — that line IS the SD-11 receipt's money shot.

## Build order (each row done when DEMONSTRATED)

| # | Builds | Done when |
|---|--------|-----------|
| 1 | wire types 9–17 + doc-comments | roundtrip + bounded-decode tests per type green (red first) |
| 2 | coordinator core (join/rejoin/leave/death, generations, range assignment, sweeper, REJOIN bit, caps, fake clock) | unit suite green: GRP-1 exact-partition, GRP-2 bound, rejoin-bumps-nothing, level-triggered-REJOIN, false-sweep-refresh, caps |
| 3 | commits.go (commitLock→mutex order, merge-inside-lock, re-fence install, two-phase lazy load) + store.go `_groups` skip | commit unit tests + two-committer `-race` test + `_groups`-survives-boot test green |
| 4 | storage TryFetch + broker multi-entry fetch loop + lift entries>1 | wake-on-any-partition test (raw multi-entry) green under `-race`; empty-shape = one group per entry asserted; SL0 single-entry tests untouched and green |
| 5 | broker group handlers + ConnClosed glue | fencing suite over real TCP green: 12/13 precedence, zero-state-change, broker-serves-after |
| 6 | client GroupConsumer | client suite green: rejoin-and-reissue on REJOIN + on STALE mid-poll, Commit surfaces fencing, CONS-2 resume-exact |
| 7 | mk consume -g | manual smoke against live broker prints records + commits (transcript kept for the receipt) |
| 8 | e2e: GRP-3 union + takeover test; crash_test grows the single committing member | union == all-N green; harness 3 cycles green with commit journal receipt |
| 9 | receipts + scenario E live (SD-11, real mk processes) + exit sabotage + code map + BRIEF + STATUS | exit checklist 1–10 receipted |

Steps 2+3 parallel after 1; 4 independent of 2/3; 5 needs 2–4; 6 needs 5; 7–8 need 6.

## Modification recipes (what later slices touch)

- **SL3 (demo):** two GroupConsumers as goroutines + narration — client API only, zero group-internals contact.
- **SL4 (hostile/caps):** group-message malformed-input rows + idle-reclaim exemption test for heartbeating control conns — handlers/server only.
- **SL6 (PROTOCOL.md):** registry diff picks up types 9–17 + codes 12/13 from the same tables; D-SL2-2's paper-trail note feeds the doc.

## Pitfalls (named so they can be checked)

- **Lock order is commitLock→mutex, globally.** One helper that takes them reversed deadlocks under two committers — the `-race` two-committer test exists to catch exactly this; run it before calling step 3 done.
- The REJOIN bit derives from `member.generation != group.generation` — do NOT store a boolean that gets cleared on read; the review killed that (lost-flag hole, F1).
- A heartbeat from a stale-generation LIVE member must refresh lastBeat AND return the bit — fencing it (12) recreates the false-sweep bug. Only UNKNOWN member errors a heartbeat.
- `coord.ConnClosed` fires from the server's conn teardown goroutine — it takes the mutex; never call it while holding anything else.
- TryFetch must capture `p.notify` in the SAME RLock section as the read (copy Fetch's exact pattern); capturing after unlock reopens the missed-wakeup race SL0 closed.
- The park's wake chan: buffered `len(entries)`, senders use plain send (can never block at that capacity), fresh `done` every round. Reusing `done` = double-close panic in production, not in tests.
- Client control-conn mutex covers a WHOLE roundtrip (write+read) — interleaved write/write from heartbeat + commit corrupts the one-in-flight protocol.
- entries>1 lift: delete ONLY the CAP_EXCEEDED guard clause; FETCH_TOO_WIDE (>16) and all other caps stay. SL0's cap tests assert both — they must still pass minus the lifted row (edit that one test knowingly, receipt the red).
- mk `-g` ignores `-p`/`-o` (assignment comes from the group) — reject the combination loudly rather than silently ignoring.
- e2e members are goroutines/processes with real 500 ms heartbeats — keep suite runtime bounded: session timeout stays 2 s, tests budget ≤ 30 s total; the fake-clock suite is where timing-sensitive assertions live.
