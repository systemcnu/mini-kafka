# SL0 Slice Design — Walking skeleton

**Status: FINALIZED (2026-07-28) after 2-seat review — 16 findings, 15 integrated, 1 declined (B1/B2 were resolved at the SLICES lock; the seat read the table row's stale conditional).**
**Derives from: SPEC v1.0 · DESIGN v1.0 · SLICES v1.0 (hashes in STATUS.md).** Scope authority: SLICES §2/SL0. Mechanisms: DESIGN DD rows (referenced, never restated).

## 1. Spec contract table

| ID | Treatment | Notes |
|---|---|---|
| LOG-1, PROD-2 | **Real mechanism, partial proof** | Full ack path (DD-4/6). Proof (a) sync-recorder here; (b) kill -9 → SL1 (G3). |
| LOG-2, LOG-3, PROD-1 | Real | Tests here. |
| LOG-4 | **Mechanism real, proofs deferred** | The FULL DD-4 boot scan ships here as code (all three branches: valid parse to frontier · refuse on acked-damage/unreadable-frontier · truncate past frontier). SL1 owns the scripted-fault PROOFS + kill -9 harness (G4). SLICES SL1's "refusal rules" line = its proof tests of this code. |
| LOG-5 | **Minimal mechanism, hardening deferred** | On append/fsync error at SL0: fail the produce with WRITE_FAILED, mark partition write-rejecting, reads continue. SL1 adds truncate-back + all proofs. |
| TOP-1 | Real | Atomic create (DD-9) + names (DD-18); duplicate → TOPIC_EXISTS. Tested here (exit item 4). |
| TOP-2 | Real (test half) | Stability test here; doc-audit half → SL6 (G6). |
| CONS-1 | **Real, partial proof** | Wire form is multi-entry; SL0 serves entryCount=1 with park/wake; entryCount>1 → CAP_EXCEEDED("multi-entry arrives with groups") until SL2 lifts it (G7). Single-partition wake proven here; multi-partition → SL2. |
| PROD-3, NFR-2 | Real (live subset) | ALL inputs live at SL0 are capped (D-SL0-8): payload, request+response frames, name length/format, fetch maxWait/maxBytes/entries, topics, partitions/topic, connections. Rejection tests here (exit item 4); SL4 audits the full inventory + adds idle reclaim & served-error-frame polish. |
| PROT-3 | Minimal | Envelope + 11 codes with pinned values (D-SL0-8); registry grows per slice (SD-8); audit → SL4. |
| NFR-4 | Real | Loopback default + pinned test method (D-SL0-12). |
| OPS-1/OPS-2 | Minimal | Build commands + ci.yml authored; every CI command also runs locally via `scripts/checks.sh` (+ GOOS=linux cross-compile as the local Linux proxy — explicitly a superset of CI, which builds Linux natively). Green-on-GitHub provable only after the publish step (G2). |
| OPS-3 | Real (floor) | LICENSE (MIT) + truthful minimal README, top-screen commands. |
| NFR-1, NFR-3 | Real | go-list audit script; package headers; vet + staticcheck. |
| CONS-2/3, GRP-1..5 | Deferred → SL2 | Conn-role primitives ready (DD-24/25). |
| DEMO, BENCH, SHOW, PROT-1/2 final | Deferred → SL3/SL5/SL7/SL6 | SHOW feasibility check: DONE at this exit (receipt committed, PASS). |

## 2. Slice-local decisions

| # | Decision |
|---|---|
| D-SL0-1 | Module path `mini-kafka` until the publish moment at this exit, then renamed to `github.com/<Sri's-account>/mini-kafka` in the publish commit (B2: existing account, resolved at SLICES lock). |
| D-SL0-2 | **Frame:** `[u32 len][u8 ver][u8 type][payload]`; **len covers ver+type+payload** (everything after len). Min len 2. `ver != 1` → Error(MALFORMED,"unsupported version") then close. Unknown type → Error(MALFORMED,"unknown type") then close. Partial frame at EOF → close (debug log only). Strings `[u16 n][bytes]`, blobs `[u32 n][bytes]`, big-endian. Types: 1 Produce · 2 ProduceResp · 3 Fetch · 4 FetchResp · 5 CreateTopic · 6 CreateTopicResp · 7 ListTopics · 8 ListTopicsResp · 255 Error. |
| D-SL0-3 | **Message bodies:** Produce `[str topic][u32 partition][blob payload]` → ProduceResp `[u64 offset]`. CreateTopic `[str topic][u32 partitions]` → CreateTopicResp empty. ListTopics empty → ListTopicsResp `[u32 n]{[str name][u32 partitions]}`. Fetch `[str topic][u32 nEntries]{[u32 partition][u64 offset]}[u32 maxWaitMs][u32 maxBytes]` → FetchResp `[u32 nGroups]{[u32 partition][u32 nRecs]{[u64 offset][blob payload]}}` — one group per requested partition, zero-rec groups legal (that IS the empty-at-timeout shape). All entries validated up front; any invalid → whole-frame Error, nothing served. Error `[u16 code][str msg]`. |
| D-SL0-4 | **FS seam** (`storage.FS`): OpenAppend (O_CREATE; returns File{Write, ReadAt, Sync, Close, Size}), OpenRead, ReadFile, WriteFileAtomic (DD-9 recipe inside — opaque; SL1 scripts its FAILURE POST-STATES, not steps: that is the contract), Truncate, Rename, MkdirAll, RemoveAll, ReadDir, SyncDir. **Error contract: not-found conditions satisfy `errors.Is(err, fs.ErrNotExist)`** — the fresh-vs-refuse boundary (DD-4/9) depends on it and SL1's fakes must honor it. Real impl `osFS`. |
| D-SL0-5 | Per-partition flusher goroutine: waiter queue, 5 ms oldest-waiter trigger, fsync via `Syncer` (implementations MAY BLOCK — the test gate is legal), then frontier atomicWrite, then acks. **Atomicity pin: frontier value update + old-notify-channel close + new-channel install happen under the partition write lock; a fetch's tail-check + channel-capture happen under the read lock** — the missed-wakeup proof depends on exactly this. Test seams: `AdvanceHook(partition, frontier)` fired post-advance + a parked-waiter count observable (DESIGN §9's notification hook — the wake test needs both). |
| D-SL0-6 | **Graceful stop (SIGINT/SIGTERM), in order:** (1) stop accepting; (2) draining flag on — new requests on open conns get Error(SHUTTING_DOWN); (3) close the global stopping channel — every parked fetch selects on it and returns its empty-at-timeout shape; (4) wait ≤5 s for the in-flight produce waiter queues to drain (their acks complete); (5) signal flusher goroutines, join them; (6) close storage files; (7) close conns. Produce arriving during drain = case (2), not case (4) — drain is a snapshot of already-queued waiters, bounded, never quiescence-chasing. |
| D-SL0-7 | Broker flags: `--addr` (default 127.0.0.1:7621), `--data` (default ./data). `mk` subcommands: `create-topic -t -p` · `topics` · `produce -t -p [-m msg \| stdin lines]` · `consume -t -p [-o offset, default 0] [-f]`; consume prints `offset<TAB>payload` per record (the scenario-B transcript proves "same offsets" from this output) and, without `-f`, exits at the tail; with `-f`, long-polls. All commands take `-addr`. |
| D-SL0-8 | **Live caps + pinned code values (u16):** 1 UNKNOWN_TOPIC · 2 TOPIC_EXISTS · 3 BAD_PARTITION · 4 INVALID_NAME · 5 MSG_TOO_LARGE · 6 FRAME_TOO_LARGE · 7 MALFORMED · 8 CAP_EXCEEDED · 9 FETCH_TOO_WIDE · 10 SHUTTING_DOWN · 11 WRITE_FAILED. Caps live now: payload ≤1 MiB · request frame ≤1 MiB+4 KiB · response frame ≤4 MiB+64 KiB · name ≤128 + `^[a-z0-9][a-z0-9._-]{0,127}$` · fetch maxWait ≤30 s (0 → default 5 s; over-cap → CAP_EXCEEDED) · maxBytes ≤4 MiB (0 → 1 MiB) · entries ≤16 (>1 → CAP_EXCEEDED at SL0, G7; >16 → FETCH_TOO_WIDE) · topics ≤64 · partitions/topic 1..16 · connections ≤256 (accept-guard close; the served-error-frame + idle reclaim polish → SL4). SL2 adds 12 STALE_GENERATION · 13 UNKNOWN_MEMBER. |
| D-SL0-9 | CI `ci.yml` jobs: test (ubuntu, Go pinned) · vet+staticcheck (pinned release) · stdlib-audit · build+smoke (macos: build broker+mk, 5 s produce/consume smoke). `scripts/checks.sh` = the same commands + `GOOS=linux go build ./...` as the local Linux proxy. |
| D-SL0-10 | Go 1.24 pinned (go.mod + CI). |
| D-SL0-11 | Feasibility check (SL7-pre): DONE — `docs/receipts/showcase-feasibility.md`, verdict PASS, kill criterion re-armed at SL7's account-creation step. |
| D-SL0-12 | **NFR-4 test method:** assert the listener's resolved address is loopback under default flags, AND for every non-loopback `net.InterfaceAddrs` address present, a dial to that address:port is refused; if the machine has none, skip that leg with a logged note. |

## 3. Known gaps accepted (each owned)

- **G1** — module path placeholder until the publish commit (owner: this exit, with Sri).
- **G2** — CI green **on GitHub** unprovable pre-publish; `scripts/checks.sh` receipt is the proxy (owner: this exit; STATUS tracks until the badge is real).
- **G3** — LOG-1/PROD-2 kill -9 half (owner: SL1).
- **G4** — LOG-4 recovery code unproven against scripted faults (owner: SL1).
- **G5** — Linux native execution only in CI (owner: G2's resolution).
- **G6** — TOP-2 doc-audit half (owner: SL6).
- **G7** — CONS-1 multi-entry service + multi-partition wake (owner: SL2; the wire shape is final now, only the behavior staged).

## 4. Exit checklist (all demonstrated, not asserted)

1. Red-before-green record for every new test file.
2. LOG-1(a) ack-ordering recorder test green; sabotage (ack before sync) witnessed red, restored.
3. Wake test via AdvanceHook + parked-count + blocking Syncer gate: parked fetch wakes on frontier advance, NOT on raw append.
4. Tests green: LOG-2 (contiguous offsets across restart) · LOG-3 (order) · PROD-1 (roundtrip) · TOP-1 (create/list/duplicate→TOPIC_EXISTS, atomic create) · TOP-2 (partition count stable across restart) · name validation (INVALID_NAME table) · PROD-3 (oversized→MSG_TOO_LARGE, nothing written) · live-cap rejections (frame, maxWait, entries>1, entries>16, partitions, topics, conn guard) · NFR-4 per D-SL0-12 · boot-scan branch tests (happy path; scripted-fault branches red-listed for SL1).
5. Scenario B live via `mk` against the real binary — transcript receipt with offsets visible.
6. `scripts/checks.sh` fully green locally.
7. LICENSE + README present and truthful for today.
8. Feasibility receipt committed (done).
9. Publish: module rename + Sri pushes to their existing account (B1/B2 resolved at SLICES lock); CI badge verified after.
10. `slices/SL0/BRIEF.md` (baked diagram) + code map + STATUS/LAB-STATE + commits.
