# SL1 Slice Design — Crash & disk hardening

**Status: FINALIZED (2026-07-28) after 2-seat independent review (buildability seat + different-model coverage seat) — 9 findings, all integrated, none needing adjudication. Worst catch: the draft's truncate-back rule would have bricked a partition on a frontier-write failure that lands after its rename (see D-SL1-3).**
**Derives from: SPEC v1.0.1 · DESIGN v1.0.1 · SLICES v1.0.1 (hashes in STATUS.md).** Scope authority: SLICES §2/SL1. Mechanisms: DESIGN DD rows (referenced, never restated).
**Done =** every crash and storage-failure promise proven with scripted faults and a real `kill -9` harness — plus the one mechanism SL0 deferred: DD-8's truncate-back on write failure.

## 1. Spec contract table

| ID | Treatment | Notes |
|---|---|---|
| LOG-4 | **Proofs land here** | Mechanism shipped whole in SL0 (`recovery.go`); SL0 already proves torn-tail truncate, kept-but-hidden past-frontier, missing-frontier refuse, fresh-init. This slice adds the rest of the matrix: **short-write tail truncate** (full header claiming N payload bytes, only M<N present — LOG-4's fourth mandated fault, a distinct parse branch from SL0's header-doesn't-fit tail) · CRC-corrupt tail truncate · corrupted acked region refuse · straddling-record refuse · frontier CRC-bad / beyond-length refuse · double-boot idempotence. (The exact-consume refusal branch is unreachable by construction — records tile from 0, so a mid-record frontier always trips the straddle check first; it stays in the code as defense-in-depth, tested only via the straddle message.) Closes SL0's G4. |
| LOG-5 | **Real (completed here)** | SL0 shipped reject + sticky + reads-continue. This slice adds DD-8's truncate-back-to-frontier + fsync on flush-path failure, and all proofs: seam-scripted append/short-write/fsync/frontier faults → wire code 11, sticky until restart, reads serve throughout, restart heals with no manual repair. |
| LOG-1, PROD-2 | **Half (b) — closes the SL0 partial** | The `kill -9` e2e harness: real broker process, real SIGKILL mid-load, acked-journal comparison after restart. Half (a) (sync-recorder invariant) proven at SL0 and untouched. |
| PROT-3 (write-failure row) | Proof | Wire-level test: produce onto a fault-scripted store → code 11 (`WRITE_FAILED`) + broker keeps serving fetches. Registry note: SLICES SL1 said "ERR_WRITE_FAILED added here" — overtaken: code 11 shipped in SL0's registry (D-SL0-8). No new codes this slice. |
| TOP-1 (crash half) | Proof | Boot cleanup of meta-less dirs proven at SL0; this slice adds the mid-create failure test (scripted fault inside CreateTopic → abort cleanup, no half-topic survives). |
| Scenario C | **Partial: harness here** | Produce-only kill -9 cycles (positions/commits don't exist yet). Full scenario C with group commits → SL2, reusing this harness. |
| Scenario J | **Owned here** | Live demo on a quota-limited volume (receipt) + the scripted ENOSPC tests as the cross-platform CI check. |
| All other IDs | Untouched | No wire, protocol, client, or coordinator changes in this slice. |

## 2. Slice-local decisions

| # | Decision |
|---|---|
| D-SL1-1 | **Two fault classes, deliberately distinct.** (a) *API failures* (write error, short write, sync error, atomicWrite failure, truncate failure) are scripted at the seam by fault wrappers around the real `osFS` in a `t.TempDir()`, exported from a tiny **`internal/storage/storagetest`** package (`FaultFS`/`FaultFile`/`FaultSyncer`) — a real package, because the broker's wire test (D-SL1-4) lives in another package and `_test.go` identifiers are never importable; "test-only" is kept true by package placement and a doc header. The fakes honor the seam's post-state contracts: a failed `WriteFileAtomic` leaves the target **old or new, never torn — unspecified which** (the coded recipe can fail AFTER its rename, on the dir-fsync — so no test and no production path may assume old-intact), and a short write reports n<len with the file really short. (b) *Crash damage* (torn/short/CRC-corrupt/straddling records, stale or corrupt frontier) is staged by writing real bytes into real files with helpers built on `encodeRecord`/`encodeFrontier` — corruption is a disk state, not an API result. Fakes never simulate what they can stage for real. |
| D-SL1-2 | **FaultFS scripting model — small, not a framework.** Each scriptable op takes "fail the Nth matching call for paths ending in X with error E" (plus a short-write byte count for `File.Write`). Scriptable ops: `File.Write`, `File.Sync` (direct — covers CreateTopic's file fsyncs and the truncate-back repair's sync, which never pass through the Syncer seam), `Syncer.Sync` (via `FaultSyncer` — the flusher's path), `FS.WriteFileAtomic`, `FS.Truncate`, `FS.SyncDir`, `FS.OpenAppend`. ENOSPC scripts use `syscall.ENOSPC` for realism; the broker maps ANY flush-path error to code 11, so no errors.Is contract is added to the seam. |
| D-SL1-3 | **DD-8 truncate-back mechanics — scoped by what the on-disk frontier provably is.** On a `File.Write` (incl. short write) or `Syncer.Sync` failure: mark failed FIRST (sticky — stops new appends racing), then best-effort repair: `fsys.Truncate(log, frontier)` + open-a-fresh-handle → `Sync` → close (recovery.go's make-truncation-durable pattern) — sound because on these branches the on-disk frontier still equals the in-memory one. **On a frontier `WriteFileAtomic` failure: mark failed only, NO truncate** — the write may have failed after its rename installed the NEW frontier (dir-fsync step), and truncating to the old value would leave frontier > log length → boot refusal → manual repair, violating LOG-5. Untouched, that state is exactly crash-walk row 2/3 territory: log valid and fsynced, frontier old or new, restart heals either way. If the repair itself fails: accept and leave it — reads are frontier-capped (DD-5) so nothing torn is servable, and restart's scan re-truncates. In-memory index/frontier need no rollback: `flush` only installs metas after full success. `fileSize` goes stale — unused once failed (write-rejecting until restart, no runtime re-probe). |
| D-SL1-4 | **Broker over an injectable FS.** The server gains a construction path taking `(storage.FS, Syncer)` with `osFS` default — the tiny seam DESIGN §9 already promises ("storage file interface with scripted faults… scenarios C/J") — so the PROT-3 wire test can script a produce failure through a real listening broker. No flag, no runtime configurability: test-constructor only. |
| D-SL1-5 | **kill -9 harness** (`internal/e2e/crash_test.go`): build `cmd/minikafka` once per test binary run; loop 3 cycles on ONE data dir: start broker → 4 producer goroutines produce sequenced payloads `<producer>-<seq>` round-robin over 1 topic × 2 partitions, journaling every ack (partition, offset, payload) in harness memory (the harness survives; the broker dies) → SIGKILL after a random 50–250 ms under load → restart → fetch every partition to tail. **Assertions are one-directional by design:** every journaled ack appears in the fetched set at its exact offset with its exact payload (sound: ack ⇒ frontier-covered ⇒ refuse-protected); the fetched set MAY contain unjournaled records — acks the kill outran, and kept-but-hidden records a later flush surfaced — each sanity-checked to match the `<producer>-<seq>` shape; density is asserted of the FETCHED offsets only (dense by recovery construction), never of the journal. Post-restart produces must still succeed. Real OS, real process, no fakes — that is what makes it half (b) by construction. Runtime budget ≤ 20 s; runs in the normal `go test ./...` battery. |
| D-SL1-6 | **Harness gets the port from the broker's own log line.** Broker starts with `--addr 127.0.0.1:0`; the harness parses the resolved address from the startup line `cmd/minikafka` already prints (confirmed in review — it goes to **stderr**, so the harness scans stderr). No production change, no fixed ports, no reserve-and-release races. |
| D-SL1-7 | **Scenario J live procedure (macOS, receipted):** `hdiutil` creates a small APFS image (size recorded in the receipt) → attach → real broker `--data` on it → **create the topic and produce a few baseline records FIRST** (topic creation needs free space; it must precede the filler, and it guarantees the consume step has records to serve) → a filler file (`dd`) eats the volume until nearly full → produce until `WRITE_FAILED` surfaces → consume still serves every pre-failure record → `rm` the filler (that's "freeing space" — the logs themselves are never touched) → restart broker → healthy: old records served, new produces accepted, zero manual repair. Transcript saved as `docs/receipts/sl1-scenario-j.txt`. CI's cross-platform check is the scripted ENOSPC test; the live run is the demo. |
| D-SL1-8 | **Sabotage target at exit:** flip recovery's acked-damage refusal into a silent truncate, witness the corrupted-acked-region test go red with my own eyes, restore, battery green. Proves the scariest new test can actually fail. |

## 3. Crash-walk rows → named tests (DESIGN §7, storage rows)

| Killed mid… | Proven by |
|---|---|
| append, pre-fsync | `TestRecoveryTruncatesInvalidTailPastFrontier` (SL0) + the new short-write tail test (LOG-4's fourth fault) |
| post-fsync(log), pre-frontier | `TestRecoveryKeepsValidUnackedRecordsPastFrontier` (SL0, the E1 row) |
| frontier atomicWrite | fault-scripted `WriteFileAtomic` failure test asserting the D-SL1-1 post-state (old or new, never torn) + kept-but-hidden covers the stale-frontier restart |
| post-frontier, pre-ack | **no fault to inject** — record fully durable at/below frontier; the duplicate-on-retry consequence (R2) is exactly the harness's tolerated fetched-but-unjournaled class (D-SL1-5) |
| topic create | `TestBootRemovesTopicDirWithoutMeta` (SL0) + new mid-create fault-abort test |
| boot recovery / runtime truncate | new double-boot idempotence test (same decisions, byte-identical state) |
| any point, for real | the kill -9 harness (D-SL1-5) |

(§7's commit-write and rebalance rows are SL2's — group state doesn't exist yet.)

## 4. Known gaps accepted

- **G-SL1-1** — kill -9 proves process-crash durability; power-loss is qualified by DD-7's platform limit (macOS fsync ≠ drive-cache flush). Permanent documented limit, already in README — not closable in software.
- **G-SL1-2** — scenario J's live receipt is macOS-local; Linux exercises the same claim via the scripted ENOSPC tests in CI. Accepted.
- **G-SL1-3** — scenario C's "group positions intact" half needs SL2's commits; the harness ships here produce-only (owner: SL2).
- **G-SL1-4** — kill points are time-randomized, not exhaustively scheduled: bounded iterations of realism, while the deterministic branch coverage lives in the scripted recovery tests. The two halves are complementary by design.

## 5. Test plan mapped to claims

- **Recovery matrix** (extends `recovery_test.go`): **short-write tail** (full header claiming N bytes, M<N present, past frontier) → truncated to frontier · CRC-corrupt tail → truncated to frontier · corrupted acked byte → refuse naming partition+byte · record straddling frontier → refuse (also the sole reachable witness of the exact-consume guard, per §1) · frontier CRC-bad → refuse · frontier beyond log → refuse · double-boot idempotence. Each refusal asserts the error NAMES the partition (LOG-4's "refuses loudly").
- **Degrade suite** (new `degrade_test.go` in storage): append write error / short write / sync error → `ErrWriteRejected` to the producer, sticky across subsequent appends, reads keep serving durable records, log truncated back to frontier (or safely not, when the repair's Truncate/Sync are also scripted to fail) · frontier-write error → sticky reject, **log NOT truncated** (D-SL1-3's scoping) · every case: restart on the SAME dir with the real FS → healthy.
- **Wire-level LOG-5/PROT-3** (broker test via D-SL1-4): scripted store failure → produce gets code 11, subsequent fetch on the same broker serves.
- **Topic-create abort** (store test): scripted fault mid-create → error out, no half-topic in listing, dir gone (or meta-less and removed on next boot).
- **e2e** (D-SL1-5): the kill -9 harness.

## 6. Validate — exit checklist (all demonstrated, not asserted)

1. Red-before-green record for every new test file (receipt).
2. Recovery matrix green — including the short-write tail test (LOG-4's fourth fault) — and each refusal message carries the partition path.
3. Degrade suite green, including truncate-back-fails-too and frontier-write-failure-does-NOT-truncate cases.
4. Wire test: code 11 + broker-still-serves green.
5. kill -9 harness: 3 cycles green, run receipt committed. (`-race` instruments the harness/test binary; the broker child is a plain build — its race coverage comes from the in-process battery, stated honestly here.)
6. Scenario J live transcript receipt committed.
7. Sabotage (D-SL1-8) witnessed red, restored, full battery green.
8. `scripts/checks.sh` fully green locally; CI green on GitHub after push.
9. `slices/SL1/BRIEF.md` (baked diagram) + code map regenerated + STATUS/LAB-STATE + commits.

---
Review record: buildability seat 7 findings (worst: the frontier-write truncate-back brick, D-SL1-3; also cross-package fake placement, one-directional harness assertions, unreachable exact-consume branch, un-seamed Sync sites, -race scope honesty, scenario J ordering) · different-model coverage seat 2 findings (short-write test missing from the authoritative lists; crash-walk row silently dropped) + six "already-proven-at-SL0" claims independently verified true. All 9 integrated above; no adjudication items.
