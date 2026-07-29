# SL3 Slice Design — The demo

**Status: DRAFT for review.**
**Derives from: SPEC v1.0.1 · DESIGN v1.0.1 · SLICES v1.0.1 (hashes in STATUS.md).** Scope authority: SLICES §2/SL3. Mechanisms: DD-20 (topology), DD-21 (external clock), D8 (Go-native visitor path), referenced never restated.
**Done =** scenario A real: a visitor types `go run ./cmd/demo`, sees messages flow inside 60 s from a cold cache, watches a consumer die and the survivor take over, all inside 3 minutes — measured by a clock the demo cannot see, gated in CI.

## 1. Spec contract table

| ID | Treatment | Notes |
|---|---|---|
| DEMO-1 | Real | `cmd/demo`, Go-native (D8: no make/shell in the visitor path). CI clean-container run gates first-flow ≤ 60 s; receipt commits the measured time; macOS approximated by a clean-cache local run (the SPEC's own split). |
| DEMO-2 | Real | Act two kills a consumer, narrates takeover; total ≤ 180 s gated in the same CI job. |
| Scenario A | **Owned here** | Run locally + the colder CI run; both receipted. |
| PROT-2/CONS-1 (reuse) | Proof-by-construction | The demo's producers/consumers ARE the shipped client over long-poll fetch — no privileged path. |
| OPS-3 (top screen) | Completed for the demo | README top screen carries the one command + what the visitor will see. |
| BENCH-*, SHOW-* | Untouched | SL5/SL7. |

## 2. Slice-local decisions

| # | Decision |
|---|---|
| D-SL3-1 | **Topology (DD-20 realized).** One process: broker in-process on `127.0.0.1:0` (ephemeral — a second copy of the demo collides with nothing), data dir `os.MkdirTemp` cleaned on exit. Topic `demo`, 4 partitions. One producer goroutine (shipped `client.Producer`, ~20 msg/s, payloads `msg-<n>`) + two `client.GroupConsumer`s ("consumer-1", "consumer-2") — all over real TCP. Act one: create topic, start everyone, narrate both consumers' ownership and first records; `#event first-flow` printed at the FIRST record any consumer receives. Act two: after ~5 s of visible flow, consumer-2 is killed ungracefully; the takeover is narrated (consumer-1's re-join and new 4-partition ownership, then records flowing from all 4); `#event done` printed at exit. Target in-process runtime ≈ 15 s — the 60 s budget belongs to `go run`'s cold compile, not to us. |
| D-SL3-2 | **The kill is `GroupConsumer.Abandon()` — a small PUBLIC client addition.** DD-20 pins the act-two kill as "connections hard-dropped, heartbeats stop (SIGKILL-equivalent to the broker)". A goroutine cannot be SIGKILLed; `Close()` sends a polite LeaveGroup (wrong story — that's resignation, not death). `Abandon()` closes both conns raw, sends nothing, returns nothing. To the broker it is indistinguishable from a process kill (conn-drop → immediate death, DD-10). Public because it is the honest "ungraceful shutdown" primitive (docs say exactly that); demo and future tests share it. Detection is via conn-drop (immediate), same as a real SIGKILL — the 2 s heartbeat-silence path stays proven by SL2's SIGSTOP receipt, and the demo does not re-prove it. |
| D-SL3-3 | **Narration discipline: legible, not a firehose.** Per consumer: one line per OWNERSHIP change ("consumer-1 now owns partitions 0,1") and one line per first-record-from-a-partition; afterwards a per-second aggregate ("consumer-1: 41 msgs from partitions 0,1"). Act headers (`— act one —`, `— act two —`) and the two `#event` lines are exact, greppable, on their own lines. `-ci` flag: identical behavior, no terminal frills (no behavioral fork — the CI run must measure the visitor's demo, D8). |
| D-SL3-4 | **External clock = `scripts/demo_timing.sh`, used by CI and by the macOS local leg (shell is harness, never visitor path — DD-21's own carve-out).** The script: make fresh temp `GOCACHE`/`GOMODCACHE` (never touching the caller's real caches) → note t0 → run `go run ./cmd/demo -ci`, timestamping every output line as it streams → compute first-flow = t(`#event first-flow`) − t0 and total = t(`#event done`) − t0 → **gate: first-flow ≤ 60, total ≤ 180, non-zero exit on breach** → emit a receipt block (times, `go version`, OS identity, commit). Timestamps at 1 s resolution (a 60 s gate does not need milliseconds). |
| D-SL3-5 | **CI job `demo-timing` (DD-21).** Container `golang:1.24` on ubuntu; fresh checkout; runs the script. Image identity recorded honestly: the job records the image TAG from the workflow plus `go version` + `/etc/os-release` from inside — a container job cannot see its own image digest; the receipt says so instead of pretending (DD-26's record-the-resolved-image rule, done with what is actually observable). The job prints the receipt and uploads it as a build artifact. |
| D-SL3-6 | **The committed receipt = one REAL CI run's output, copied into the repo at this slice's exit.** CI gates every push forever but cannot commit to the repo; DD-21's "receipt committed as docs/receipts/demo-timing.txt" is satisfied by capturing the artifact of an actual run of this slice's CI (named commit inside) and committing it by hand — same pattern the benches will use (BENCH-3). The macOS leg (`demo-timing-macos.txt`) comes from running the same script locally. Staleness rule: the committed receipts name their commit; they are refreshed only when a slice materially changes the demo's startup path (recorded as a convention in the receipt header). |
| D-SL3-7 | **README top screen.** The first screen: what this is (2 lines), the ONE command (`git clone … && cd mini-kafka && go run ./cmd/demo`), what the visitor will see (3 bullet lines incl. both acts + timings), and the honesty pointers (limitations section stays; bench numbers arrive at SL5). No badges beyond the existing CI badge. |

## 3. Known gaps accepted

- **G-SL3-1** — the CI container's image digest is not observable from inside; the receipt records tag + go version + os-release instead (stated in the receipt itself).
- **G-SL3-2** — demo timing on macOS is a local clean-cache approximation, per DEMO-1's own check text; CI's macOS job stays build+smoke (SPEC's split, DD-21).
- **G-SL3-3** — the demo does not re-prove the 2 s heartbeat-silence detection (conn-drop death is instant); SL2's SIGSTOP receipt owns that proof. The demo narrates takeover, not deadline mechanics.
- **G-SL3-4** — committed timing receipts go stale by design between refreshes; each names the commit it measured (D-SL3-6).

## 4. Test plan mapped to claims

- **Demo smoke test** (`cmd/demo` gets a real test): run the demo binary (built, not `go run` — the test asserts demo BEHAVIOR; the compile-time half belongs to the timing harness) with a short `-flow` duration override (hidden flag, default matches the visitor build — reviewers: attack this), assert the transcript contains, in order: act one header · `#event first-flow` · both consumers' ownership lines · act two header · a takeover line showing consumer-1 owning all 4 · `#event done`; exit 0; temp dir removed.
- **Timing-script test:** feed the script a FAKE demo (a tiny stdin script emitting the `#event` lines at controlled delays — shell-level, test-only) and assert: passes under the gates, fails (non-zero) when first-flow is late, fails when total is late. The gate must be seen RED before trusted.
- **`Abandon()` unit:** after Abandon, the broker rebalances (survivor owns all) and the abandoned member's later commit gets 13 — a compressed client-level re-proof reusing SL2's patterns.
- **Live legs (exit):** scenario A run locally (transcript receipt) · macOS clean-cache timed run (receipt) · the CI job's first real run green with its artifact captured into the repo.

## 5. Validate — exit checklist (all demonstrated, not asserted)

1. Red-before-green for the demo smoke test, the timing-script gates (both breach directions), and the Abandon test.
2. `go run ./cmd/demo` locally: full two-act transcript receipt (`docs/receipts/sl3-scenario-a.txt`).
3. macOS clean-cache timed run via the script: `docs/receipts/demo-timing-macos.txt` with measured first-flow and total.
4. Push → CI `demo-timing` job green; its receipt captured as `docs/receipts/demo-timing.txt` (names the measured commit).
5. Exit sabotage (verifier's hands): break the demo's first-flow marker (or delay it past the gate in the fake-demo test) → the gate goes red → restore → green.
6. README top screen per D-SL3-7; `scripts/checks.sh` fully green; code map + BRIEF + STATUS/LAB-STATE + commits.

---
**DRAFT for review** — attack surface: D-SL3-2's public Abandon (right call vs test-only?), D-SL3-4's 1 s resolution and gate mechanics, D-SL3-6's committed-receipt staleness convention, the hidden `-flow` override in §4 (does it fork visitor behavior?), and whether the smoke test's assertions pin ENOUGH of the transcript.
