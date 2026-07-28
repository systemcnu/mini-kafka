# STATUS — mini-kafka (public name, resolved at SPEC lock)

**Updated: 2026-07-24.** Project authority for this repo. Lab/process view: `~/skills/process/LAB-STATE.md`.

## Where things stand

- **SPEC: LOCKED v1.0.1** (locked 2026-07-24; v1.0.1 redaction erratum 2026-07-28, accepted at the SL0 gate). 37 active IDs. Hash: `50237dd479caf8bbf7268d92304699efaaf0ed9834646d483bf3d018e313dcfe`. Read `SPEC-BRIEF.md`.
- Locked by Sri after the 3-question quiz (two re-asks, both then answered on the brief's exact lines) and four gate decisions, recorded in SPEC §7: name stays "mini-kafka" · MIT license · durable-only benchmarks · showcase kept conditional.
- Lie-hunt record: 5 seats (4 Claude — ambiguity 10, untestability 10, gaps 9, contradiction/goal-fit 8 — plus Codex feasibility 9) = 46 findings, all integrated or gate-resolved. Orphan-ID audit clean.
- **DESIGN: LOCKED v1.0.1** (locked 2026-07-28; erratum E1 accepted at the SL0 gate — crash-walk wording, no behavior change). 26 DD rows, coverage 37/37. Hash: `5d48d8ac7b795ca4306d0cd5eca3038ef3cded4fafb5f0ce789c4b99df6eae08`. Read `DESIGN-BRIEF.md`.
- Locked by Sri after the 3-question quiz (two clean, disk-error question answered correctly on its one re-ask). Lie-hunt record: 4 seats (coverage 8, buildability 9, simplicity 10, Codex platform-reality 10) = 37 findings — all integrated, one declined with written reason (DESIGN §10).
- **SLICES: LOCKED v1.0.1** (restamp + redaction, SL0 gate). 8 slices, 24.5d, A–K owned exactly once. Hash: `273187dc7b2c9b9cf5d9a22d1aba1a25922dacba988dc9806e2d3a9f40136cd3`. Read `SLICES-BRIEF.md`.
- Locked by Sri after the 2-question quiz (clean). Gate decisions: **B1 — publish at SL0 exit (public early) · B2 — Sri's existing GitHub account.** Lie-hunt record: 4 seats = 33 findings, all integrated (v0.1's skeleton had no broker; sizing under ~1.6x).
- **SL0 (walking skeleton): PASSED at Sri's gate 2026-07-28** (verdict: pass; erratum E1 accepted; email redaction accepted; module renamed to github.com/systemcnu/mini-kafka). 51 tests green (race-checked), red-before-green receipted (216-line receipt), ack-ordering sabotage witnessed red and restored, scenario B live transcript committed, full local CI battery green, code map 84/84 anchors. Read `slices/SL0/BRIEF.md`.
- **Open partial proofs (SD-5 tracking):** LOG-1/PROD-2 half (b) kill -9 → SL1 · LOG-4 scripted-fault proofs → SL1 · CONS-1 multi-entry service → SL2 · TOP-2 doc-audit half → SL6. CI-green-on-GitHub: CLOSED 2026-07-28 (run green on 82fa448 — staticcheck caught 2 dead helpers on the first real run; fixed).
- **PUBLISHED: github.com/systemcnu/mini-kafka (public, CI green at HEAD).** Sri authorized on-their-behalf pushes for this repo (2026-07-28). **Next: SL1 (crash & disk hardening) when invoked.** Showcase feasibility: PASS receipt on file.

## Grill record

8 questions, 2026-07-24: portfolio piece · success = 60-second demo + honest benchmarks · local + optional hosted · watch-only self-driving showcase · core + consumer groups tier · four explicit OUTs · Go · $0-hard free-tier-only hosting.
