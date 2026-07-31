# SL6 Implementation Plan

**Derives from: slices/SL6/DESIGN.md FINALIZED 2026-07-31.** Any design change patches this plan in the same change. Contracts live in D-SL6-1..10 and the ledger; this plan owns where things live, the build order, and the anchor map the PROTOCOL.md prose is written against. Scale: 1 new doc (~a full day of writing, SD-9), 1 test file, CI edits, README edits outside markers, 2 receipts. **ZERO production-code changes** (D-SL6-10) — if the build surfaces one, STOP; it needs its own ledger row first.

Counts used below were derived by command on the tree this plan was written against — re-run them, never trust memory:

```sh
grep -cE '^\tCode[A-Za-z]+ +Code = [0-9]+$' internal/wire/errors.go   # 13 codes
grep -cE '^\tType[A-Za-z]+ +byte = [0-9]+$' internal/wire/frame.go    # 18 types (1..17 + 255)
grep -cE '^  [a-z-]+:$' .github/workflows/ci.yml                      # 5 jobs today -> 6 after row 6
```

## Codebase map (delta only)

```
docs/PROTOCOL.md                   NEW: the stranger-first protocol doc (D-SL6-1),
                                   outline + anchors in §P below; two marker-bounded
                                   registry tables under the D-SL6-2 grammar
docs/diagrams/*.png                NEW: bake output for the ONE mermaid sequence (D-SL6-9)
internal/wire/protocoldoc_test.go  NEW: registry-diff test, legs A-D (D-SL6-3);
                                   package wire, test-only, reads ../../docs/PROTOCOL.md
docs/receipts/sl6-red-green.txt    NEW (builder): the two pinned reds + the green (§Red)
docs/receipts/sl6-audits.txt       NEW: D-SL6-8 command outputs (row 7; integrator
                                   refreshes the push-dependent lines at exit)
README.md                          EDIT outside the bench markers ONLY (D-SL6-6 a-f);
                                   lines 80-123 (<!-- bench:begin/end -->) are NEVER touched
.github/workflows/ci.yml           EDIT: sixth job protocol-doc + echo lines into
                                   vet-staticcheck and stdlib-audit + build-smoke matrix
```

**Where do I look for X?** the table grammar contract → the in-situ HTML comment inside each begin marker (D-SL6-2) · why the test fails hard instead of skipping → protocoldoc_test.go top comment (contrast with the bench bootstrap rule) · the go/parser precedent to extend → internal/wire/errors_test.go:17-50 (`declaredCodes`) · the one fsync wording authority → cmd/bench/report.go:30-35 (`reportCaveats`, third entry) · every behavioral claim's source → the §P anchor map below.

Exit process (STATUS, BRIEF, code-map regen, commits, push, scenario-K receipt, gate sabotage) is SD-6's — **no build rows for it here**.

## Build order (each row done when DEMONSTRATED; every row ≤ half a day)

| # | Builds | Verify (command) |
|---|--------|------------------|
| 1 | `internal/wire/protocoldoc_test.go` complete: legs A–D + the strict D-SL6-2 grammar parser (spec in §G below). Missing file or missing markers = `t.Fatalf` naming what is absent — NEVER `t.Skip` | `go test ./internal/wire -run TestProtocolDoc -count=1` → **RED 1**, the named missing-doc failure; capture per §Red |
| 2 | PROTOCOL.md wire reference (§P1–§P6, §P10): envelope + ASCII byte-layout fence, encoding primitives, BOTH marker-bounded registry tables (grammar comments in situ), per-message field tables, name rule, limits table | mid-row, with markers + headers but tables incomplete: focused test → **RED 2** (count-inequality naming the shortfall); capture per §Red. End of row: focused test output shows ONLY leg D still failing (tables parse clean, sets ≡ registries) |
| 3 | PROTOCOL.md behavior prose (§P7–§P9, §P11–§P13): connection lifecycle, fetch/long-poll incl. the min-one formula + read-deadline rule, groups incl. 12-vs-13 + parked disposition, durability with the verbatim fsync sentence, normative-vs-informative, "does NOT have" + no-resize line, both D-SL6-7 notes | `go test ./internal/wire -run TestProtocolDoc -count=1` → **GREEN**; capture per §Red · section-presence + pinned-phrase grep battery (§P checks) all pass |
| 4 | Diagrams (D-SL6-9): ONE mermaid group-lifecycle sequenceDiagram (join → assignment → heartbeat/REJOIN → re-join adopts generation → fenced late commit) added to §P9; render-check then bake. Bake AFTER prose is final (rows 2–3 done) | `npx @mermaid-js/mermaid-cli -i /tmp/gl.mmd -o /tmp/gl.svg` renders · `python3 ~/.claude/templates/bake_mermaid.py docs/PROTOCOL.md` · `grep -rn '^\`\`\`mermaid' docs/PROTOCOL.md README.md` → 0 · `find docs/diagrams -name '*.png' -size +0c` non-empty · focused test STILL green after bake |
| 5 | README pass (D-SL6-6 a–f, spec in §R): fsync prose swap (verbatim), R2 sentence, `--addr` port warning on the loopback sentence, PROTOCOL.md link; top screen and lines 80–123 untouched | §R grep battery (all four greps) · `go test ./cmd/bench -count=1` green — `TestReadmeBenchSectionMatchesCommittedReport` (render_test.go:131) proves the marker section byte-identical |
| 6 | ci.yml (D-SL6-4 + ledger row 6, spec in §C): sixth job `protocol-doc`, echo line into vet-staticcheck AND stdlib-audit, build-smoke → `strategy.matrix.os [ubuntu-latest, macos-latest]` | `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` clean · job-count grep → 6 · `grep -c 'runner image='` → 5 · local equivalent of the new job: the focused test command, green |
| 7 | Audits (D-SL6-8, exact commands in §A) → `docs/receipts/sl6-audits.txt`; push-dependent lines (OPS-1 matrix green, `gh run list` at the new HEAD) marked for integrator refresh at exit | `cat docs/receipts/sl6-audits.txt` shows every §A command + output; NFR-3 line ends `ALL-DOCUMENTED` |
| 8 | Full suite + local sabotage: all SIX sabotage rows run locally (mutate → focused test RED → restore → GREEN), redaction check | `go test ./... -race -count=1` green, count reported by the command · `bash scripts/checks.sh` → ALL CHECKS GREEN · six red/green pairs observed (§S) · redaction grep per §S |

1 strictly first (red-before-green is structural: the test exists before any table it diffs). 2–5 in order (each consumes the prior; the bake in 4 must follow final prose; the README link in 5 needs the doc to exist). 6–8 in order after the doc is stable.

## §Red — exactly which reds go into docs/receipts/sl6-red-green.txt

Append each focused-test run verbatim (`go test ./internal/wire -run TestProtocolDoc -count=1 2>&1 | tee -a docs/receipts/sl6-red-green.txt`):

1. **RED 1 (end of row 1):** `docs/PROTOCOL.md` does not exist yet — the failure must NAME the missing doc (the test's own `t.Fatalf("reading ../../docs/PROTOCOL.md: %v", err)`), proving absence is a hard fail, not a skip.
2. **RED 2 (mid-row 2):** markers + grammar comment + header/separator rows are in place but data rows are incomplete — the failure must NAME the count inequality (e.g. `codes table has 4 rows, registry has 13`), proving the count-equality leg bites.
3. **GREEN (end of row 3):** the same command passing — tables ≡ registries, all prose cells non-empty, leg D satisfied.

## §G — the diff test, pinned (D-SL6-2/3; builder implements exactly this)

- **Location/pattern:** package `wire`, file `internal/wire/protocoldoc_test.go`; doc path `../../docs/PROTOCOL.md` (package dir is cwd under `go test` — ledger row 10). Extend the `declaredCodes` precedent at internal/wire/errors_test.go:17-50.
- **Grammar (both tables):** between `<!-- registry:codes:begin -->`/`end` and `<!-- registry:types:begin -->`/`end`, the ONLY legal line classes: blank · the in-situ grammar HTML comment · exactly one header row · exactly one separator row · data rows whose FIRST cell parses as base-10 int. Any other line = hard failure. Cells split on UNESCAPED `|` only (a `\|` is a literal pipe inside a cell — split, then unescape); leading/trailing pipes trimmed. Duplicate first cells caught by count-equality: parsed row count MUST equal registry cardinality (13 / 18).
- **Columns:** codes = `code | name | meaning | when sent` · types = `type | name | direction | response | body summary`. The `response` cell names the answering type (`FetchResp` for GroupFetch) or `—` for response/error rows.
- **Leg A (codes):** `go/parser` over `errors.go`, consts with explicit type `Code` → (identifier, value); derived doc name = strip `Code` prefix, camelCase→UPPER_SNAKE (`CodeMsgTooLarge` → `MSG_TOO_LARGE`). Three-way set equality, names AND values, both directions: parsed source ≡ `wire.AllCodes()` (errors.go:32-39) ≡ doc codes table.
- **Leg B (types):** `go/parser` over ALL non-test `.go` files in the package dir (`os.ReadDir` + per-file parse — package-WIDE, never just frame.go), filtering consts by BOTH the `Type` name prefix AND explicit type `byte` (never type alone). Derived name = strip `Type` (`TypeGroupFetch` → `GroupFetch`). Assert parsed ≡ doc table both directions AND contiguity: values exactly {1..17, 255}, 18 entries.
- **Leg C:** every prose cell in both tables non-empty (`meaning`, `when sent`, `direction`, `response`, `body summary`).
- **Leg D (TOP-2):** no documented type name matches `(?i)resize|alter|grow|shrink|repartition`; the doc contains the fixed-partition statement line (grep `no resize`).

## §P — PROTOCOL.md outline with source anchors (prose is written AGAINST these, never from memory)

Presence check per section: `grep -n '^## ' docs/PROTOCOL.md` lists P1–P13's headings; pinned phrases checked by the greps given inline.

- **P1 Overview.** Version 1 is the only version (wire/frame.go:12); registries add-only, never renumbered (errors.go:9-10, frame.go:14-15); single broker, plain TCP, big-endian throughout; normative-vs-informative convention announced (ledger row 8).
- **P2 Frame envelope + ASCII byte layout.** `[u32 len][u8 ver=1][u8 type][payload]`, len covers everything after itself (frame.go:46-54); caps are TOTAL on-the-wire size incl. the 4-byte prefix, asymmetric: `MaxRequestFrame` = 1 MiB+4 KiB, `MaxResponseFrame` = 4 MiB+64 KiB (frame.go:37-42); len<2 → MALFORMED (frame.go:67-69); oversized → FRAME_TOO_LARGE before allocation (70-72); bad version → MALFORMED (81-83); mid-frame cut → I/O error, not a code (74-79). The fenced-ASCII byte diagram (D-SL6-9 diagram 1) lives here — plain ```` ```text ````, no bake needed.
- **P3 Encoding primitives.** u8/u16/u32/u64 big-endian · str=[u16 n]bytes · blob=[u32 n]bytes (frame.go:89-96); strict decode: truncation (108-112), hostile lengths bounded by the frame (114-125, 159-160), trailing bytes → MALFORMED (164-169).
- **P4 Message-type registry** — the types marker block + table, 18 rows per §G. Source: frame.go:16-35. NORMATIVE read loop beside it: every request is answered by its paired `response` type OR an Error frame (255) — read the type byte FIRST, branch on 255 before matching (messages.go:373-379 pairing; server.go:246-256 error path). GroupFetch(17) is answered by FetchResp(4); no GroupFetchResp exists.
- **P5 Per-message field tables** (field | wire form | meaning; one subsection per type). Anchors: Produce messages.go:5-26 · ProduceResp 28-45 · Fetch 105-145 · FetchResp 147-195 (zero-rec group legal, 153-158) · CreateTopic 47-66 · CreateTopicResp/ListTopics EMPTY bodies 68 · ListTopicsResp 76-103 · JoinGroup 197-217 (one topic per group) · JoinGroupResp 219-257 (join carries resume state) · Heartbeat 259-282 · HeartbeatResp + REJOIN bit0 284-306 · CommitOffsets 308-348 (next-to-read, 308-314) · CommitOffsetsResp/LeaveGroupResp EMPTY 350 · LeaveGroup 352-371 · GroupFetch 373-416 · ErrorMsg 418-437. Name rule: regex `^[a-z0-9][a-z0-9._-]{0,127}$` (names.go:8-16) → INVALID_NAME. **D-SL6-7 note 1 at GroupFetch** (why it exists: group fetches carry memberID+generation for serve-time fencing, Fetch has no such fields, topic implied by binding — messages.go:373-379, handlers.go:399-410; supersedes DD-15's prose picture, surfaced at SL2): `grep -c 'serve-time fencing' docs/PROTOCOL.md` ≥ 1. **Note 2 at Fetch** (multi-entry 2..16 fully supported; the shipped client happens not to use it raw): `grep -c 'no shipped-client caller' docs/PROTOCOL.md` ≥ 1.
- **P6 Error-code registry** — the codes marker block + table, 13 rows per §G. Source: errors.go:11-25. `when sent` cells anchored per code via handlers.go: storageError 439-456 · groupError 421-436 · MSG_TOO_LARGE 70-73 · FETCH_TOO_WIDE 98-99/382-383 · CAP_EXCEEDED caps 101-106/385-390 + conn cap · MALFORMED decode/name/empty-entries/unknown-type · SHUTTING_DOWN server.go:236-239 · WRITE_FAILED as the no-ack persistence failure (handlers.go:431-435). Note beside 13 codes: DD-17's locked prose listed 12 — surfaced at SL4 (D-SL4-6), referenced not re-flagged.
- **P7 Connection lifecycle.** One request in flight per conn (the sequential frame loop, server.go:200-227). Idle reclaim: no complete request for 5 min → SILENT close, no farewell frame (server.go:22, 203-213). Error-then-CLOSE class: oversized frame / bad version (server.go:213-221), unknown type (handlers.go:56-58 + server.go:249-252). Error-then-CONTINUE class: malformed body and every semantic rejection (server.go:246-252). **CAP_EXCEEDED is UNSOLICITED** (`grep -c 'unsolicited' docs/PROTOCOL.md` ≥ 1): written immediately on accept before any request, ~1 s write deadline, conn never enters the broker's table — a slow reader can miss the frame and see only EOF, stated (server.go:162-177). SHUTTING_DOWN drain: every request answered with code 10 while draining; parked fetches return the empty shape (server.go:124-152 stop sequence, 236-239; handlers.go:242-244).
- **P8 Fetch & long-poll.** Validation order exactly as coded (handlers.go:85-114 / 371-398): decode → name → zero entries MALFORMED → >16 entries FETCH_TOO_WIDE → maxWait>30 000 CAP_EXCEEDED → maxBytes>4 MiB CAP_EXCEEDED; defaults maxWait 0→5 s, maxBytes 0→1 MiB (handlers.go:17-24, 107-114). **Min-one budget formula, NORMATIVE interop contract:** each record costs `12 + len(payload)` budget bytes (8-byte offset + 4-byte length prefix); min-one is per-RESPONSE — only the FIRST record served across all entries in request order may exceed maxBytes (handlers.go:196-210). Pinned empty-at-timeout shape: exactly one zero-rec group per requested entry, in request order (handlers.go:252-260). **NORMATIVE read-deadline rule:** a client's read timeout MUST exceed its requested maxWait — the broker legally parks up to maxWait (≤30 s) and arms its write deadline only AFTER dispatch, it will never write early (server.go:241-245; handlers.go:164-250).
- **P9 Consumer groups** (+ the baked lifecycle diagram, D-SL6-9 diagram 2). Control conn = liveness; drop = immediate member death (server.go:190-198, coordinator.go:348-365). Timing: heartbeat every 500 ms vs 2 s session window, 100 ms sweep (coordinator.go:21-25, 410-424). Immediate rebalance, range assignment, generations (coordinator.go:289-317). REJOIN is a LEVEL in flags bit0 — re-JoinGroup required (messages.go:284-287, coordinator.go:319-332). Join-carries-state (messages.go:226-232, coordinator.go:276-287). Heartbeats EXEMPT from the generation fence (coordinator.go:319-332, handlers.go:324-326). Serve-time fencing, pinned precedence UNKNOWN_MEMBER-before-STALE_GENERATION (coordinator.go:33-47, 367-395). **The 12-vs-13 SPLIT, NORMATIVE, implemented differently:** 13 = identity gone, fresh join mints a NEW member, generation bump, group-wide rebalance (coordinator.go:219-231); 12 = live member, re-Join on the SAME control conn ADOPTS the current generation, NO bump, then reissue (coordinator.go:209-217); either on a FETCH = re-join+reissue, never fatal. **Parked-GroupFetch disposition is race-dependent, documented as such:** a data-wake re-fences and returns 12 (handlers.go:172-176, 399-406); a timeout/shutdown returns the empty shape WITHOUT re-fencing (handlers.go:236-244). Commit = fence → merge-onto-current → atomicWrite BEFORE the ack → re-fence at install (commits.go:29-69); commits naming partitions outside the assignment → STALE_GENERATION (commits.go:83-91).
- **P10 Limits table** (NOT machine-diffed, ledger row 5 — cite Go constant NAMES so the human audit is one grep per row): wire.MaxRequestFrame / MaxResponseFrame → FRAME_TOO_LARGE (frame.go:39-42) · broker.MaxPayload 1 MiB → MSG_TOO_LARGE (handlers.go:18, 70-73) · broker.MaxFetchWaitMs 30 000 / MaxFetchBytes 4 MiB → CAP_EXCEEDED (handlers.go:20-22) · broker.MaxFetchEntries 16 → FETCH_TOO_WIDE (handlers.go:23) · name ≤128 bytes (names.go:8) → INVALID_NAME · storage.MaxTopics 64 / MaxPartitionsPerTopic 16 → CAP_EXCEEDED (store.go:21-22, 126-127, 134) · group.MaxGroups 64 / MaxMembersPerGroup 32 → CAP_EXCEEDED (coordinator.go:28-31) · broker.DefaultMaxConns 256 → CAP_EXCEEDED, unsolicited (server.go:19-24).
- **P11 Durability semantics.** Ack = append → fsync → frontier atomicWrite → ack (partition.go:1-4, 129-131, flusher 290-345); reads capped at the durable frontier (partition.go:161-166, 197-198, 226-230); 5 ms group-commit window (`flushWindow`, partition.go:19-22); committed offset = next-to-read; at-least-once — duplicates possible after crashes/rebalances, loss is not. **The fsync platform caveat is the VERBATIM sentence from cmd/bench/report.go:33** (`reportCaveats`): "durability is platform-qualified: on macOS Go's Sync is F_FULLFSYNC (drive-cache barrier — stronger and slower); on Linux plain fsync (DD-7, corrected)" — one wording, one authority (ledger row 7); README P§R(a) reuses the same sentence.
- **P12 Normative client obligations vs informative notes** (ledger row 8). Normative: heartbeat cadence ≤500 ms against the 2 s window · re-JoinGroup on REJOIN · the 12-vs-13 split as in P9 · the read-deadline rule as in P8. Informative: the shipped client's redial behavior. G-SL6-4 stated: violations are enforced by liveness/fencing, not by errors naming the violation — a slow heartbeater is swept, not lectured.
- **P13 What this protocol does NOT have.** No partition resize — "partition count is fixed at creation; this message set contains no resize operation", ALSO stated beside CreateTopic in P5 (D-SL6-5; `grep -n 'no resize' docs/PROTOCOL.md` ≥ 1) · no topic delete · no auth/TLS · no Kafka compatibility · version 1 only; types/codes add-only, never renumbered.

## §R — README pass (D-SL6-6; NEVER touch lines 80–123, the bench markers)

- (a) Replace the OLD inverted fsync caveat at README lines 56–58 ("fsync on macOS may not flush the drive cache") with the report.go:33 sentence VERBATIM. Check: `grep -c 'may not flush' README.md` → 0.
- (b) Add R2 under "What it guarantees today": delivery is at-least-once; duplicates possible after crashes/rebalances, loss is not. Check: `grep -c 'at-least-once' README.md` ≥ 1 (whole file — the render gate proves it cannot be satisfied from inside the markers).
- (c) Port warning ON the existing loopback sentence (README lines 61–63), naming the flag (cmd/minikafka/main.go:17), with the pinned phrase: broker binds 127.0.0.1 by default; pointing `--addr` anywhere else exposes **an unauthenticated protocol** to that network. Check: `grep -c 'unauthenticated protocol' README.md` ≥ 1.
- (d) Link docs/PROTOCOL.md ("implement your own client from…"), e.g. in Development.
- (e) Top screen (lines 1–10) untouched — OPS-3 stays true.
- (f) Bench markers untouched — proven by `go test ./cmd/bench -count=1` (the armed byte-compare gate, render_test.go:131), not by eyeballing.

## §C — ci.yml change spec (D-SL6-4 + ledger row 6)

- NEW sixth job `protocol-doc`: checkout@v4 · setup-go@v5 go-version "1.24.x" · `echo "runner image= $ImageOS $ImageVersion"` · `go test ./internal/wire -run TestProtocolDoc -count=1`. (The test also rides the existing test job's `go test ./...` — harmless double-run; checks.sh covers it locally with zero script changes.)
- ADD the echo line to `vet-staticcheck` (after setup-go, ci.yml:19-29) and `stdlib-audit` (ci.yml:31-38) — the two label-runner jobs missing it. `demo-timing` is EXEMPT (digest-pinned container + its `cat /etc/os-release`). After: 5 echo lines total (test, vet-staticcheck, stdlib-audit, build-smoke, protocol-doc).
- `build-smoke` (ci.yml:68-92): `runs-on: ${{ matrix.os }}` + `strategy: matrix: os: [ubuntu-latest, macos-latest]`; steps unchanged. Push → six jobs / SEVEN check runs (build-smoke × 2) — verified at exit by the integrator; the builder verifies YAML validity + greps only.

## §A — audits (D-SL6-8), outputs tee'd into docs/receipts/sl6-audits.txt

```sh
# PROT-2: only client (+ broker-hosting in demo/bench per D-SL5-1/D-SL3-1 precedent)
go list -deps ./cmd/demo ./cmd/bench ./cmd/mk | grep mini-kafka
# OPS-2: green at HEAD + pins (integrator refreshes gh line after the exit push)
gh run list -L1
grep -n 'go 1.24' go.mod; grep -n '1.24.x\|@2025.1.1\|@v[0-9]\|sha256:' .github/workflows/ci.yml scripts/checks.sh
# OPS-3
test -f LICENSE && echo LICENSE-OK
# NFR-1 (local equivalent of the stdlib-audit job)
bash scripts/stdlib_audit.sh
# NFR-3 — the FIXED loud-failure form (seat A killed the vacuous sed); vet+staticcheck via checks.sh
go list ./... | while read -r p; do go doc "$p" | grep -q '^Package ' || { echo "MISSING doc header: $p"; exit 1; }; done && echo ALL-DOCUMENTED
```

OPS-1 (matrix green on both platforms) is push-evidence — integrator captures it at exit into the same receipt.

## §S — sabotage rows (all SIX locally, row 8) + redaction

Each: mutate docs/PROTOCOL.md → `go test ./internal/wire -run TestProtocolDoc -count=1` RED → restore (git checkout the file) → GREEN. (1) delete a codes row · (2) add a fictitious `ERR_RESIZE_TOPIC` row (legs A and D both fire) · (3) renumber a types row · (4) blank a `meaning` cell · (5) insert a stray prose line between markers · (6) duplicate an existing row. Redaction: grep the repo for the scrubbed registration address — the PATTERN is held outside the repo (integrator supplies it at exit; committing the pattern anywhere, including this plan or a receipt, would itself violate the scrub). ZERO matches repo-wide is the bar; the receipt records the count, never the pattern (D-SL6-10).

## Pitfalls (named so they can be checked)

- **go/parser type filter:** leg B keys on the `Type` NAME prefix **AND** explicit `byte` type together — never type alone (a future non-message `byte` const must not sweep in), never name alone. And it walks ALL non-test `.go` files in `internal/wire`, not just frame.go (seat B's false-green).
- **Grammar strictness vs the in-situ comment:** the grammar HTML comment sits INSIDE the begin marker and is itself a LEGAL line class — a parser that only allows blank/header/separator/data will red on the doc's own contract comment.
- **Escaped-pipe splitting:** split cells on UNESCAPED `|` only; `\|` is a literal pipe in prose (e.g. a wire-form cell like `[u16 n]\|bytes` sketches). Unescape after splitting.
- **Hard fail, never skip:** missing doc, missing markers, or an unparseable table is `t.Fatal` with a named reason. This is the OPPOSITE of the bench gate's bootstrap skip (render_test.go) — do not copy that pattern; there is no bootstrap state for PROTOCOL.md once row 2 lands.
- **The README render gate fails the whole suite** if ANYTHING between `<!-- bench:begin -->` and `<!-- bench:end -->` shifts — including an editor "fixing" trailing whitespace or a formatter pass. Edit README outside the markers only; verify with `go test ./cmd/bench -count=1`, not by reading the diff.
- **Bake order and bake safety:** bake AFTER prose is final (row 4); the baker edits docs/PROTOCOL.md in place — re-run the focused test after baking to prove the marker blocks survived. No `;` in mermaid message/note text (repo rule — a `;` kills the whole diagram's parse); render-check with mmdc BEFORE baking.
- **Module path:** imports are `github.com/systemcnu/mini-kafka/internal/wire` — the test file needs no import of broker/group/storage; if you find yourself importing them, you are re-deriving behavior the doc anchors already pin.
- **Counts by command, never memory:** 13 codes / 18 types / 5→6 jobs are held by the commands at the top of this plan; if a re-run disagrees, the TREE changed — stop and re-check against DESIGN.md, do not "fix" the test to the remembered number.
- **Derived names are mechanical:** `CodeMsgTooLarge` → `MSG_TOO_LARGE` (strip `Code`, camel→UPPER_SNAKE — watch multi-hump words; derive in code, don't hand-write a map) and `TypeGroupFetch` → `GroupFetch` (strip `Type` only). A hand-maintained name map anywhere is a second registry (ledger row 3).
- **Verbatim means verbatim:** the fsync sentence in README and PROTOCOL.md is copy-pasted from report.go:33 — a paraphrase regresses the SL5-corrected claim and breaks the one-authority rule (ledger row 7).
- **ZERO production-code changes:** if any `.go` file outside `internal/wire/protocoldoc_test.go` needs touching to go green, STOP — that is a design-level event (D-SL6-10), not a build detail.
