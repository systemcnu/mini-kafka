# SL7 Implementation Plan — Phase A only

**Derives from: slices/SL7/DESIGN.md FINALIZED 2026-07-31.** Any design change patches this plan in the same change. Contracts live in D-SL7-1..14 and the ledger; this plan owns where things live, the build order, and the per-row verify commands — **for Phase A ONLY** (D-SL7-9 steps 0–5). The plan STOPS at: Phase A complete, full suite race-green, pushed, CI green, **the slice PAUSES for Sri's checkpoint**. The builder NEVER touches account creation, the Blueprint/deploy, Pages settings, or any hostname — the checkpoint and both Phase B endings are D-SL7-9/10's, executed later by Sri + the integrator. Commit structure (ledger 12): every Phase A commit carries `RENDER_URL_TBD`; the hostname flip is ONE later commit, outside this plan.

**ZERO production-code changes (D-SL7-1/13 STOP rule):** all new `.go` code lives under `cmd/showcase/`; if going green ever needs a change to broker/client/wire/storage/group code, STOP — that is a design-level event needing its own loud ledger row first (SL4's lesson). **ZERO ci.yml changes (ledger 13):** showcase tests ride `go test ./...`; the stdlib audit already sweeps `cmd/...`.

Counts used below were derived by command on the tree this plan was written against — re-run them, never trust memory:

```sh
grep -rn '^func Test' --include='*_test.go' . | wc -l      # 147 test functions today (DESIGN §6.2 baseline)
grep -n 'bench:begin\|bench:end' README.md                  # lines 85 and 128 today — NEVER touched
grep -n '^## ' README.md                                    # "Run it by hand" line 24; new section lands after it
```

## Codebase map (delta only)

```
cmd/showcase/main.go             NEW: flagless main — env wiring (PORT, SHOWCASE_DISK_CAP_MB),
                                 MkdirTemp data dir (NOT removed on exit — see Pitfalls),
                                 start feeder, serve the §H http.Server
cmd/showcase/server.go           NEW: listenAddr(), mux ("GET /{$}" page, "GET /feed" JSON+CORS),
                                 the non-zero-timeouts http.Server literal (§H)
cmd/showcase/snapshot.go         NEW: the immutable snapshot struct + the ONE atomic.Value holder (§S)
cmd/showcase/feeder.go           NEW: in-process broker + producer/consumer/walker goroutines,
                                 ring of 50, sticky pause, clean stop (§F)
cmd/showcase/page.html           NEW: //go:embed watch-only page, inline-everything (§P)
cmd/showcase/server_test.go      NEW: route table, 405/404, CORS, bind literals, timeouts-nonzero
cmd/showcase/feed_test.go        NEW: ten-field JSON shape against an injected snapshot (§J)
cmd/showcase/feeder_test.go      NEW: live-feeder integration + broker-config literal + clean-stop
cmd/showcase/cap_test.go         NEW: plateau, fresh-boot, WRITE_FAILED→pause, env-cap parse
cmd/showcase/page_test.go        NEW: page served, states present, 10 s constant, no-external-assets
cmd/showcase/concurrent_test.go  NEW: the -race concurrent-load proof (§S test shape)
docs/showcase/index.html         NEW: Pages loading shim — pinned predicate + TBD short-circuit (§M)
docs/.nojekyll                   NEW: empty file
render.yaml                      NEW: §Y exact content (plan: free, autoDeploy: false, list-form envVars)
scripts/showcase_portscan.sh     NEW + showcase_portscan.expected + showcase_portscan.selftest.expected (§N)
docs/receipts/sl7-red-green.txt  NEW (builder): §Red
README.md                        EDIT: "## Live showcase" inserted after "Run it by hand" (§R);
                                 top screen (lines 1–10) and the bench markers NEVER touched
```

**Where do I look for X?** broker-hosting-in-process precedent → cmd/demo/main.go:54 (MkdirTemp), :76 (`broker.New(broker.Config{Addr: "127.0.0.1:0", DataDir: dataDir})`), :80–83 (Start + `srv.Addr().String()`); same shape cmd/bench/main.go:118–131 (adds `defer srv.Stop()`) · client surface the feeder consumes → client/client.go:109 `DialProducer` / :118 `Produce` / :239 `JoinGroup` / :353 `Poll` / :457 `Commit` / :489 `Assignment` / :547 `DialAdmin` / :556 `CreateTopic` · WRITE_FAILED semantics → `client.CodeWriteFailed` = 11 (client/client.go:36); the reads-still-serve posture the pause mirrors → internal/broker/writefailed_test.go:29 · broker Config/New/Start/Addr/Stop → internal/broker/server.go:27–35, 68, 111, 122, 129 · the armed README byte-compare → cmd/bench/render_test.go:131 · black-box build-the-binary precedent (not needed here — showcase tests are in-package `package main`) → cmd/demo/demo_test.go:20–37.

Exit process (STATUS, BRIEF + its ONE baked diagram per D-SL7-14, code-map regen, receipts beyond red-green, sabotage rows, commits, push mechanics) is SD-6's and D-SL7-9's — **no build rows for it here**. Sabotage candidates (DESIGN §5) run at exit in the integrator's hands, not in this plan.

## Build order (each row done when DEMONSTRATED; every row ≤ half a day)

| # | Builds | Verify (command) |
|---|--------|------------------|
| 0 | **Step-0 feasibility re-verification (D-SL7-9 — BEFORE any build spend):** re-read Render's free-tier docs page (the one the feasibility receipt cites); diff its facts against `docs/receipts/showcase-feasibility.md`. Unchanged → proceed; a card requirement now documented → report it — the kill fires and rows 1–12 are never built | Dated note appended to `docs/receipts/showcase-feasibility.md` (slice-exit record section): "re-verified <date>: no delta" or the delta found |
| 1 | **Skeleton + ALL six test files (red-before-green is structural).** Skeleton compiles but does nothing: routeless mux (no patterns registered), placeholder `page.html` (empty body), feeder whose `start` is a no-op, snapshot holder returning the zero snapshot. All six `*_test.go` files land COMPLETE against the §F/§H/§J/§P/§S specs | `go build ./cmd/showcase` clean · one focused run per test file → **six named REDs**, captured per §Red |
| 2 | **HTTP surface green (D-SL7-2, §H):** `"GET /{$}"` + `"GET /feed"` patterns (never bare `"/"`); `/feed` sets `Access-Control-Allow-Origin: *`; `listenAddr()` bind rule (PORT set → `0.0.0.0:$PORT`, unset → `127.0.0.1:8080`); the explicit `&http.Server{...}` with all four timeouts non-zero; no handler reads a body or forwards a query param | `go test ./cmd/showcase -run 'TestRoutes\|TestBind\|TestTimeouts' -count=1` green · `grep -rn 'flag\.' cmd/showcase/` → 0 hits (env-only, no flags by construction) |
| 3 | **Snapshot + feed handler green (D-SL7-3, §S/§J):** the immutable snapshot struct, the ONE `atomic.Value` (always stores `*snapshot`), `/feed` handler = Load + `json.Marshal` — computes NOTHING; all ten fields carried by the snapshot including `uptimeSeconds`, `memBytes`, `startedAt` | `go test ./cmd/showcase -run TestFeedShape -count=1` green (all ten fields present, types checked, `assignment` present, NO `members` key) |
| 4 | **Feeder wiring green (D-SL7-1, §F):** `broker.New(broker.Config{Addr: "127.0.0.1:0", DataDir: dir})` per the demo/bench precedent, topic 1×4, producer 500 ms ticker `msg-<n>` round-robin, GroupConsumer poll+commit loop, ring of 50, 30 s disk walker + ReadMemStats, snapshot rebuilt after every mutation; clean `stop()` that joins all goroutines (§F order) | `go test ./cmd/showcase -run 'TestFeeder\|TestBrokerConfig' -count=1` green · `grep -n '127.0.0.1:0' cmd/showcase/*.go` → the ONE literal · `go list -deps ./cmd/showcase | grep mini-kafka` = client + hosted internal/* only (PROT-2, D-SL6-8 form) |
| 5 | **Disk bound green (D-SL7-4, §F):** sticky pause when walk reports `diskBytes ≥ cap` OR a produce returns `client.CodeWriteFailed` (injected fake at the producer seam); `status` flips to `paused-at-cap`; reads/feed keep serving; fresh-boot = new MkdirTemp per start; `SHOWCASE_DISK_CAP_MB` parse (default 200 MiB) | `go test ./cmd/showcase -run 'TestPlateau\|TestFreshBoot\|TestWriteFailedPause\|TestCapEnv' -count=1` green (plateau: cap 64 KiB + accelerated seams — dir size stops growing across further walks while `/feed` still 200s) |
| 6 | **Embedded page final (D-SL7-5, §P):** all four states, the 10 s poll constant, watch-only copy, inline CSS/JS, `paused-at-cap` banner, "feed unreachable" narration | `go test ./cmd/showcase -run TestPage -count=1` green · `grep -c 'src="http' cmd/showcase/page.html` → 0 · no `http(s)://` in page except (optionally) the repo link |
| 7 | **Concurrency proof (D-SL7-1/2, §S test shape):** the concurrent-load test green under `-race` — live feeder + N goroutines hammering `/feed` + `/` while the ticker runs; clean-stop leak check | `go test ./cmd/showcase -race -count=1` — WHOLE package green under race (this is also §Red's GREEN capture) |
| 8 | **Pages shim (D-SL7-6, §M):** `docs/showcase/index.html` + empty `docs/.nojekyll`; `RENDER_URL_TBD` short-circuit (fallback immediately, never fetch); pinned live predicate; 5 s poll, elapsed counter, 5-min degrade, redirect-on-live | `grep -c 'RENDER_URL_TBD' docs/showcase/index.html` → 1 · `grep -c 'src="http' docs/showcase/index.html` → 0 · manual: open the file in a browser → the "not currently hosted" fallback shows IMMEDIATELY, zero network requests (the short-circuit observed) |
| 9 | **`render.yaml` (D-SL7-7, §Y exact content):** plan: free, autoDeploy: false, list-of-objects envVars, no healthCheckPath | `python3 -c "import yaml; yaml.safe_load(open('render.yaml'))"` clean · `grep -c 'plan: free' render.yaml` → 1 · `grep -c 'autoDeploy: false' render.yaml` → 1 · `grep -c healthCheckPath render.yaml` → 0 |
| 10 | **Port-scan script + BOTH expected files + self-test (D-SL7-8, §N):** `scripts/showcase_portscan.sh` with `--ports`/`--expected` overrides, stable `port NNN: open\|closed` lines, non-zero on ANY deviation; header documents the per-deploy procedure incl. the two live curl probes; `showcase_portscan.expected` (live pattern) + `showcase_portscan.selftest.expected` committed | `shellcheck scripts/showcase_portscan.sh` clean · §N self-test sequence: open-detected-open, closed-detected-closed, deviation → exit non-zero — all three observed by command |
| 11 | **README variant A committed (D-SL7-11, §R):** "## Live showcase" after "Run it by hand"; hostname-free (Pages link only); teardown criterion verbatim from root DESIGN.md DD-23; variant B STAGED in §R, NOT committed | §R grep battery · `go test ./cmd/bench -count=1` green (render_test.go:131 — bench section byte-identical) · `git diff README.md` shows lines 1–10 and the marker block untouched |
| 12 | **Phase A closure:** full suite race-green, local battery, count delta, scrub pass on every touched file (§Scrub), receipt complete. Then HAND OFF: integrator commits/pushes Phase A, confirms CI green — **the slice PAUSES for Sri's checkpoint (D-SL7-9). Nothing past this row is this plan's.** | `go test ./... -race -count=1` green — test-function count recorded (up from 147), tee'd into the §Red receipt · `bash scripts/checks.sh` → ALL CHECKS GREEN · `git status` touch list ⊆ the codebase-map delta (D-SL7-13) |

0 strictly first (it can cancel everything). 1 before 2–7 (reds are structural). 2→3→4→5 in order (each consumes the prior); 6–7 after 5 (the page renders states the feeder produces; the race proof needs the live feeder). 8–11 in any order after 7. 12 last.

## §Red — exactly which reds go into docs/receipts/sl7-red-green.txt

Receipted PER TEST FILE (DESIGN §5). At end of row 1, capture one focused run per file, verbatim (`go test ./cmd/showcase -run <Name> -count=1 2>&1 | tee -a docs/receipts/sl7-red-green.txt`):

1. **RED server_test:** routeless skeleton — `GET /feed` → 404, want 200 JSON (the failure names the route).
2. **RED feed_test:** zero snapshot / absent handler — missing-field failure naming the ten-field contract.
3. **RED feeder_test:** no-op feeder — `produced` still 0 after the wait window (named: "feeder produced nothing").
4. **RED cap_test:** no pause logic — `status` never flips / dir keeps growing past the 64 KiB test cap.
5. **RED page_test:** placeholder page — state markers and the 10 s poll constant absent.
6. **RED concurrent_test:** hammered `/feed` on the skeleton → non-200s.

Then: **GREEN (end of row 7):** `go test ./cmd/showcase -race -count=1` — whole package green under race. **GREEN (row 12):** `go test ./... -race -count=1` with the test-function count (up from 147). Both appended to the same receipt.

## §S — the concurrency contract (D-SL7-1's seat-B HIGH; builder implements exactly this)

- **The snapshot type:** one exported-nothing `snapshot` struct carrying ALL ten §J fields with value/deep-copied contents — `partitions []partRow` and `recent []recentRow` are FRESH slices built at swap time, `assignment []uint32` a fresh copy of `Assignment()`'s return. Nothing in a stored snapshot is ever mutated afterward.
- **The holder:** ONE `atomic.Value`; every `Store` is the same concrete type `*snapshot` (atomic.Value panics on type change — store a pointer, always). Initialized with a valid zero-state snapshot BEFORE the listener starts, so a request racing startup still reads a complete snapshot.
- **The discipline:** feeder internals (producer loop, consumer loop, walker) share one `feederState` guarded by ONE plain mutex — mutated ONLY by those three goroutines. After EVERY mutation the mutator rebuilds a fresh `*snapshot` under the mutex and `Store`s it. HTTP handlers do exactly one thing: `Load` + marshal. Handlers never touch `feederState`, never hold the mutex, never see a partial write.
- **The concurrent-load test shape (concurrent_test.go, run under `-race`):** start the REAL feeder (accelerated seams: 10 ms tick, 50 ms walk, tiny cap NOT hit) against a live `127.0.0.1:0` broker; `httptest.NewServer(mux)`; launch N=16 goroutines, each doing 100 GETs alternating `/feed` and `/`, fully decoding `/feed`'s JSON every time (forces reads of every snapshot field) — all while the feeder keeps swapping snapshots under `t`. Every response MUST be 200. Then `stop()` the feeder and assert it returns (bounded select — see Pitfalls) with no goroutine left running. A torn snapshot, shared map, or unjoined goroutine dies loudly here because the whole battery runs `-race`.

## §F — feeder wiring (D-SL7-1/4; ZERO production-code changes — public client surface only)

- **Broker hosting (demo/bench precedent, cited above):** `dir, _ := os.MkdirTemp("", "minikafka-showcase-")` → `srv, err := broker.New(broker.Config{Addr: "127.0.0.1:0", DataDir: dir})` → `srv.Start()` → `addr := srv.Addr().String()`. The loopback literal is HARD-CODED in exactly one place, exposed to tests via `func brokerConfig(dir string) broker.Config` so the config-literal unit test asserts `.Addr == "127.0.0.1:0"` on the same value production uses. NO flags anywhere in the package (`flag` unimported — grep-verified in row 2).
- **Topic:** `client.DialAdmin(addr)` → `CreateTopic("showcase", 4)` → close admin. 1 topic × 4 partitions (ledger 13's rates).
- **Producer loop:** `client.DialProducer(addr)`; `time.Ticker` at 500 ms (~2 msg/s); on each tick, if not paused: `Produce("showcase", uint32(n%4), []byte(fmt.Sprintf("msg-%d", n)))`; increment `produced` on ack. A returned `*client.Error` with `Code == client.CodeWriteFailed` → sticky pause (below). The producer is held behind a tiny in-package interface (`produce(topic, partition, payload) (uint64, error)`) satisfied by `*client.Producer` — the seam the WRITE_FAILED test injects its failing fake through (the client boundary, per D-SL7-4's check).
- **Consumer loop:** `client.JoinGroup(addr, "showcase-watchers", "showcase")`; loop: `Poll(1 * time.Second)` (short maxWait so shutdown is fast) → append records to the ring of 50 (fixed circular buffer; snapshot emits oldest-first) → advance per-partition `nextOffset` = max(consumed offset+1) (frontiers derived from CONSUMED offsets — all 4 partitions initialized at 0 so the JSON always has 4 rows) → `Commit()` per non-empty batch → refresh `assignment` from `Assignment()`. ONE member, no rebalance theater (ledger 7).
- **Walker loop:** every 30 s (seam), `filepath.WalkDir` over the feeder's OWN data dir summing file sizes → `diskBytes`; `runtime.ReadMemStats` → `memBytes` (HeapAlloc); cap check → pause.
- **Sticky pause (ledger 8):** one boolean in `feederState`, set by cap-hit or WRITE_FAILED, NEVER cleared (restart is the only un-pause); producer skips ticks while set; consumer and walker keep running; `status` = `paused-at-cap`.
- **Test seams (struct fields on the feeder config, not flags, not env):** tick interval, walk interval, cap bytes, the producer interface. Production `main` passes the pinned values (500 ms / 30 s / env-parsed cap / real Producer).
- **`stop()` order (the leak-free shutdown):** close stop channel → `gc.Close()` (unblocks a Poll parked server-side — the poll loop treats any post-stop error as exit, never a crash) → producer `Close()` → join all three goroutines (done channels) → `srv.Stop()` (broker last). Tests ALWAYS call `stop()` (defer) — the whole battery runs `-race` and a leaked goroutine touching state after test teardown is a failure.
- **`main` wiring:** parse `SHOWCASE_DISK_CAP_MB` (unset/garbage → 200), MkdirTemp (NOT removed on exit — see Pitfalls), start feeder, `listenAddr(os.Getenv("PORT"))`, serve the §H server. No signal-handler cleanup ceremony: Render restarts reset everything by design (D-SL7-4).

## §H — the HTTP surface (D-SL7-2)

- **Routes:** `mux.HandleFunc("GET /{$}", page)` and `mux.HandleFunc("GET /feed", feed)` — Go 1.22+ method+exact patterns (go.mod says go 1.24). The method in the pattern makes non-GET → 405 and the `{$}` makes unknown paths → 404, both by the stdlib mux — provable, not hand-rolled. NEVER a bare `"/"` (Pitfalls).
- **`/feed`:** `Content-Type: application/json` + `Access-Control-Allow-Origin: *` (ledger 2), body = the Loaded snapshot marshaled. `/` serves the embedded page; no CORS needed.
- **No request influence:** neither handler reads the body or any query parameter — a request can influence nothing but its own response (route-table test asserts POST/PUT/DELETE → 405 on both routes, `/nope` → 404, and that a query-string GET returns the identical body).
- **The server literal:** `&http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5s, ReadTimeout: 10s, WriteTimeout: 20s, IdleTimeout: 60s}` — the timeouts-nonzero test asserts all four > 0 on the same constructor production uses (a zero-valued server is Slowloris-open).
- **`listenAddr(port string) string`:** `port == ""` → `"127.0.0.1:8080"`; else `"0.0.0.0:" + port` (ledger 6). Pure function; both literals unit-tested.

## §J — the feed contract (D-SL7-3; exactly TEN fields, no more, no fewer)

`status` (string: `live`|`paused-at-cap`) · `uptimeSeconds` (int64, computed at snapshot-build time — the handler computes nothing) · `produced` (uint64 total acked) · `partitions` ([{`partition` uint32, `nextOffset` uint64}] ×4, sorted) · `recent` ([{`partition`,`offset`,`payload` string}], ring of 50, oldest-first) · `assignment` ([]uint32 from `Assignment()` — there is NO `members` field; the shipped client exposes no member enumeration and a constant dressed as telemetry was seat B's catch) · `diskBytes` (int64) · `diskCapBytes` (int64) · `memBytes` (uint64 HeapAlloc) · `startedAt` (RFC3339 string). The shape test unmarshals into a `map[string]json.RawMessage`, asserts exactly these ten keys, and type-checks each.

## §P — the embedded page (D-SL7-5)

One `page.html`, `//go:embed`, ALL CSS/JS inline. States: **connecting** (first load, before the first `/feed` succeeds) → **live** (recent records flowing, per-partition nextOffsets ticking, the assignment shown) → **paused-at-cap** (honest banner, feed still rendered) → **feed unreachable** ("the instance may be restarting — free instances restart at any time", narrated). Poll `fetch('/feed')` every 10 000 ms (the DD-23 constant — page_test greps for `10000`). Copy says watch-only out loud: "you are watching a self-driving instance; there is no way to write to it". No external references: `grep -c 'src="http' cmd/showcase/page.html` → 0; the only permitted absolute URL is the repo link.

## §M — the Pages shim (D-SL7-6; the pinned predicate, verbatim logic)

`docs/showcase/index.html` (+ empty `docs/.nojekyll`), static, inline-everything. Top of the script: `const RENDER_URL = "RENDER_URL_TBD";`. Logic, in order:

1. **Short-circuit (as built, patched post-review — verifier judged it strictly safer than the literal equality):** any `RENDER_URL` that is not an absolute `https://` URL → `showFallback(); return;` — NEVER fetch. This catches the `RENDER_URL_TBD` placeholder AND typo'd/http/relative values with one predicate, and keeps the pinned grep contract (placeholder count 1 before the flip, 0 after) intact; a real https hostname passes the check and polling starts. (Original rationale unchanged: a bare placeholder resolves as a RELATIVE url against github.io, and fetch resolves on a 404.)
2. Else poll `RENDER_URL + "/feed"` every 5 000 ms, each fetch under an AbortController timeout (~4 s). **Live means ALL of:** the fetch resolved AND `response.ok` AND `JSON.parse` of the body succeeds AND the object contains a `produced` key. A CORS rejection (TypeError), network error, timeout, non-ok status, or unparseable body (Render's own holding page) ALL count as still-waking — never as live, never as an error state.
3. While waking: "waking the showcase — free-tier instances sleep when idle; first wake takes about a minute" + a running elapsed counter (R6 narrated as a feature).
4. First live response → `location = RENDER_URL` (ledger 1: swap-in = redirect, one page implementation).
5. Elapsed > 300 000 ms → degrade to the fallback: "the showcase is not currently hosted — run it yourself: `go run ./cmd/showcase`" + repo link. (5 s ≪ the ~1-min wake; 5 min = 5× the stated wake — both derived from the feasibility receipt, not invented.)

Verify beyond greps: open the committed file directly in a browser — the fallback must appear IMMEDIATELY with zero network requests (the short-circuit observed); this is the same rendering the kill path ships unchanged.

## §Y — render.yaml, exact content (D-SL7-7 + ledger 4/14)

```yaml
services:
  - type: web
    runtime: go
    name: mini-kafka-showcase
    plan: free
    buildCommand: go build -o showcase ./cmd/showcase
    startCommand: ./showcase
    autoDeploy: false
    envVars:
      - key: SHOWCASE_DISK_CAP_MB
        value: "200"
```

`envVars` MUST be the list-of-objects form shown (a scalar map fails Blueprint parse — seat B). No `healthCheckPath` key anywhere (ledger 14 — a platform pinger would blur the sleeps-when-unwatched story). `name` is a chosen public name, not an account identifier.

## §N — the port-scan script (D-SL7-8)

- **Interface:** `showcase_portscan.sh <hostname> [--ports "<space list>"] [--expected <file>]`. Defaults: ports `443 80 7621 7620 7622 7623 7624 7625 7626 7627 7628 7629 7630 8080 9092 3000 5432 6379`, expected `scripts/showcase_portscan.expected`. Probe: `nc -z -w 3`. Output: one stable line per port, `port NNN: open` / `port NNN: closed`, in argument order. Compare against the expected file; ANY deviation → print the diff, exit non-zero.
- **`showcase_portscan.expected` (live pattern):** 443 open, 80 open (Render's redirect-to-HTTPS edge — noted as expected in a header comment), every other listed port closed.
- **Header comment documents the full per-deploy procedure** (run after EVERY deploy and at slice exit, from a host outside Render; output → `docs/receipts/sl7-portscan.txt`), the two live route-posture probes that ride the same receipt (`curl -X POST https://<host>/feed` → expect 405 · `curl https://<host>/nope` → expect 404), and the honesty note verbatim from D-SL7-8: the scan witnesses that RENDER'S EDGE exposes exactly 443/80 — the loopback bind's actual proof is the config-literal unit test; the scan exists so the exposure claim is WITNESSED per deploy, not deduced (G-SL7-2).
- **Self-test (row 10, all three observed by command):** `showcase_portscan.selftest.expected` pins two high localhost ports (e.g. 39471 open, 39472 closed). (1) `python3 -m http.server 39471 &` then `scripts/showcase_portscan.sh 127.0.0.1 --ports "39471 39472" --expected scripts/showcase_portscan.selftest.expected` → exit 0; (2) kill the listener, re-run → non-zero with 39471 named (deviation detected); (3) restart listener, re-run → 0 again. Proves open-detected-open, closed-detected-closed, deviation→non-zero — without the live service.

## §R — README (D-SL7-11; NEVER touch lines 1–10 or the bench marker block, today lines 85–128)

Insert `## Live showcase` immediately after the "Run it by hand" section (before "What it guarantees today", today line 42).

**Variant A — COMMITTED in Phase A (hostname-free; the github.io URL is derivable and permitted per D-SL7-12; no onrender.com string exists yet):** the shim link `https://systemcnu.github.io/mini-kafka/showcase/` as the stable entry point · one honest paragraph: a watch-only, self-driving instance of this broker; the first load can take about a minute while the free instance wakes (R6 as narrative); visitors cannot write to it (U4) · the teardown criterion **verbatim from root DESIGN.md DD-23** (shape per D-SL7-11: if Render's free tier gains a card requirement, starts charging, or the free instance hours run out — the service is deleted and this link reverts to "not currently hosted") · the local line: `go run ./cmd/showcase` then open `127.0.0.1:8080`.

**Variant B — STAGED HERE ONLY, committed only if the kill fires (D-SL7-10 verbatim):** "The showcase is not currently hosted: deploying it required attaching a payment method, and this project's hosting rule is $0 hard (no card, ever — SPEC U8). Run it yourself: `go run ./cmd/showcase`. If a genuinely free no-card tier reappears, `render.yaml` is the deploy, unchanged."

Grep battery (variant A, row 11): `grep -c 'watch-only' README.md` ≥ 1 · `grep -c 'not currently hosted' README.md` ≥ 1 (the criterion names its own fallback) · `grep -c 'required attaching a payment method' README.md` → **0** (variant B's unique discriminator — must NOT appear on the build path) · `go test ./cmd/bench -count=1` green (the armed byte-compare, render_test.go:131 — the ONLY valid proof the marker block survived).

## §Scrub — before EVERY commit that touches any SL7 file (D-SL7-12)

The full patterns are held OUTSIDE the repo and supplied by the integrator at exit (SL6's lesson, now applied to ALL patterns — committing any pattern anywhere, including this plan or a receipt, is itself the violation; that is why this section DESCRIBES and never spells). The builder's pre-commit pass on touched files: no email-shaped literal of any kind, no registration identity, no platform dashboard hostname, no platform-issued ID slugs (the service/team/owner prefixed identifiers), no API keys, no pasted deploy logs. The ONLY two platform strings that will EVER be committed are the public service hostname and the Pages URL — and in Phase A the first does not exist yet (`RENDER_URL_TBD` everywhere, count = 1 in the shim, by grep). Receipts record COUNTS only, never patterns.

## Pitfalls (named so they can be checked)

- **The `"/"` catch-all trap:** a bare `mux.HandleFunc("/", page)` serves the page on EVERY path and silently defeats the 404 rule — the route-enumeration test only proves anything because the pattern is `"GET /{$}"`. If the 404 test passes, the pattern is right; never "fix" the test.
- **HEAD is not 405:** Go's mux treats a `GET` pattern as also matching HEAD. The method tests assert POST/PUT/DELETE → 405 (the design's enumeration) — do NOT assert HEAD → 405; it will fail and the failure is the stdlib, not the surface.
- **Zero-valued http.Server:** `http.ListenAndServe(...)` or `&http.Server{}` has NO timeouts — Slowloris-open on a $0 box. The four-timeout literal in §H is the contract; the nonzero test pins it against the production constructor, not a test copy.
- **fetch resolves on a 404 (the shim):** `fetch` only rejects on network-level failure — a 404/500 RESOLVES. The live predicate MUST check `response.ok` AND JSON-parses AND `produced` present. And the placeholder MUST short-circuit before any fetch: `RENDER_URL_TBD` is a relative URL that resolves against github.io.
- **CORS absence is the keep-waiting signal:** while Render's holding page answers during wake, the cross-origin fetch throws a TypeError (no CORS headers). That is NOT an error state — it is the "still waking" state. Only the §M predicate's full conjunction means live.
- **atomic.Value discipline:** always `Store` the same concrete type (`*snapshot`) — a type change panics at runtime; seed a valid zero snapshot before the listener starts; NEVER mutate a snapshot after `Store` — build fresh, deep-copying every slice, or `-race` in row 7 will name you.
- **Feeder shutdown vs a parked Poll:** `Poll` can park server-side up to its maxWait; a `stop()` that joins goroutines before closing the GroupConsumer deadlocks up to the wait. The §F order (stop channel → `gc.Close()` → join) is load-bearing; the poll loop must treat any post-stop error as clean exit. Every test defers `stop()` — the battery runs `-race` and leaks are failures.
- **MkdirTemp cleanup — tests yes, prod main NO:** tests own their dirs (`t.TempDir` / TMPDIR-owned, per the demo precedent cmd/demo/demo_test.go:44–69). Production `main` deliberately does NOT remove the data dir on exit — Render's ephemeral disk resets it, and restart-fresh (a NEW MkdirTemp per boot) is the design (D-SL7-4). Do not copy the demo's `removeData` sync.OnceFunc into showcase main.
- **The README render gate fails the whole suite** if ANYTHING between `<!-- bench:begin -->` and `<!-- bench:end -->` shifts — including a formatter "fixing" whitespace. Edit outside the markers only; verify with `go test ./cmd/bench -count=1`, never by eyeballing the diff.
- **Counts by command, never memory:** 147 test functions, marker lines 85/128, section line 24 are held by the commands at the top of this plan; if a re-run disagrees, the TREE changed — stop and re-derive, do not "fix" a test or a grep to the remembered number.
- **ZERO production-code changes:** if any `.go` file outside `cmd/showcase/` needs touching to go green, STOP — that is a design-level event (D-SL7-1/13), not a build detail. Same for ci.yml: the check-run count stays 7.
- **Scrub applies to THIS plan and every receipt too:** describe forbidden patterns, never spell them (§Scrub). The DESIGN wrote one of them with interpuncts for exactly this reason.
