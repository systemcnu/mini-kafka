// Scenario H battery (D-SL4-1 leg 2): every registry code elicited from live
// brokers over real TCP, union == wire.AllCodes(), broker-still-serving after
// every non-terminal elicitation. Three instances (F2/CF2): one shared OSFS
// broker for the 11 non-terminal codes · WRITE_FAILED on a dedicated FaultFS
// broker with the fault scoped to the target topic's own log (F4) ·
// SHUTTING_DOWN last, on a throwaway broker under a concurrent-Stop hammer
// (≥1 code-10 frame before EOF). The drain window is SCRIPTED open through
// the Syncer seam (a slow fsync keeps an ack pending, so Drain must wait) —
// racing the natural window loses under machine load: the serialized hammer
// missed 20/20 attempts in a loaded full-suite run.
//
// Cap inventory audit table (NFR-2's check, DD-16: 12 input caps + the
// structural in-flight row, G-SL4-2). Cap → enforcing code path → rejection
// test:
//
//	 1 payload ≤ 1 MiB      handlers.go handleProduce (MaxPayload)     TestOversizedPayloadRejectedAndNothingWritten
//	 2 request frame        wire ReadFrame (MaxRequestFrame)           TestOversizedFrameRejectedThenClosed
//	 3 response frame       receiving side: client ReadFrame at        wire TestReadFrameTooLarge; sending side
//	                        MaxResponseFrame (G-SL4-3)                 respected by construction (fetch budget
//	                                                                   ≪ headroom) — no broker-side test forces
//	                                                                   an oversized response into existence
//	 4 fetch maxBytes       handlers.go (MaxFetchBytes)                TestFetchCapRejections
//	 5 fetch maxWait        handlers.go (MaxFetchWaitMs)               TestFetchCapRejections
//	 6 fetch entries ≤ 16   handlers.go (MaxFetchEntries)              TestFetchCapRejections / TestGroupFetchCaps
//	 7 connections ≤ 256    server.go acceptLoop (maxConns)            TestConnectionCapGuard (served frame,
//	                                                                   D-SL4-2; slots reclaimable per
//	                                                                   TestIdleReclaimFreesCapSlots, D-SL4-3)
//	 8 topics ≤ 64          storage store.go (MaxTopics)               TestTopicCountCap
//	 9 partitions 1..16     storage store.go (MaxPartitionsPerTopic)   TestCreateTopicCapRejections
//	10 groups ≤ 64          group coordinator.go (MaxGroups)           TestGroupCapRejectionServedOverWire
//	11 members/group ≤ 32   group coordinator.go (MaxMembersPerGroup)  TestMemberCapRejectionServedOverWire
//	12 name ≤ 128 bytes     wire names.go (ValidateName)               wire TestValidateName (129-byte row);
//	                                                                   served INVALID_NAME rows in
//	                                                                   TestCreateTopicCapRejections
//	 — in-flight/conn = 1   structural — by construction: DD-15's      none possible (G-SL4-2: nothing to
//	                        serve loop reads the next frame only       reject; a pipelining client's second
//	                        after the response is written              request waits in the kernel buffer)

package broker

import (
	"fmt"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/storage/storagetest"
	"github.com/systemcnu/mini-kafka/internal/wire"
)

func TestScenarioH(t *testing.T) {
	elicited := make(map[wire.Code]bool)
	mark := func(code wire.Code, how string) {
		elicited[code] = true
		t.Logf("elicited code %d — %s", code, how)
	}
	serving := func(c net.Conn, after string) {
		t.Helper()
		if typ, _ := roundtrip(t, c, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
			t.Fatalf("broker not serving after %s", after)
		}
		t.Logf("  broker still serving after %s", after)
	}

	// --- Instance 1: shared OSFS broker — the 11 non-terminal codes ---
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)

	// 1 UNKNOWN_TOPIC: produce to an absent topic.
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "ghost", Partition: 0, Payload: []byte("x")}.Encode(), wire.CodeUnknownTopic)
	mark(wire.CodeUnknownTopic, "produce to absent topic")
	serving(conn, "UNKNOWN_TOPIC")

	// 2 TOPIC_EXISTS: duplicate create.
	mustCreateTopic(t, conn, "h", 1)
	expectError(t, conn, wire.TypeCreateTopic,
		wire.CreateTopic{Topic: "h", Partitions: 1}.Encode(), wire.CodeTopicExists)
	mark(wire.CodeTopicExists, "duplicate create")
	serving(conn, "TOPIC_EXISTS")

	// 3 BAD_PARTITION: produce AND fetch at partition ≥ count.
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "h", Partition: 1, Payload: []byte("x")}.Encode(), wire.CodeBadPartition)
	expectError(t, conn, wire.TypeFetch,
		wire.Fetch{Topic: "h", Entries: []wire.FetchEntry{{Partition: 1, Offset: 0}}, MaxWaitMs: 1}.Encode(),
		wire.CodeBadPartition)
	mark(wire.CodeBadPartition, "produce and fetch at partition ≥ count")
	serving(conn, "BAD_PARTITION")

	// 4 INVALID_NAME: hostile traversal-shaped name.
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "../escape", Partition: 0, Payload: []byte("x")}.Encode(), wire.CodeInvalidName)
	mark(wire.CodeInvalidName, `produce to "../escape"`)
	serving(conn, "INVALID_NAME")

	// 5 MSG_TOO_LARGE naming the cap, partition bytes unchanged (PROD-3).
	mustCreateTopic(t, conn, "big", 1)
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "big", Partition: 0, Payload: make([]byte, MaxPayload+1)}.Encode(),
		wire.CodeMsgTooLarge)
	if off := mustProduce(t, conn, "big", 0, "small"); off != 0 {
		t.Fatalf("offset after rejected oversized produce = %d, want 0 (bytes changed)", off)
	}
	if recs := mustFetch(t, conn, "big", 0, 0, 1000); len(recs) != 1 || string(recs[0].Payload) != "small" {
		t.Fatalf("log after rejected produce = %v, want only the small record", recs)
	}
	mark(wire.CodeMsgTooLarge, "1 MiB+1 payload; partition bytes unchanged")
	serving(conn, "MSG_TOO_LARGE")

	// 7 MALFORMED: truncated body (non-closing — same conn serves).
	enc := wire.Produce{Topic: "h", Partition: 0, Payload: []byte("x")}.Encode()
	expectError(t, conn, wire.TypeProduce, enc[:len(enc)-1], wire.CodeMalformed)
	mark(wire.CodeMalformed, "truncated Produce body")
	serving(conn, "MALFORMED")

	// 8 CAP_EXCEEDED: fetch maxWait over cap.
	expectError(t, conn, wire.TypeFetch,
		wire.Fetch{Topic: "h", Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: MaxFetchWaitMs + 1}.Encode(),
		wire.CodeCapExceeded)
	mark(wire.CodeCapExceeded, "fetch maxWait over cap")
	serving(conn, "CAP_EXCEEDED")

	// 9 FETCH_TOO_WIDE: 17 entries.
	var many []wire.FetchEntry
	for i := 0; i < MaxFetchEntries+1; i++ {
		many = append(many, wire.FetchEntry{Partition: 0, Offset: 0})
	}
	expectError(t, conn, wire.TypeFetch,
		wire.Fetch{Topic: "h", Entries: many, MaxWaitMs: 1}.Encode(), wire.CodeFetchTooWide)
	mark(wire.CodeFetchTooWide, "17-entry fetch")
	serving(conn, "FETCH_TOO_WIDE")

	// 6 FRAME_TOO_LARGE: closing rejection — dedicated conn, survival proven
	// on the untouched main conn.
	fc := dialBroker(t, s)
	if _, err := fc.Write(rawBytes(wire.MaxRequestFrame)); err != nil {
		t.Fatal(err)
	}
	fc.SetReadDeadline(time.Now().Add(3 * time.Second))
	typ, body, err := wire.ReadFrame(fc, wire.MaxResponseFrame)
	if err != nil {
		t.Fatalf("reading FRAME_TOO_LARGE: %v", err)
	}
	if em, werr := wire.DecodeErrorMsg(body); typ != wire.TypeError || werr != nil || em.Code != uint16(wire.CodeFrameTooLarge) {
		t.Fatalf("got type %d %+v, want Error{FRAME_TOO_LARGE}", typ, em)
	}
	mark(wire.CodeFrameTooLarge, "oversized frame header (conn closes after)")
	serving(conn, "FRAME_TOO_LARGE")

	// 12 STALE_GENERATION: the rebalance MUST come from a second conn —
	// same-conn re-joins are re-Joins by design and cannot go stale.
	joinA := mustJoinGroup(t, conn, "workers", "h")
	conn2 := dialBroker(t, s)
	mustJoinGroup(t, conn2, "workers", "h")                             // generation bump
	mustHeartbeat(t, conn, "workers", joinA.MemberID, joinA.Generation) // keep A live
	expectError(t, conn, wire.TypeCommitOffsets,
		commitBody("workers", joinA.MemberID, joinA.Generation, map[uint32]uint64{0: 1}),
		wire.CodeStaleGeneration)
	mark(wire.CodeStaleGeneration, "commit at generation N−1 after a second-conn rebalance")
	serving(conn, "STALE_GENERATION")

	// 13 UNKNOWN_MEMBER: commit from a fabricated member.
	expectError(t, conn, wire.TypeCommitOffsets,
		commitBody("workers", "m999", 2, map[uint32]uint64{0: 1}), wire.CodeUnknownMember)
	mark(wire.CodeUnknownMember, "commit from a fabricated member")
	serving(conn, "UNKNOWN_MEMBER")

	// --- Instance 2: dedicated FaultFS broker — WRITE_FAILED ---
	ffs := storagetest.WrapFS(storage.OSFS())
	fs := startFaultBroker(t, t.TempDir(), ffs)
	fconn := dialBroker(t, fs)
	mustCreateTopic(t, fconn, "wf", 1)
	mustProduce(t, fconn, "wf", 0, "durable")
	// The fault suffix is the target topic's OWN log path (F4): an unscoped
	// "log" suffix would fire on any partition and sticky-degrade it under
	// later subtests.
	ffs.FailWrite(filepath.Join("wf", "0", "log"), 1, 0, syscall.ENOSPC)
	expectError(t, fconn, wire.TypeProduce,
		wire.Produce{Topic: "wf", Partition: 0, Payload: []byte("doomed")}.Encode(), wire.CodeWriteFailed)
	mark(wire.CodeWriteFailed, "produce onto a scoped storage fault")
	// Non-terminal: the degraded broker still serves its durable data.
	if recs := mustFetch(t, fconn, "wf", 0, 0, 1000); len(recs) != 1 || string(recs[0].Payload) != "durable" {
		t.Fatalf("degraded broker served %v, want the durable record", recs)
	}
	serving(fconn, "WRITE_FAILED")

	// --- Instance 3: throwaway broker(s) — the SHUTTING_DOWN Stop-hammer ---
	// Assert ≥1 code-10 frame, never exactly-once, never frame-then-clean-
	// close (F2: the window's edges race conn force-close). The window itself
	// is scripted, not raced: a slow Syncer makes every produce ack's fsync
	// take ~200 ms, so the hammer's in-flight produce holds store.Drain open
	// while draining=true. A single conn cannot COLLECT inside that window —
	// its serve loop is blocked in the produce's dispatch, and after the ack
	// the force-close race is lost under load — so a SECOND conn does the
	// collecting: A parks the slow produce, B hammers cheap requests. If the
	// initial race instead flips draining before A's produce is dispatched,
	// A's own response IS the code-10 — both frames count, so one of the two
	// conns must see it. Retries on fresh brokers stay as a bounded, loud
	// belt-and-braces, never a silent flake.
	elicit10 := func(attempt int) int {
		hs, err := newWithFS(Config{Addr: "127.0.0.1:0", DataDir: t.TempDir()},
			storage.OSFS(), slowSyncer{200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := hs.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(hs.Stop) // idempotent; joins the concurrent Stop below
		connA, err := net.Dial("tcp", hs.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connA.Close() })
		connB, err := net.Dial("tcp", hs.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connB.Close() })
		mustCreateTopic(t, connA, "hammer", 1)
		mustProduce(t, connA, "hammer", 0, "pre-stop") // live service confirmed

		// A parks the drain-occupying produce (response read AFTER the run);
		// only then does Stop start, so the window opens behind a real ack.
		slowBody := wire.Produce{Topic: "hammer", Partition: 0, Payload: []byte("occupier")}.Encode()
		if err := wire.WriteFrame(connA, wire.TypeProduce, slowBody); err != nil {
			t.Fatal(err)
		}
		stopped := make(chan struct{})
		go func() {
			hs.Stop()
			close(stopped)
		}()
		saw := 0
		count := func(typ byte, body []byte) {
			if typ != wire.TypeError {
				return
			}
			if em, werr := wire.DecodeErrorMsg(body); werr == nil && em.Code == uint16(wire.CodeShuttingDown) {
				saw++
			}
		}
		connB.SetReadDeadline(time.Now().Add(15 * time.Second))
		for {
			if err := wire.WriteFrame(connB, wire.TypeListTopics, nil); err != nil {
				break
			}
			typ, body, err := wire.ReadFrame(connB, wire.MaxResponseFrame)
			if err != nil {
				break // EOF/reset: the force-close landed
			}
			count(typ, body)
		}
		connA.SetReadDeadline(time.Now().Add(15 * time.Second))
		if typ, body, err := wire.ReadFrame(connA, wire.MaxResponseFrame); err == nil {
			count(typ, body) // A's produce: ack (case i) or the code-10 itself (case ii)
		}
		<-stopped
		t.Logf("stop-hammer attempt %d: %d code-10 frame(s) before EOF", attempt, saw)
		return saw
	}
	saw10 := 0
	for attempt := 1; attempt <= 20 && saw10 == 0; attempt++ {
		saw10 = elicit10(attempt)
	}
	if saw10 < 1 {
		t.Fatal("no SHUTTING_DOWN frame observed across 20 stop-hammer attempts")
	}
	mark(wire.CodeShuttingDown, fmt.Sprintf("stop hammer, %d code-10 frame(s) before EOF (terminal — no serving assertion)", saw10))

	// --- Completeness: union of elicitations across instances == AllCodes ---
	all := wire.AllCodes()
	for _, c := range all {
		if !elicited[c] {
			t.Errorf("registry code %d never elicited by the battery", c)
		}
	}
	if len(elicited) != len(all) {
		t.Errorf("battery elicited %d codes, registry has %d", len(elicited), len(all))
	}
}

// slowSyncer scripts the Stop-drain window open: each ack's fsync sleeps
// before syncing, so a pending produce occupies store.Drain for its duration.
type slowSyncer struct{ d time.Duration }

func (s slowSyncer) Sync(f storage.File) error {
	time.Sleep(s.d)
	return f.Sync()
}
