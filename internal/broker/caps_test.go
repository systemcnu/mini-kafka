// Live-cap battery over real TCP: every D-SL0-8 cap rejected with its pinned
// code, PROD-3's nothing-written proof, the NFR-4 loopback test, plus SL4's
// rows — conn-cap served frame (D-SL4-2), idle reclaim (D-SL4-3), the
// malformed table (D-SL4-4), and group caps at the wire (D-SL4-5).
package broker

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/internal/wire"
)

func TestOversizedFrameRejectedThenClosed(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)

	// Hand-built header claiming a body that would push the total frame
	// over the request cap; the broker must reject BEFORE reading it.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], wire.MaxRequestFrame) // 4+len > cap
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	typ, body, err := wire.ReadFrame(conn, wire.MaxResponseFrame)
	if err != nil {
		t.Fatalf("reading rejection: %v", err)
	}
	em, werr := wire.DecodeErrorMsg(body)
	if typ != wire.TypeError || werr != nil || em.Code != uint16(wire.CodeFrameTooLarge) {
		t.Fatalf("got type %d code %v, want Error{FRAME_TOO_LARGE}", typ, em)
	}
	// The stream is not trustworthy past an oversized frame: conn closes.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := wire.ReadFrame(conn, wire.MaxResponseFrame); err != io.EOF {
		t.Fatalf("after oversize: err = %v, want io.EOF (closed)", err)
	}
}

func TestOversizedPayloadRejectedAndNothingWritten(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "prodthree", 1)

	big := make([]byte, MaxPayload+1)
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "prodthree", Partition: 0, Payload: big}.Encode(),
		wire.CodeMsgTooLarge)

	// PROD-3: the rejected payload wrote NOTHING — the next record gets
	// offset 0 and is the only one served.
	if off := mustProduce(t, conn, "prodthree", 0, "small"); off != 0 {
		t.Fatalf("offset after rejected produce = %d, want 0", off)
	}
	recs := mustFetch(t, conn, "prodthree", 0, 0, 1000)
	if len(recs) != 1 || string(recs[0].Payload) != "small" {
		t.Fatalf("log contains %v, want only the small record", recs)
	}
}

func fetchBody(entries []wire.FetchEntry, maxWaitMs, maxBytes uint32) []byte {
	return wire.Fetch{Topic: "caps", Entries: entries, MaxWaitMs: maxWaitMs, MaxBytes: maxBytes}.Encode()
}

func TestFetchCapRejections(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "caps", 1)
	one := []wire.FetchEntry{{Partition: 0, Offset: 0}}

	// maxWait over cap → CAP_EXCEEDED.
	expectError(t, conn, wire.TypeFetch, fetchBody(one, MaxFetchWaitMs+1, 0), wire.CodeCapExceeded)
	// maxBytes over cap → CAP_EXCEEDED.
	expectError(t, conn, wire.TypeFetch, fetchBody(one, 1, MaxFetchBytes+1), wire.CodeCapExceeded)
	// entries 2..16 are SERVED since SL2 lifted the G7 guard (D-SL2-7) —
	// this row asserts the lift so a regression to CAP_EXCEEDED is caught.
	two := []wire.FetchEntry{{Partition: 0, Offset: 0}, {Partition: 0, Offset: 0}}
	if typ, _ := roundtrip(t, conn, wire.TypeFetch, fetchBody(two, 1, 0)); typ != wire.TypeFetchResp {
		t.Fatalf("2-entry fetch response type %d, want FetchResp (G7 lifted)", typ)
	}
	// entries > 16 → FETCH_TOO_WIDE (checked before the >1 rule).
	var many []wire.FetchEntry
	for i := 0; i < MaxFetchEntries+1; i++ {
		many = append(many, wire.FetchEntry{Partition: 0, Offset: 0})
	}
	expectError(t, conn, wire.TypeFetch, fetchBody(many, 1, 0), wire.CodeFetchTooWide)
	// zero entries → MALFORMED.
	expectError(t, conn, wire.TypeFetch, fetchBody(nil, 1, 0), wire.CodeMalformed)
}

func TestCreateTopicCapRejections(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)

	// Partition count outside 1..16 → CAP_EXCEEDED.
	expectError(t, conn, wire.TypeCreateTopic,
		wire.CreateTopic{Topic: "zero", Partitions: 0}.Encode(), wire.CodeCapExceeded)
	expectError(t, conn, wire.TypeCreateTopic,
		wire.CreateTopic{Topic: "wide", Partitions: 17}.Encode(), wire.CodeCapExceeded)

	// Invalid names → INVALID_NAME, before any path is formed (DD-18).
	expectError(t, conn, wire.TypeCreateTopic,
		wire.CreateTopic{Topic: "Bad^Name", Partitions: 1}.Encode(), wire.CodeInvalidName)
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "../escape", Partition: 0, Payload: []byte("x")}.Encode(), wire.CodeInvalidName)
	expectError(t, conn, wire.TypeFetch,
		wire.Fetch{Topic: "UPPER", Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 1}.Encode(), wire.CodeInvalidName)
}

func TestTopicCountCap(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	for i := 0; i < 64; i++ {
		mustCreateTopic(t, conn, "t"+strconv.Itoa(i), 1)
	}
	expectError(t, conn, wire.TypeCreateTopic,
		wire.CreateTopic{Topic: "onetoomany", Partitions: 1}.Encode(), wire.CodeCapExceeded)
}

func TestConnectionCapGuard(t *testing.T) {
	if DefaultMaxConns != 256 {
		t.Fatalf("DefaultMaxConns = %d, want the pinned 256", DefaultMaxConns)
	}
	// A small cap keeps the test inside fd limits; the guard logic is the
	// same code path as the 256 default.
	srv, err := New(Config{Addr: "127.0.0.1:0", DataDir: t.TempDir(), MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	open := make([]net.Conn, 0, 4)
	for i := 0; i < 4; i++ {
		c := dialBroker(t, srv)
		// Prove the conn is actually served, not just accepted.
		if typ, _ := roundtrip(t, c, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
			t.Fatalf("conn %d not served", i)
		}
		open = append(open, c)
	}

	// Over-cap conn: DD-24's accept → write error → close (D-SL4-2) — it
	// READS the served CAP_EXCEEDED frame, then EOF.
	over := dialBroker(t, srv)
	over.SetReadDeadline(time.Now().Add(3 * time.Second))
	typ, body, err := wire.ReadFrame(over, wire.MaxResponseFrame)
	if err != nil {
		t.Fatalf("over-cap conn: reading the served frame: %v", err)
	}
	em, werr := wire.DecodeErrorMsg(body)
	if typ != wire.TypeError || werr != nil || em.Code != uint16(wire.CodeCapExceeded) {
		t.Fatalf("over-cap conn got type %d %+v, want Error{CAP_EXCEEDED}", typ, em)
	}
	if _, _, err := wire.ReadFrame(over, wire.MaxResponseFrame); err != io.EOF {
		t.Fatalf("after the cap frame: read = %v, want io.EOF", err)
	}
	// Existing under-cap conns are unaffected by the rejection.
	if typ, _ := roundtrip(t, open[3], wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("under-cap conn disturbed by the over-cap rejection")
	}
	// The guard must decrement on conn exit — otherwise the cap wedges the
	// broker after MaxConns total conns ever (PLAN pitfall).
	open[0].Close()
	waitForCond(t, func() bool {
		c, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			return false
		}
		defer c.Close()
		if err := wire.WriteFrame(c, wire.TypeListTopics, nil); err != nil {
			return false
		}
		c.SetReadDeadline(time.Now().Add(time.Second))
		typ, _, err := wire.ReadFrame(c, wire.MaxResponseFrame)
		return err == nil && typ == wire.TypeListTopicsResp
	}, "a freed slot to admit a new connection")

	// Re-fill the freed slot (retrying: the probe conn's close lands
	// asynchronously), then leave one over-cap conn UNREAD as the test
	// ends: t.Cleanup's Stop races the in-flight writer goroutine —
	// wg-tracked, it must be joined, not leaked (D-SL4-2; -race covers it).
	waitForCond(t, func() bool {
		c := dialBroker(t, srv)
		if err := wire.WriteFrame(c, wire.TypeListTopics, nil); err != nil {
			return false
		}
		c.SetReadDeadline(time.Now().Add(time.Second))
		typ, _, err := wire.ReadFrame(c, wire.MaxResponseFrame)
		return err == nil && typ == wire.TypeListTopicsResp
	}, "the freed slot to refill")
	_ = dialBroker(t, srv)
}

// TestNFR4LoopbackOnlyByDefault pins D-SL0-12's method: under default flags
// the resolved listen address is loopback, and no non-loopback interface
// address serves the port.
func TestNFR4LoopbackOnlyByDefault(t *testing.T) {
	srv, err := New(Config{DataDir: t.TempDir()}) // Addr empty → default
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("binding the default address: %v", err)
	}
	t.Cleanup(srv.Stop)

	tcp, ok := srv.Addr().(*net.TCPAddr)
	if !ok || !tcp.IP.IsLoopback() {
		t.Fatalf("default listener address %v is not loopback", srv.Addr())
	}
	port := strconv.Itoa(tcp.Port)

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	nonLoopback := 0
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		nonLoopback++
		target := net.JoinHostPort(ipnet.IP.String(), port)
		c, err := net.DialTimeout("tcp", target, 500*time.Millisecond)
		if err == nil {
			c.Close()
			t.Errorf("non-loopback %s accepted a connection — NFR-4 violated", target)
			continue
		}
		t.Logf("dial %s correctly failed: %v", target, err)
	}
	if nonLoopback == 0 {
		t.Log("no non-loopback interface addresses present; skipping that leg (per D-SL0-12)")
	}
}

// --- SL4 rows: idle reclaim (D-SL4-3, all 5 §4 rows) ---

// startIdleBroker runs a broker with a short IdleTimeout; maxConns 0 keeps
// the default.
func startIdleBroker(t *testing.T, idle time.Duration, maxConns int) *Server {
	t.Helper()
	s, err := New(Config{Addr: "127.0.0.1:0", DataDir: t.TempDir(), MaxConns: maxConns, IdleTimeout: idle})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(s.Stop)
	return s
}

func TestIdleConnReclaimed(t *testing.T) {
	s := startIdleBroker(t, 200*time.Millisecond, 0)
	conn := dialBroker(t, s)
	if typ, _ := roundtrip(t, conn, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("conn not served before idling")
	}
	// Go idle past the window: the broker closes silently — EOF, never a
	// farewell frame (G-SL4-1: the peer is absent by definition).
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	typ, _, err := wire.ReadFrame(conn, wire.MaxResponseFrame)
	if err == nil {
		t.Fatalf("idle conn was served a frame (type %d), want silent close", typ)
	}
	if err != io.EOF {
		t.Fatalf("idle conn read = %v, want io.EOF (reclaimed)", err)
	}
	// The broker still serves new conns after the reclaim.
	fresh := dialBroker(t, s)
	if typ, _ := roundtrip(t, fresh, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("broker not serving after idle reclaim")
	}
}

// TestHeartbeatingConnSurvivesIdleWindow proves D-SL2-11's exemption by
// arithmetic, not by any special-casing: a control conn beating well inside
// the window crosses many idle windows unharmed.
func TestHeartbeatingConnSurvivesIdleWindow(t *testing.T) {
	s := startIdleBroker(t, 500*time.Millisecond, 0)
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "hb", 1)
	join := mustJoinGroup(t, conn, "beaters", "hb")
	deadline := time.Now().Add(2 * time.Second) // 4 idle windows
	for time.Now().Before(deadline) {
		mustHeartbeat(t, conn, "beaters", join.MemberID, join.Generation)
		time.Sleep(120 * time.Millisecond)
	}
	// Still a served, live member after all the windows.
	if hb := mustHeartbeat(t, conn, "beaters", join.MemberID, join.Generation); hb.Flags&wire.HeartbeatRejoin != 0 {
		t.Fatal("solo member sees REJOIN after the idle windows")
	}
}

// TestIdleReclaimFreesCapSlots is DD-24's cap-protection claim: idle conns
// cannot hold the connection cap hostage.
func TestIdleReclaimFreesCapSlots(t *testing.T) {
	s := startIdleBroker(t, 300*time.Millisecond, 4)
	for i := 0; i < 4; i++ {
		c := dialBroker(t, s)
		if typ, _ := roundtrip(t, c, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
			t.Fatalf("conn %d not served", i)
		}
		// ...and left idle, never closed by the test.
	}
	// Within one idle window the slots free up: a NEW conn is admitted and
	// served.
	waitForCond(t, func() bool {
		c := dialBroker(t, s)
		if err := wire.WriteFrame(c, wire.TypeListTopics, nil); err != nil {
			return false
		}
		c.SetReadDeadline(time.Now().Add(time.Second))
		typ, _, err := wire.ReadFrame(c, wire.MaxResponseFrame)
		return err == nil && typ == wire.TypeListTopicsResp
	}, "idle reclaim to free a cap slot")
}

// TestParkedFetchSurvivesIdleTimeout is the adversarial deadline-never-
// spans-a-park proof (D-SL4-3): the read deadline is re-armed at the top of
// each serve iteration, and a park (no conn reads) far past IdleTimeout
// still completes on its conn.
func TestParkedFetchSurvivesIdleTimeout(t *testing.T) {
	s := startIdleBroker(t, 200*time.Millisecond, 0)
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "parky", 1)
	// Park at the empty tail with maxWait 5× the idle window.
	req := wire.Fetch{Topic: "parky", Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 1000}
	if err := wire.WriteFrame(conn, wire.TypeFetch, req.Encode()); err != nil {
		t.Fatal(err)
	}
	p, err := s.store.Partition("parky", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool { return p.ParkedWaiters() == 1 }, "fetch to park")

	// Wake it three idle windows into the park.
	time.Sleep(600 * time.Millisecond)
	prod := dialBroker(t, s)
	mustProduce(t, prod, "parky", 0, "late")

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	typ, body, rerr := wire.ReadFrame(conn, wire.MaxResponseFrame)
	if rerr != nil {
		t.Fatalf("parked fetch died under IdleTimeout < maxWait: %v", rerr)
	}
	if typ != wire.TypeFetchResp {
		t.Fatalf("woken fetch response type %d, want FetchResp", typ)
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		t.Fatal(werr)
	}
	if len(resp.Groups) != 1 || len(resp.Groups[0].Recs) != 1 || string(resp.Groups[0].Recs[0].Payload) != "late" {
		t.Fatalf("woken resp = %+v, want the late record", resp.Groups)
	}
	// The conn survives the long park and serves again.
	conn.SetReadDeadline(time.Time{})
	if typ, _ := roundtrip(t, conn, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("conn dead after the long park")
	}
}

// TestStalledReaderReclaimedByWriteDeadline: F3's sabotage-resistant sizing —
// the response overruns loopback socket buffers so WriteFrame genuinely
// blocks, and only the WRITE deadline can free it. Removing the write
// deadline must turn this red (receipted in sl4-red-green.txt).
func TestStalledReaderReclaimedByWriteDeadline(t *testing.T) {
	s := startIdleBroker(t, 300*time.Millisecond, 0)
	setup := dialBroker(t, s)
	mustCreateTopic(t, setup, "stall", 1)
	payload := strings.Repeat("x", 900<<10)
	total := 0
	for i := 0; i < 4; i++ {
		mustProduce(t, setup, "stall", 0, payload)
		total += len(payload)
	}

	// Request all ~3.6 MiB, then never read: the broker's WriteFrame blocks
	// once kernel buffers fill, and the write deadline reclaims the conn.
	stall := dialBroker(t, s)
	req := wire.Fetch{Topic: "stall", Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 1000, MaxBytes: MaxFetchBytes}
	if err := wire.WriteFrame(stall, wire.TypeFetch, req.Encode()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // several write-deadline windows

	// Drain: if the deadline fired, the response was cut and the conn
	// closed — far fewer bytes than the full response arrive. WITHOUT the
	// write deadline this very drain unblocks the parked WriteFrame and the
	// full response lands, failing the assertion.
	stall.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, _ := io.Copy(io.Discard, stall)
	if n >= int64(total) {
		t.Fatalf("drained %d bytes ≥ the %d-byte response — the stalled conn was never reclaimed", n, total)
	}
	// The broker still serves.
	fresh := dialBroker(t, s)
	if typ, _ := roundtrip(t, fresh, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("broker not serving after reclaiming the stalled reader")
	}
}

// --- SL4 rows: group caps at the wire (D-SL4-5) + fetch bad-partition (CF4) ---

// TestMemberCapRejectionServedOverWire: the 33rd member's join is answered
// by a served CAP_EXCEEDED frame. Raw-wire conns never heartbeat and the
// 2 s session window is unseamable at broker level (F9): the fill count is
// asserted — all 32 members heartbeat OK — immediately before the over-cap
// attempt, so a sweep fails loudly, never as a silent pass.
func TestMemberCapRejectionServedOverWire(t *testing.T) {
	s := startBroker(t, t.TempDir())
	setup := dialBroker(t, s)
	mustCreateTopic(t, setup, "orders", 1)
	// One conn per member: a join on a conn already bound to a live member
	// of the group is that member's re-Join, never a new member (D-SL2-3b).
	conns := make([]net.Conn, 0, 32)
	members := make([]wire.JoinGroupResp, 0, 32)
	for i := 0; i < 32; i++ {
		c := dialBroker(t, s)
		conns = append(conns, c)
		members = append(members, mustJoinGroup(t, c, "packed", "orders"))
	}
	// Fill count asserted at the wire: every member still heartbeats OK.
	for i, m := range members {
		mustHeartbeat(t, conns[i], "packed", m.MemberID, m.Generation)
	}
	over := dialBroker(t, s)
	expectError(t, over, wire.TypeJoinGroup,
		wire.JoinGroup{Group: "packed", Topic: "orders"}.Encode(), wire.CodeCapExceeded)
	// Non-closing rejection: the same conn serves afterwards.
	if typ, _ := roundtrip(t, over, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("conn dead after served CAP_EXCEEDED")
	}
}

// TestGroupCapRejectionServedOverWire: the 65th group's join gets a served
// CAP_EXCEEDED frame. ONE conn holds all 64 memberships (the coordinator's
// conn binding is per-(conn, group) — verified at that seam), and groups
// are never evicted, so 64 successful join responses ARE the fill count: no
// sweep can un-fill this cap.
func TestGroupCapRejectionServedOverWire(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "orders", 1)
	for i := 0; i < 64; i++ {
		mustJoinGroup(t, conn, "g"+strconv.Itoa(i), "orders")
	}
	expectError(t, conn, wire.TypeJoinGroup,
		wire.JoinGroup{Group: "onetoomany", Topic: "orders"}.Encode(), wire.CodeCapExceeded)
	if typ, _ := roundtrip(t, conn, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("conn dead after served CAP_EXCEEDED")
	}
}

// TestFetchBadPartitionRejected (CF4): the SLICES-bolded input has fetch-side
// per-cap coverage outside the battery, sabotage-visible.
func TestFetchBadPartitionRejected(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "narrow", 1)
	expectError(t, conn, wire.TypeFetch,
		wire.Fetch{Topic: "narrow", Entries: []wire.FetchEntry{{Partition: 1, Offset: 0}}, MaxWaitMs: 1}.Encode(),
		wire.CodeBadPartition)
	// Non-closing: the same conn serves.
	if typ, _ := roundtrip(t, conn, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("conn dead after BAD_PARTITION")
	}
}

// --- SL4 rows: the broker-level malformed table (D-SL4-4) ---

// rawBytes hand-builds an envelope so the table can send invalid ones.
func rawBytes(length uint32, rest ...byte) []byte {
	b := make([]byte, 4+len(rest))
	binary.BigEndian.PutUint32(b, length)
	copy(b[4:], rest)
	return b
}

// TestMalformedTableOverWire proves the SERVED frame and the broker's
// survival per row — what PROT-3's check text demands beyond the wire-layer
// decode tables: every body-bearing type × {truncated, trailing} is a
// non-closing MALFORMED (same conn serves after); ListTopics' one row is
// any-non-empty-body (F6 — its valid body is empty, truncation does not
// exist); the four frame-level rows close the conn (untrustworthy stream),
// so a NEW conn proves survival.
func TestMalformedTableOverWire(t *testing.T) {
	s := startBroker(t, t.TempDir())

	bodies := []struct {
		name string
		typ  byte
		enc  []byte
	}{
		{"Produce", wire.TypeProduce, wire.Produce{Topic: "t", Partition: 0, Payload: []byte("x")}.Encode()},
		{"Fetch", wire.TypeFetch, wire.Fetch{Topic: "t", Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 1, MaxBytes: 1}.Encode()},
		{"CreateTopic", wire.TypeCreateTopic, wire.CreateTopic{Topic: "t", Partitions: 1}.Encode()},
		{"JoinGroup", wire.TypeJoinGroup, wire.JoinGroup{Group: "g", Topic: "t"}.Encode()},
		{"Heartbeat", wire.TypeHeartbeat, wire.Heartbeat{Group: "g", MemberID: "m1", Generation: 1}.Encode()},
		{"CommitOffsets", wire.TypeCommitOffsets, wire.CommitOffsets{Group: "g", MemberID: "m1", Generation: 1, Entries: []wire.CommitEntry{{Partition: 0, Next: 1}}}.Encode()},
		{"LeaveGroup", wire.TypeLeaveGroup, wire.LeaveGroup{Group: "g", MemberID: "m1"}.Encode()},
		{"GroupFetch", wire.TypeGroupFetch, wire.GroupFetch{Group: "g", MemberID: "m1", Generation: 1, Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 1, MaxBytes: 1}.Encode()},
	}
	conn := dialBroker(t, s)
	for _, tc := range bodies {
		expectError(t, conn, tc.typ, tc.enc[:len(tc.enc)-1], wire.CodeMalformed)                    // truncated body
		expectError(t, conn, tc.typ, append(bytes.Clone(tc.enc), 0x00), wire.CodeMalformed)         // trailing byte
		if typ, _ := roundtrip(t, conn, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp { // same conn serves
			t.Fatalf("%s: conn dead after body-level MALFORMED", tc.name)
		}
	}
	// ListTopics' special row (F6): any non-empty body → MALFORMED.
	expectError(t, conn, wire.TypeListTopics, []byte{0x00}, wire.CodeMalformed)
	if typ, _ := roundtrip(t, conn, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("conn dead after the ListTopics body row")
	}

	// Frame-level rows: served Error, then close.
	var oversize [4]byte
	binary.BigEndian.PutUint32(oversize[:], wire.MaxRequestFrame)
	frames := []struct {
		name string
		raw  []byte
		code wire.Code
	}{
		// Header only — an extra unread byte would turn the close into an
		// RST and the EOF assertion into a flake.
		{"below-min length", rawBytes(1), wire.CodeMalformed},
		{"bad version", rawBytes(2, wire.Version+1, wire.TypeProduce), wire.CodeMalformed},
		{"unknown type", nil, wire.CodeMalformed}, // sent via WriteFrame below
		{"oversized frame", oversize[:], wire.CodeFrameTooLarge},
	}
	for _, tc := range frames {
		c := dialBroker(t, s)
		if tc.raw != nil {
			if _, err := c.Write(tc.raw); err != nil {
				t.Fatalf("%s: write: %v", tc.name, err)
			}
		} else if err := wire.WriteFrame(c, 99, nil); err != nil {
			t.Fatalf("%s: write: %v", tc.name, err)
		}
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		typ, body, err := wire.ReadFrame(c, wire.MaxResponseFrame)
		if err != nil {
			t.Fatalf("%s: reading the served frame: %v", tc.name, err)
		}
		em, werr := wire.DecodeErrorMsg(body)
		if typ != wire.TypeError || werr != nil || em.Code != uint16(tc.code) {
			t.Fatalf("%s: got type %d %+v, want Error{%d}", tc.name, typ, em, tc.code)
		}
		if _, _, err := wire.ReadFrame(c, wire.MaxResponseFrame); err != io.EOF {
			t.Fatalf("%s: conn open after a frame-level rejection: %v", tc.name, err)
		}
		// A NEW conn proves the broker survived.
		nc := dialBroker(t, s)
		if typ, _ := roundtrip(t, nc, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
			t.Fatalf("%s: broker not serving on a new conn", tc.name)
		}
	}
}
