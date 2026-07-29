// Exit-checklist item 4's live-cap battery: every D-SL0-8 cap live at SL0
// rejected over the wire with its pinned code, PROD-3's nothing-written
// proof, the connection accept-guard, and the NFR-4 loopback test
// (D-SL0-12).
package broker

import (
	"encoding/binary"
	"io"
	"net"
	"strconv"
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

	// Over-cap conn: accepted then closed without service (SL0 accept-guard;
	// the served-error-frame polish is SL4's).
	over := dialBroker(t, srv)
	over.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := wire.ReadFrame(over, wire.MaxResponseFrame); err != io.EOF {
		t.Fatalf("over-cap conn: read = %v, want io.EOF (guard close)", err)
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
