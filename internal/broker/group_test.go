// SL2 broker suite over real loopback TCP — step 4: the multi-entry fetch
// loop (D-SL2-7): CONS-1's G7 closure (2..16 entries served), wake on ANY
// listed partition's FRONTIER ADVANCE (not raw append), the one-group-per-
// entry empty shape (F7), request-order maxBytes budgeting, and the
// ParkedWaiters leak check. Step 5 adds the fencing suite (D-SL2-6).
package broker

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/wire"
)

// gateSyncer blocks the flusher's covering fsync while armed — the window
// where a raw append has happened but the frontier has NOT advanced.
type gateSyncer struct {
	mu      sync.Mutex
	gate    chan struct{}
	entered chan struct{}
}

func newGateSyncer() *gateSyncer { return &gateSyncer{entered: make(chan struct{}, 64)} }

func (g *gateSyncer) Sync(f storage.File) error {
	g.mu.Lock()
	gate := g.gate
	g.mu.Unlock()
	select {
	case g.entered <- struct{}{}:
	default:
	}
	if gate != nil {
		<-gate
	}
	return storage.FileSyncer{}.Sync(f)
}

// Arm gates the next Syncs and drains stale entered tokens.
func (g *gateSyncer) Arm() {
	g.mu.Lock()
	g.gate = make(chan struct{})
	g.mu.Unlock()
	for {
		select {
		case <-g.entered:
		default:
			return
		}
	}
}

// Release opens the gate; idempotent so t.Cleanup can double up.
func (g *gateSyncer) Release() {
	g.mu.Lock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
	g.mu.Unlock()
}

func (g *gateSyncer) waitSyncEntered(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("flusher never entered the covering fsync")
	}
}

// mustMultiFetch sends a raw multi-entry Fetch and decodes the FetchResp.
func mustMultiFetch(t *testing.T, conn net.Conn, topic string, entries []wire.FetchEntry, maxWaitMs, maxBytes uint32) wire.FetchResp {
	t.Helper()
	req := wire.Fetch{Topic: topic, Entries: entries, MaxWaitMs: maxWaitMs, MaxBytes: maxBytes}
	typ, body := roundtrip(t, conn, wire.TypeFetch, req.Encode())
	if typ != wire.TypeFetchResp {
		t.Fatalf("multi-entry fetch response type %d, body %x — want FetchResp", typ, body)
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		t.Fatalf("decode multi fetch resp: %v", werr)
	}
	return resp
}

// TestMultiEntryFetchServesAcrossPartitions closes SL0's G7: a raw
// 2-entry fetch is served (no CAP_EXCEEDED), one group per entry in
// request order, each with its partition's records.
func TestMultiEntryFetchServesAcrossPartitions(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "multi", 2)
	mustProduce(t, conn, "multi", 0, "a0")
	mustProduce(t, conn, "multi", 0, "a1")
	mustProduce(t, conn, "multi", 1, "b0")

	resp := mustMultiFetch(t, conn, "multi",
		[]wire.FetchEntry{{Partition: 0, Offset: 0}, {Partition: 1, Offset: 0}}, 1000, 0)
	if len(resp.Groups) != 2 {
		t.Fatalf("groups = %d, want 2 (one per entry)", len(resp.Groups))
	}
	if resp.Groups[0].Partition != 0 || len(resp.Groups[0].Recs) != 2 ||
		string(resp.Groups[0].Recs[0].Payload) != "a0" || string(resp.Groups[0].Recs[1].Payload) != "a1" {
		t.Fatalf("group 0 = %+v, want partition 0 [a0 a1]", resp.Groups[0])
	}
	if resp.Groups[1].Partition != 1 || len(resp.Groups[1].Recs) != 1 ||
		string(resp.Groups[1].Recs[0].Payload) != "b0" {
		t.Fatalf("group 1 = %+v, want partition 1 [b0]", resp.Groups[1])
	}
}

// TestMultiEntryFetchWakesOnAnyPartitionNotRawAppend extends SL0's wake
// test through the new loop: a parked 2-entry fetch stays parked while a
// listed partition's append sits un-fsynced, wakes on the frontier
// advance, and the park goroutines all exit (ParkedWaiters → 0).
func TestMultiEntryFetchWakesOnAnyPartitionNotRawAppend(t *testing.T) {
	gate := newGateSyncer()
	s, err := newWithFS(Config{Addr: "127.0.0.1:0", DataDir: t.TempDir()}, storage.OSFS(), gate)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	t.Cleanup(gate.Release)

	setup := dialBroker(t, s)
	mustCreateTopic(t, setup, "wakey", 2)
	p0, err := s.store.Partition("wakey", 0)
	if err != nil {
		t.Fatal(err)
	}
	p1, err := s.store.Partition("wakey", 1)
	if err != nil {
		t.Fatal(err)
	}

	// Park a 2-entry fetch at both (empty) tails.
	parked := dialBroker(t, s)
	req := wire.Fetch{Topic: "wakey", Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}, {Partition: 1, Offset: 0}}, MaxWaitMs: 10_000}
	if err := wire.WriteFrame(parked, wire.TypeFetch, req.Encode()); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool { return p0.ParkedWaiters() == 1 && p1.ParkedWaiters() == 1 },
		"both entries to park")

	// Gate the fsync, then produce to partition 1: raw append lands, the
	// frontier does NOT advance.
	gate.Arm()
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		prod := dialBroker(t, s)
		mustProduce(t, prod, "wakey", 1, "wake-me")
	}()
	gate.waitSyncEntered(t)

	// No wake on raw append: the parked fetch must produce NO response yet.
	parked.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if typ, _, rerr := wire.ReadFrame(parked, wire.MaxResponseFrame); rerr == nil {
		t.Fatalf("parked fetch answered (type %d) on raw append, before the covering fsync", typ)
	} else {
		var nerr net.Error
		if !errors.As(rerr, &nerr) || !nerr.Timeout() {
			t.Fatalf("expected a read timeout while parked, got %v", rerr)
		}
	}
	if p0.ParkedWaiters() != 1 || p1.ParkedWaiters() != 1 {
		t.Fatalf("parked waiters = %d/%d during gated fsync, want 1/1", p0.ParkedWaiters(), p1.ParkedWaiters())
	}

	// Release: frontier advances on partition 1 → the fetch wakes with one
	// group per entry, partition 0 empty, partition 1 carrying the record.
	gate.Release()
	parked.SetReadDeadline(time.Now().Add(5 * time.Second))
	typ, body, rerr := wire.ReadFrame(parked, wire.MaxResponseFrame)
	if rerr != nil {
		t.Fatalf("reading woken fetch response: %v", rerr)
	}
	if typ != wire.TypeFetchResp {
		t.Fatalf("woken response type %d, want FetchResp", typ)
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		t.Fatal(werr)
	}
	if len(resp.Groups) != 2 ||
		resp.Groups[0].Partition != 0 || len(resp.Groups[0].Recs) != 0 ||
		resp.Groups[1].Partition != 1 || len(resp.Groups[1].Recs) != 1 ||
		string(resp.Groups[1].Recs[0].Payload) != "wake-me" {
		t.Fatalf("woken resp = %+v, want [p0:empty p1:[wake-me]]", resp.Groups)
	}
	<-prodDone

	// Leak check: every park goroutine exited (D-SL2-7's teardown pin).
	waitForCond(t, func() bool { return p0.ParkedWaiters() == 0 && p1.ParkedWaiters() == 0 },
		"park goroutines to exit")
}

// TestMultiEntryEmptyShapeOneGroupPerEntry pins F7: a timed-out
// multi-entry fetch answers with exactly one zero-rec group per requested
// entry, in request order.
func TestMultiEntryEmptyShapeOneGroupPerEntry(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "shape", 3)

	order := []uint32{2, 0, 1} // deliberately not sorted: request order must survive
	entries := make([]wire.FetchEntry, 0, len(order))
	for _, p := range order {
		entries = append(entries, wire.FetchEntry{Partition: p, Offset: 0})
	}
	resp := mustMultiFetch(t, conn, "shape", entries, 1, 0)
	if len(resp.Groups) != 3 {
		t.Fatalf("groups = %d, want 3 (one per entry)", len(resp.Groups))
	}
	for i, g := range resp.Groups {
		if g.Partition != order[i] {
			t.Errorf("group %d partition = %d, want %d (request order)", i, g.Partition, order[i])
		}
		if len(g.Recs) != 0 {
			t.Errorf("group %d has %d recs, want 0 (empty-at-timeout shape)", i, len(g.Recs))
		}
	}
}

// TestMultiEntryBudgetIsRequestOrder: maxBytes budgets the WHOLE response
// in request order — a later entry gets zero records once the budget is
// spent, and min-one applies to the response, not per entry (D-SL2-7).
func TestMultiEntryBudgetIsRequestOrder(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "budget", 2)
	payload := make([]byte, 100)
	for i := range payload {
		payload[i] = 'x'
	}
	for i := 0; i < 2; i++ {
		mustProduce(t, conn, "budget", 0, string(payload))
		mustProduce(t, conn, "budget", 1, string(payload))
	}

	// One record encodes to 12+100 bytes; 150 admits exactly one.
	resp := mustMultiFetch(t, conn, "budget",
		[]wire.FetchEntry{{Partition: 0, Offset: 0}, {Partition: 1, Offset: 0}}, 1000, 150)
	if len(resp.Groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(resp.Groups))
	}
	if got := len(resp.Groups[0].Recs); got != 1 {
		t.Fatalf("first entry served %d recs, want 1 (budget 150)", got)
	}
	if got := len(resp.Groups[1].Recs); got != 0 {
		t.Fatalf("second entry served %d recs, want 0 — budget must be spent in request order", got)
	}
}
