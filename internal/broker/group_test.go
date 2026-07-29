// SL2 broker suite over real loopback TCP — step 4: the multi-entry fetch
// loop (D-SL2-7): CONS-1's G7 closure (2..16 entries served), wake on ANY
// listed partition's FRONTIER ADVANCE (not raw append), the one-group-per-
// entry empty shape (F7), request-order maxBytes budgeting, and the
// ParkedWaiters leak check. Step 5 adds the fencing suite (D-SL2-6).
package broker

import (
	"errors"
	"net"
	"strings"
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

// --- Step-5 half: the fencing suite (D-SL2-6, GRP-5, PROT-3 rows) ---

func mustJoinGroup(t *testing.T, conn net.Conn, groupName, topic string) wire.JoinGroupResp {
	t.Helper()
	typ, body := roundtrip(t, conn, wire.TypeJoinGroup, wire.JoinGroup{Group: groupName, Topic: topic}.Encode())
	if typ != wire.TypeJoinGroupResp {
		t.Fatalf("join response type %d, body %x", typ, body)
	}
	resp, werr := wire.DecodeJoinGroupResp(body)
	if werr != nil {
		t.Fatalf("decode join resp: %v", werr)
	}
	return resp
}

func mustHeartbeat(t *testing.T, conn net.Conn, groupName, memberID string, generation uint64) wire.HeartbeatResp {
	t.Helper()
	typ, body := roundtrip(t, conn, wire.TypeHeartbeat, wire.Heartbeat{Group: groupName, MemberID: memberID, Generation: generation}.Encode())
	if typ != wire.TypeHeartbeatResp {
		t.Fatalf("heartbeat response type %d, body %x", typ, body)
	}
	resp, werr := wire.DecodeHeartbeatResp(body)
	if werr != nil {
		t.Fatalf("decode heartbeat resp: %v", werr)
	}
	return resp
}

func commitBody(groupName, memberID string, generation uint64, offsets map[uint32]uint64) []byte {
	m := wire.CommitOffsets{Group: groupName, MemberID: memberID, Generation: generation}
	for p, n := range offsets {
		m.Entries = append(m.Entries, wire.CommitEntry{Partition: p, Next: n})
	}
	return m.Encode()
}

func groupFetchBody(groupName, memberID string, generation uint64, partitions []wire.FetchEntry, maxWaitMs uint32) []byte {
	return wire.GroupFetch{Group: groupName, MemberID: memberID, Generation: generation,
		Entries: partitions, MaxWaitMs: maxWaitMs}.Encode()
}

// nextByPartition indexes a join response's resume offsets.
func nextByPartition(resp wire.JoinGroupResp) map[uint32]uint64 {
	out := make(map[uint32]uint64, len(resp.Assigned))
	for _, a := range resp.Assigned {
		out[a.Partition] = a.NextOffset
	}
	return out
}

// TestGroupJoinFetchCommitLeaveRoundtrip is the happy path over real TCP:
// join carries state (DD-14), GroupFetch serves owned partitions, an acked
// commit is visible at the next join, leave fences the member.
func TestGroupJoinFetchCommitLeaveRoundtrip(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "orders", 4)
	mustProduce(t, conn, "orders", 0, "r0")
	mustProduce(t, conn, "orders", 0, "r1")

	join := mustJoinGroup(t, conn, "workers", "orders")
	if join.MemberID == "" || join.Generation != 1 || len(join.Assigned) != 4 {
		t.Fatalf("join = %+v, want a member owning all 4 partitions at generation 1", join)
	}
	for _, a := range join.Assigned {
		if a.NextOffset != 0 {
			t.Fatalf("fresh group resume offset for partition %d = %d, want 0 (D14 earliest)", a.Partition, a.NextOffset)
		}
	}

	// GroupFetch across all owned partitions on a second conn (the client's
	// fetch-conn shape, DD-19).
	fetchConn := dialBroker(t, s)
	entries := make([]wire.FetchEntry, 0, 4)
	for _, a := range join.Assigned {
		entries = append(entries, wire.FetchEntry{Partition: a.Partition, Offset: a.NextOffset})
	}
	typ, body := roundtrip(t, fetchConn, wire.TypeGroupFetch, groupFetchBody("workers", join.MemberID, join.Generation, entries, 1000))
	if typ != wire.TypeFetchResp {
		t.Fatalf("group fetch response type %d, body %x", typ, body)
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		t.Fatal(werr)
	}
	total := 0
	for _, g := range resp.Groups {
		total += len(g.Recs)
	}
	if len(resp.Groups) != 4 || total != 2 {
		t.Fatalf("group fetch = %d groups %d recs, want 4 groups carrying the 2 records", len(resp.Groups), total)
	}

	// Commit next-to-read for partition 0, then re-join: the commit is the
	// resume point (CONS-2's next-to-read semantics at the wire).
	if typ, _ = roundtrip(t, conn, wire.TypeCommitOffsets, commitBody("workers", join.MemberID, join.Generation, map[uint32]uint64{0: 2})); typ != wire.TypeCommitOffsetsResp {
		t.Fatalf("commit response type %d, want CommitOffsetsResp", typ)
	}
	rejoin := mustJoinGroup(t, conn, "workers", "orders")
	if rejoin.MemberID != join.MemberID {
		t.Fatalf("re-join changed memberID %s → %s", join.MemberID, rejoin.MemberID)
	}
	if got := nextByPartition(rejoin)[0]; got != 2 {
		t.Fatalf("resume offset after commit = %d, want 2", got)
	}

	if hb := mustHeartbeat(t, conn, "workers", join.MemberID, rejoin.Generation); hb.Flags&wire.HeartbeatRejoin != 0 {
		t.Fatalf("REJOIN bit set for a current member (flags %d)", hb.Flags)
	}
	if typ, _ = roundtrip(t, conn, wire.TypeLeaveGroup, wire.LeaveGroup{Group: "workers", MemberID: join.MemberID}.Encode()); typ != wire.TypeLeaveGroupResp {
		t.Fatalf("leave response type %d, want LeaveGroupResp", typ)
	}
	expectError(t, conn, wire.TypeHeartbeat,
		wire.Heartbeat{Group: "workers", MemberID: join.MemberID, Generation: rejoin.Generation}.Encode(),
		wire.CodeUnknownMember)
}

// TestStaleGenerationFetchAndCommitRejected is GRP-5 at the wire: codes 12
// live for fetch AND commit, with zero state change proven by re-reading
// positions, and the broker serving normal traffic after every rejection
// (PROT-3).
func TestStaleGenerationFetchAndCommitRejected(t *testing.T) {
	s := startBroker(t, t.TempDir())
	connA := dialBroker(t, s)
	mustCreateTopic(t, connA, "orders", 4)
	joinA := mustJoinGroup(t, connA, "workers", "orders") // generation 1
	connB := dialBroker(t, s)
	mustJoinGroup(t, connB, "workers", "orders") // generation 2: A is stale now

	// Keep A live through the assertions (heartbeats are fence-exempt).
	mustHeartbeat(t, connA, "workers", joinA.MemberID, joinA.Generation)

	one := []wire.FetchEntry{{Partition: 0, Offset: 0}}
	expectError(t, connA, wire.TypeGroupFetch,
		groupFetchBody("workers", joinA.MemberID, joinA.Generation, one, 1), wire.CodeStaleGeneration)
	expectError(t, connA, wire.TypeCommitOffsets,
		commitBody("workers", joinA.MemberID, joinA.Generation, map[uint32]uint64{0: 9}), wire.CodeStaleGeneration)

	// Zero state change: A's re-join shows partition 0 still at 0.
	rejoinA := mustJoinGroup(t, connA, "workers", "orders")
	if got := nextByPartition(rejoinA)[0]; got != 0 {
		t.Fatalf("fenced commit changed the position: partition 0 = %d, want 0", got)
	}
	// The broker serves normal traffic after the rejections.
	mustProduce(t, connA, "orders", 0, "still-alive")
}

// TestUnknownMemberAlwaysGets13 pins D-SL2-6's precedence at the wire: an
// unknown or dead member sees 13 on fetch, commit, and heartbeat — even
// with a stale generation, where 12 would also match (13 before 12).
func TestUnknownMemberAlwaysGets13(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "orders", 2)
	join := mustJoinGroup(t, conn, "workers", "orders") // generation 1
	conn2 := dialBroker(t, s)
	mustJoinGroup(t, conn2, "workers", "orders") // generation 2

	one := []wire.FetchEntry{{Partition: 0, Offset: 0}}
	// Never-seen memberID, current generation.
	expectError(t, conn, wire.TypeGroupFetch, groupFetchBody("workers", "m999", 2, one, 1), wire.CodeUnknownMember)
	expectError(t, conn, wire.TypeHeartbeat,
		wire.Heartbeat{Group: "workers", MemberID: "m999", Generation: 2}.Encode(), wire.CodeUnknownMember)
	// Unknown group.
	expectError(t, conn, wire.TypeCommitOffsets,
		commitBody("ghosts", "m1", 1, map[uint32]uint64{0: 1}), wire.CodeUnknownMember)

	// A DEAD member with a STALE generation: 13 wins over 12.
	if typ, _ := roundtrip(t, conn, wire.TypeLeaveGroup, wire.LeaveGroup{Group: "workers", MemberID: join.MemberID}.Encode()); typ != wire.TypeLeaveGroupResp {
		t.Fatal("leave failed")
	}
	expectError(t, conn, wire.TypeCommitOffsets,
		commitBody("workers", join.MemberID, join.Generation, map[uint32]uint64{0: 1}), wire.CodeUnknownMember)
	expectError(t, conn, wire.TypeGroupFetch,
		groupFetchBody("workers", join.MemberID, join.Generation, one, 1), wire.CodeUnknownMember)
}

// TestHeartbeatExemptFromGenerationFence is F1's wire-level proof: a live
// member heartbeating with a stale generation gets the level-triggered
// REJOIN bit — never 12 — repeatedly, until it re-joins.
func TestHeartbeatExemptFromGenerationFence(t *testing.T) {
	s := startBroker(t, t.TempDir())
	connA := dialBroker(t, s)
	mustCreateTopic(t, connA, "orders", 2)
	joinA := mustJoinGroup(t, connA, "workers", "orders")
	connB := dialBroker(t, s)
	mustJoinGroup(t, connB, "workers", "orders") // A is behind now

	for i := 0; i < 3; i++ {
		hb := mustHeartbeat(t, connA, "workers", joinA.MemberID, joinA.Generation)
		if hb.Flags&wire.HeartbeatRejoin == 0 {
			t.Fatalf("heartbeat %d: REJOIN bit clear for a behind member (or 12 served)", i)
		}
	}
	rejoinA := mustJoinGroup(t, connA, "workers", "orders")
	if hb := mustHeartbeat(t, connA, "workers", joinA.MemberID, rejoinA.Generation); hb.Flags&wire.HeartbeatRejoin != 0 {
		t.Fatal("REJOIN bit still set after re-join")
	}
}

// TestParkedGroupFetchRefencedOnWake: a GroupFetch parked across a
// rebalance re-validates on wake and serves 12 — the one honest route to
// STALE_GENERATION for a live member (D-SL2-6), with no generation-bump
// wake (DD-11).
func TestParkedGroupFetchRefencedOnWake(t *testing.T) {
	s := startBroker(t, t.TempDir())
	ctl := dialBroker(t, s)
	mustCreateTopic(t, ctl, "orders", 2)
	join := mustJoinGroup(t, ctl, "workers", "orders")

	// Park the member's GroupFetch at both tails on its fetch conn.
	fetchConn := dialBroker(t, s)
	entries := []wire.FetchEntry{{Partition: 0, Offset: 0}, {Partition: 1, Offset: 0}}
	if err := wire.WriteFrame(fetchConn, wire.TypeGroupFetch,
		groupFetchBody("workers", join.MemberID, join.Generation, entries, 10_000)); err != nil {
		t.Fatal(err)
	}
	p0, err := s.store.Partition("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool { return p0.ParkedWaiters() == 1 }, "group fetch to park")

	// A second member joins: generation bumps, the parked fetch does NOT
	// wake (serve-time fencing, not bump-wakes). Keep member 1 live.
	ctl2 := dialBroker(t, s)
	mustJoinGroup(t, ctl2, "workers", "orders")
	mustHeartbeat(t, ctl, "workers", join.MemberID, join.Generation)

	// Frontier advance wakes the park → re-fence → 12 on the fetch conn.
	mustProduce(t, ctl, "orders", 0, "wake")
	fetchConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	typ, body, rerr := wire.ReadFrame(fetchConn, wire.MaxResponseFrame)
	if rerr != nil {
		t.Fatalf("reading woken group fetch: %v", rerr)
	}
	if typ != wire.TypeError {
		t.Fatalf("woken group fetch response type %d, want Error{STALE_GENERATION}", typ)
	}
	em, werr := wire.DecodeErrorMsg(body)
	if werr != nil || em.Code != uint16(wire.CodeStaleGeneration) {
		t.Fatalf("woken group fetch error = %+v (%v), want code 12", em, werr)
	}
	// The fetch conn survives the rejection (PROT-3).
	fetchConn.SetReadDeadline(time.Time{})
	if typ, _ := roundtrip(t, fetchConn, wire.TypeListTopics, nil); typ != wire.TypeListTopicsResp {
		t.Fatal("fetch conn dead after served 12")
	}
}

// TestGroupJoinValidation: unknown topic → UNKNOWN_TOPIC, invalid group
// name → INVALID_NAME, cross-topic join → MALFORMED naming the binding
// (D-SL2-6, D15).
func TestGroupJoinValidation(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "orders", 2)

	expectError(t, conn, wire.TypeJoinGroup,
		wire.JoinGroup{Group: "workers", Topic: "ghost"}.Encode(), wire.CodeUnknownTopic)
	expectError(t, conn, wire.TypeJoinGroup,
		wire.JoinGroup{Group: "UPPER", Topic: "orders"}.Encode(), wire.CodeInvalidName)

	mustJoinGroup(t, conn, "workers", "orders")
	mustCreateTopic(t, conn, "other", 1)
	conn2 := dialBroker(t, s)
	typ, body := roundtrip(t, conn2, wire.TypeJoinGroup, wire.JoinGroup{Group: "workers", Topic: "other"}.Encode())
	em, werr := wire.DecodeErrorMsg(body)
	if typ != wire.TypeError || werr != nil || em.Code != uint16(wire.CodeMalformed) {
		t.Fatalf("cross-topic join = type %d %+v, want MALFORMED", typ, em)
	}
	if !strings.Contains(em.Msg, "bound to topic orders") {
		t.Fatalf("MALFORMED msg %q does not name the binding", em.Msg)
	}
}

// TestGroupFetchCaps: the fetch caps hold for GroupFetch too — 0 entries
// MALFORMED, >16 FETCH_TOO_WIDE, oversized maxWait CAP_EXCEEDED.
func TestGroupFetchCaps(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "orders", 2)
	join := mustJoinGroup(t, conn, "workers", "orders")

	expectError(t, conn, wire.TypeGroupFetch,
		groupFetchBody("workers", join.MemberID, join.Generation, nil, 1), wire.CodeMalformed)
	var many []wire.FetchEntry
	for i := 0; i < MaxFetchEntries+1; i++ {
		many = append(many, wire.FetchEntry{Partition: 0, Offset: 0})
	}
	expectError(t, conn, wire.TypeGroupFetch,
		groupFetchBody("workers", join.MemberID, join.Generation, many, 1), wire.CodeFetchTooWide)
	one := []wire.FetchEntry{{Partition: 0, Offset: 0}}
	expectError(t, conn, wire.TypeGroupFetch,
		groupFetchBody("workers", join.MemberID, join.Generation, one, MaxFetchWaitMs+1), wire.CodeCapExceeded)
}

// TestConnCloseTriggersImmediateRebalance proves the D-SL2-11 teardown
// glue over real TCP: dropping a member's control conn is immediate death —
// the survivor sees REJOIN and re-joins into full ownership, no session
// timeout involved.
func TestConnCloseTriggersImmediateRebalance(t *testing.T) {
	s := startBroker(t, t.TempDir())
	ctlA := dialBroker(t, s)
	mustCreateTopic(t, ctlA, "orders", 4)
	joinA := mustJoinGroup(t, ctlA, "workers", "orders")
	ctlB := dialBroker(t, s)
	joinB := mustJoinGroup(t, ctlB, "workers", "orders")
	if len(joinB.Assigned) != 2 {
		t.Fatalf("member B owns %d partitions, want 2 of 4", len(joinB.Assigned))
	}

	ctlB.Close() // abrupt drop, no LeaveGroup

	waitForCond(t, func() bool {
		hb := mustHeartbeat(t, ctlA, "workers", joinA.MemberID, joinA.Generation)
		return hb.Flags&wire.HeartbeatRejoin != 0
	}, "survivor to see REJOIN after the conn drop")
	rejoinA := mustJoinGroup(t, ctlA, "workers", "orders")
	if len(rejoinA.Assigned) != 4 {
		t.Fatalf("survivor owns %d partitions after the drop, want all 4", len(rejoinA.Assigned))
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
