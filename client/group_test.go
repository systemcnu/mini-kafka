// GroupConsumer suite against a live in-process broker (D-SL2-8): lazy
// rejoin-and-reissue on REJOIN and on a mid-poll fence, Commit surfacing
// 12/13 to the caller (the pinned public contract), CONS-2's resume-exact
// check (scenario D), GRP-4's second-group-from-zero (scenario F), and the
// control-conn serialization race. Event assertions with generous
// deadlines — tight timing lives in the fake-clock coordinator suite.
package client_test

import (
	"errors"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/client"
)

// pollUntil keeps polling until pred is satisfied by the accumulated
// records or the deadline passes.
func pollUntil(t *testing.T, c *client.GroupConsumer, deadline time.Duration, pred func(got []client.PartRecord) bool) []client.PartRecord {
	t.Helper()
	var got []client.PartRecord
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		recs, err := c.Poll(200 * time.Millisecond)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		got = append(got, recs...)
		if pred(got) {
			return got
		}
	}
	t.Fatalf("deadline: accumulated %d records without satisfying the condition", len(got))
	return nil
}

func hasPayload(got []client.PartRecord, want string) bool {
	for _, r := range got {
		if string(r.Payload) == want {
			return true
		}
	}
	return false
}

func mustProduceN(t *testing.T, p *client.Producer, topic string, partition uint32, payloads ...string) {
	t.Helper()
	for _, m := range payloads {
		if _, err := p.Produce(topic, partition, []byte(m)); err != nil {
			t.Fatalf("Produce(%q): %v", m, err)
		}
	}
}

// TestGroupConsumerRebalancesOnRejoinBit: a second member joins; the first
// member's heartbeat records REJOIN and its next Poll re-joins lazily —
// afterwards each member receives exactly its own partition's records.
func TestGroupConsumerRebalancesOnRejoinBit(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	mustProduceN(t, prod, "t", 0, "r0")
	mustProduceN(t, prod, "t", 1, "r1")

	c1, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	// Solo member: both partitions' records arrive.
	pollUntil(t, c1, 10*time.Second, func(got []client.PartRecord) bool {
		return hasPayload(got, "r0") && hasPayload(got, "r1")
	})

	c2, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	mustProduceN(t, prod, "t", 0, "r2")
	mustProduceN(t, prod, "t", 1, "r3")

	// After the split (sorted memberIDs × ranges): c1 owns partition 0,
	// c2 owns partition 1. Each must see its new record.
	got1 := pollUntil(t, c1, 10*time.Second, func(got []client.PartRecord) bool {
		return hasPayload(got, "r2")
	})
	for _, r := range got1 {
		if string(r.Payload) == "r2" && r.Partition != 0 {
			t.Fatalf("r2 arrived from partition %d, want 0", r.Partition)
		}
	}
	pollUntil(t, c2, 10*time.Second, func(got []client.PartRecord) bool {
		return hasPayload(got, "r3")
	})
}

// TestPollRejoinsWhenFencedMidPoll kills the assignment under a parked
// poll: a rebalance while the GroupFetch is in flight fences it (12), and
// Poll re-joins and reissues internally — the caller just gets records.
func TestPollRejoinsWhenFencedMidPoll(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	c1, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	type pollResult struct {
		recs []client.PartRecord
		err  error
	}
	resCh := make(chan pollResult, 1)
	go func() {
		recs, perr := c1.Poll(5 * time.Second)
		resCh <- pollResult{recs, perr}
	}()
	// Give the poll a moment to park server-side, then bump the generation
	// under it and wake it with a record.
	time.Sleep(200 * time.Millisecond)
	c2, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	mustProduceN(t, prod, "t", 0, "wake")

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("fenced poll surfaced %v — it must re-join and reissue internally", res.err)
		}
		// c1 (first joiner, lowest memberID) owns partition 0 after the
		// split, so the reissued fetch delivers the record.
		if !hasPayload(res.recs, "wake") {
			// The reissue may legally return empty if it raced the produce;
			// keep polling briefly.
			pollUntil(t, c1, 10*time.Second, func(got []client.PartRecord) bool {
				return hasPayload(got, "wake")
			})
		}
	case <-time.After(15 * time.Second):
		t.Fatal("poll never returned across the rebalance")
	}
}

// TestCommitSurfacesFencingToCaller pins D-SL2-8's public contract: a
// fenced Commit returns its 12/13 to the caller — no auto-heal before
// surfacing — and the NEXT Poll re-joins, after which Commit works again.
func TestCommitSurfacesFencingToCaller(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	mustProduceN(t, prod, "t", 0, "a")

	c1, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	pollUntil(t, c1, 10*time.Second, func(got []client.PartRecord) bool { return len(got) >= 1 })

	// A second join bumps the generation; c1 is stale from here.
	c2, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	cerr := c1.Commit()
	var typed *client.Error
	if !errors.As(cerr, &typed) {
		t.Fatalf("fenced commit returned %v, want a *client.Error carrying 12/13", cerr)
	}
	if typed.Code != client.CodeStaleGeneration && typed.Code != client.CodeUnknownMember {
		t.Fatalf("fenced commit code = %d, want 12 or 13", typed.Code)
	}

	// The next Poll heals; a later Commit is acked.
	if _, err := c1.Poll(200 * time.Millisecond); err != nil {
		t.Fatalf("healing poll: %v", err)
	}
	if err := c1.Commit(); err != nil {
		t.Fatalf("commit after heal: %v", err)
	}
}

// TestConsumeCommitRestartResumesExact is CONS-2's check (scenario D):
// consume the first half, commit, restart the consumer — the first record
// received is exactly the committed next-to-read offset.
func TestConsumeCommitRestartResumesExact(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	mustProduceN(t, prod, "t", 0, "h0", "h1", "h2", "h3", "h4")

	c1, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	pollUntil(t, c1, 10*time.Second, func(got []client.PartRecord) bool { return len(got) == 5 })
	if err := c1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	c1.Close()

	mustProduceN(t, prod, "t", 0, "h5", "h6", "h7", "h8", "h9")

	c2, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	got := pollUntil(t, c2, 10*time.Second, func(got []client.PartRecord) bool { return len(got) >= 1 })
	if got[0].Offset != 5 || string(got[0].Payload) != "h5" {
		t.Fatalf("restart resumed at %q@%d, want exactly h5@5 (the committed next-to-read)", got[0].Payload, got[0].Offset)
	}
}

// TestSecondGroupGetsFullStreamFromZero is GRP-4 (scenario F): a second
// group joining after the first finished independently receives the whole
// stream from offset 0 (D14 earliest).
func TestSecondGroupGetsFullStreamFromZero(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	mustProduceN(t, prod, "t", 0, "f0", "f1", "f2", "f3", "f4", "f5")

	one, err := client.JoinGroup(addr, "one", "t")
	if err != nil {
		t.Fatal(err)
	}
	pollUntil(t, one, 10*time.Second, func(got []client.PartRecord) bool { return len(got) == 6 })
	if err := one.Commit(); err != nil {
		t.Fatal(err)
	}
	one.Close()

	two, err := client.JoinGroup(addr, "two", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()
	got := pollUntil(t, two, 10*time.Second, func(got []client.PartRecord) bool { return len(got) == 6 })
	for i, r := range got {
		if r.Offset != uint64(i) {
			t.Fatalf("second group record %d has offset %d, want %d (full stream from 0)", i, r.Offset, i)
		}
	}
}

// waitAssignment polls c until its Assignment() snapshot equals want (the
// rebalance is adopted lazily, so polling is what drives the re-join).
func waitAssignment(t *testing.T, c *client.GroupConsumer, deadline time.Duration, want []uint32) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if equalParts(c.Assignment(), want) {
			return
		}
		if _, err := c.Poll(200 * time.Millisecond); err != nil {
			t.Fatalf("Poll while waiting for assignment %v: %v", want, err)
		}
	}
	t.Fatalf("assignment never settled to %v (last snapshot %v)", want, c.Assignment())
}

func equalParts(got, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestAssignmentSnapshotAcrossRebalance pins D-SL3-2(b): Assignment()
// returns the sorted owned-partition snapshot, and it tracks a rebalance —
// solo owner of all 4, then the settled 2+2 split after a second join.
func TestAssignmentSnapshotAcrossRebalance(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 4); err != nil {
		t.Fatal(err)
	}

	c1, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	if got := c1.Assignment(); !equalParts(got, []uint32{0, 1, 2, 3}) {
		t.Fatalf("solo member Assignment() = %v, want [0 1 2 3]", got)
	}

	c2, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	// The second joiner's join response already carries the settled upper
	// range (sorted memberIDs × contiguous ranges, DD-11).
	if got := c2.Assignment(); !equalParts(got, []uint32{2, 3}) {
		t.Fatalf("second member Assignment() = %v, want [2 3]", got)
	}
	// c1 adopts its half lazily via Poll.
	waitAssignment(t, c1, 10*time.Second, []uint32{0, 1})
}

// TestAbandonKillsMemberAndSurvivorTakesOver is D-SL3-2(a)'s unit
// (F2-corrected): after Abandon, Commit returns a NON-NIL error (the conn
// is closed client-side — a literal 13 cannot reach a closed socket), the
// survivor ends up owning all 4 partitions (the death proof), and
// Abandon/Close are mutually idempotent via the shared closeOnce. Abandon
// returning at all proves the heartbeat goroutine was joined (it waits on
// hbDone). Run under -race.
func TestAbandonKillsMemberAndSurvivorTakesOver(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 4); err != nil {
		t.Fatal(err)
	}

	c1, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	waitAssignment(t, c1, 10*time.Second, []uint32{0, 1})
	waitAssignment(t, c2, 10*time.Second, []uint32{2, 3})

	c2.Abandon()
	if err := c2.Commit(); err == nil {
		t.Fatal("Commit after Abandon returned nil, want a non-nil error (conn closed client-side)")
	}
	// Control-conn drop is the death event (DD-10): the survivor's next
	// polls re-join into sole ownership of all 4.
	waitAssignment(t, c1, 10*time.Second, []uint32{0, 1, 2, 3})

	// Mutual idempotence: Close after Abandon is a shared-closeOnce no-op.
	if err := c2.Close(); err != nil {
		t.Fatalf("Close after Abandon: %v", err)
	}
	c2.Abandon() // and Abandon again is a no-op too
}

// TestControlConnSerializationUnderRace hammers Commit and Poll while the
// heartbeat goroutine ticks — the control-conn mutex must serialize WHOLE
// roundtrips (write+read), or the one-in-flight protocol corrupts. Run
// under -race.
func TestControlConnSerializationUnderRace(t *testing.T) {
	addr := startBroker(t)
	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for i := 0; i < 10; i++ {
		mustProduceN(t, prod, "t", uint32(i%2), "x")
	}

	c1, err := client.JoinGroup(addr, "grp", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	done := make(chan error, 2)
	go func() {
		for i := 0; i < 30; i++ {
			if err := c1.Commit(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	go func() {
		for i := 0; i < 10; i++ {
			if _, err := c1.Poll(50 * time.Millisecond); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent commit/poll: %v", err)
		}
	}
}
