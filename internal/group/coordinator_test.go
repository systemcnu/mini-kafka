// Coordinator unit suite on the fake clock (D-SL2-3/3b/6 · GRP-1 · GRP-2):
// exact range assignment, the measured detection→new-generation bound,
// re-Join-bumps-nothing, the level-triggered REJOIN bit, the F1
// false-sweep guard, caps, and 12/13 precedence. No real sleeps anywhere.
package group

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/internal/storage"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newTestCoord builds a coordinator on a fake clock. The sweeper goroutine
// is never started: tests call sweepOnce themselves after advancing.
func newTestCoord(t *testing.T) (*Coordinator, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	c, err := New(Config{Clock: clk}, t.TempDir(), storage.OSFS())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, clk
}

func mustJoin(t *testing.T, c *Coordinator, connID uint64, group, topic string, partitions uint32) JoinResult {
	t.Helper()
	res, err := c.Join(connID, group, topic, partitions)
	if err != nil {
		t.Fatalf("Join(conn %d, %s): %v", connID, group, err)
	}
	return res
}

// partitionsOf flattens a join result's owned partitions.
func partitionsOf(res JoinResult) []uint32 {
	out := make([]uint32, 0, len(res.Assigned))
	for _, a := range res.Assigned {
		out = append(out, a.Partition)
	}
	return out
}

// TestRangeAssignmentIsExactPartition is GRP-1's check: 2 members × 4
// partitions → the assignment table is an exact partition of {0..3}, tagged
// with its generation.
func TestRangeAssignmentIsExactPartition(t *testing.T) {
	c, _ := newTestCoord(t)
	mustJoin(t, c, 1, "g", "t", 4)
	r2 := mustJoin(t, c, 2, "g", "t", 4)
	r1 := mustJoin(t, c, 1, "g", "t", 4) // re-Join adopts the current generation

	if r1.Generation != 2 || r2.Generation != 2 {
		t.Fatalf("generations = %d, %d; want both 2 (one bump per new member)", r1.Generation, r2.Generation)
	}
	owned := make(map[uint32]string)
	for _, res := range []JoinResult{r1, r2} {
		if len(res.Assigned) != 2 {
			t.Fatalf("%s owns %d partitions, want 2 of 4", res.MemberID, len(res.Assigned))
		}
		for _, p := range partitionsOf(res) {
			if prev, dup := owned[p]; dup {
				t.Fatalf("partition %d assigned to both %s and %s", p, prev, res.MemberID)
			}
			owned[p] = res.MemberID
		}
	}
	for p := uint32(0); p < 4; p++ {
		if _, ok := owned[p]; !ok {
			t.Fatalf("partition %d unowned; table = %v", p, owned)
		}
	}
}

// TestGRP2ReassignmentBoundOnFakeClock measures marked-dead → new-generation
// against the 1 s bound, driving the sweep cadence on the fake clock. The
// measurement runs from the moment the dead member's session deadline
// expired, which includes detection lag — the stronger claim.
func TestGRP2ReassignmentBoundOnFakeClock(t *testing.T) {
	c, clk := newTestCoord(t)
	mustJoin(t, c, 1, "g", "t", 4)
	mustJoin(t, c, 2, "g", "t", 4)
	mustJoin(t, c, 1, "g", "t", 4) // m1 adopts generation 2

	// Both heartbeat once; then m2 goes silent. Its deadline runs from here.
	if _, err := c.Heartbeat("g", "m1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Heartbeat("g", "m2"); err != nil {
		t.Fatal(err)
	}
	deadline := clk.Now().Add(c.cfg.SessionTimeout)

	var bumpedAt time.Time
	for i := 0; i < 100; i++ {
		clk.Advance(c.cfg.SweepInterval)
		// m1 keeps heartbeating through the whole window at its own cadence
		// (every 5th sweep tick = 500 ms) — it must never be swept.
		if i%5 == 0 {
			if _, err := c.Heartbeat("g", "m1"); err != nil {
				t.Fatalf("live member swept during the wait: %v", err)
			}
		}
		c.sweepOnce()
		rejoin, err := c.Heartbeat("g", "m1")
		if err != nil {
			t.Fatalf("live member swept after sweep: %v", err)
		}
		if rejoin {
			bumpedAt = clk.Now()
			break
		}
	}
	if bumpedAt.IsZero() {
		t.Fatal("dead member was never swept; no new generation produced")
	}
	measured := bumpedAt.Sub(deadline)
	t.Logf("GRP-2 measured deadline-expiry → new-generation: %v (bound 1s)", measured)
	if measured > time.Second {
		t.Fatalf("detection→assignment took %v, bound is 1s (GRP-2)", measured)
	}

	// The survivor's re-Join adopts all four partitions.
	r1 := mustJoin(t, c, 1, "g", "t", 4)
	if len(r1.Assigned) != 4 {
		t.Fatalf("survivor owns %d partitions after the sweep, want 4", len(r1.Assigned))
	}
	// The swept member is fenced from here on (D-SL2-6: always 13).
	if _, err := c.Heartbeat("g", "m2"); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("swept member heartbeat err = %v, want ErrUnknownMember", err)
	}
}

// TestMembershipEventsBumpGenerationOnce pins D-SL2-3: NEW-member join,
// leave, and death each bump exactly once. Generation is observed via
// re-Join, which itself must not bump.
func TestMembershipEventsBumpGenerationOnce(t *testing.T) {
	c, clk := newTestCoord(t)
	r := mustJoin(t, c, 1, "g", "t", 4)
	if r.Generation != 1 {
		t.Fatalf("first join generation = %d, want 1", r.Generation)
	}
	r2 := mustJoin(t, c, 2, "g", "t", 4)
	if r2.Generation != 2 {
		t.Fatalf("second member join generation = %d, want 2", r2.Generation)
	}
	if err := c.Leave("g", r2.MemberID); err != nil {
		t.Fatal(err)
	}
	if r = mustJoin(t, c, 1, "g", "t", 4); r.Generation != 3 {
		t.Fatalf("generation after leave = %d, want 3", r.Generation)
	}
	r3 := mustJoin(t, c, 3, "g", "t", 4)
	if r3.Generation != 4 {
		t.Fatalf("generation after third join = %d, want 4", r3.Generation)
	}
	// Death by silence: m1 heartbeats, m3 does not.
	for i := 0; i < 25; i++ {
		clk.Advance(100 * time.Millisecond)
		if _, err := c.Heartbeat("g", r.MemberID); err != nil {
			t.Fatal(err)
		}
		c.sweepOnce()
	}
	if r = mustJoin(t, c, 1, "g", "t", 4); r.Generation != 5 {
		t.Fatalf("generation after death = %d, want 5 (exactly one bump)", r.Generation)
	}
}

// TestReJoinBumpsNothingAndKeepsMemberID is D-SL2-3b's livelock guard: a
// join on a conn bound to a live member returns the SAME memberID and the
// CURRENT generation, no bump, no membership event.
func TestReJoinBumpsNothingAndKeepsMemberID(t *testing.T) {
	c, _ := newTestCoord(t)
	first := mustJoin(t, c, 1, "g", "t", 4)
	for i := 0; i < 3; i++ {
		again := mustJoin(t, c, 1, "g", "t", 4)
		if again.MemberID != first.MemberID {
			t.Fatalf("re-Join %d returned memberID %s, want %s", i, again.MemberID, first.MemberID)
		}
		if again.Generation != first.Generation {
			t.Fatalf("re-Join %d bumped generation to %d (was %d) — rebalance livelock", i, again.Generation, first.Generation)
		}
	}
}

// TestRejoinBitIsLevelTriggered pins D-SL2-3: the REJOIN bit is derived
// from member.generation ≠ group.generation on every heartbeat — visible
// repeatedly until the member re-Joins, never cleared by delivery.
func TestRejoinBitIsLevelTriggered(t *testing.T) {
	c, _ := newTestCoord(t)
	r1 := mustJoin(t, c, 1, "g", "t", 4)
	mustJoin(t, c, 2, "g", "t", 4) // bump: m1 is now behind

	for i := 0; i < 3; i++ {
		rejoin, err := c.Heartbeat("g", r1.MemberID)
		if err != nil {
			t.Fatal(err)
		}
		if !rejoin {
			t.Fatalf("heartbeat %d: REJOIN bit clear while behind — the bit was consumed", i)
		}
	}
	mustJoin(t, c, 1, "g", "t", 4) // re-Join catches up
	rejoin, err := c.Heartbeat("g", r1.MemberID)
	if err != nil {
		t.Fatal(err)
	}
	if rejoin {
		t.Fatal("REJOIN bit still set after re-Join")
	}
}

// TestStaleMemberHeartbeatRefreshesLastBeat is the F1 false-sweep guard: a
// live member heartbeating with a stale generation through a long rebalance
// window must never be swept — heartbeats are exempt from the fence.
func TestStaleMemberHeartbeatRefreshesLastBeat(t *testing.T) {
	c, clk := newTestCoord(t)
	r1 := mustJoin(t, c, 1, "g", "t", 4)
	mustJoin(t, c, 2, "g", "t", 4) // m1 behind from here on; it never re-Joins

	// 4 s of fake time — two full session timeouts — heartbeating every
	// 500 ms without ever re-Joining.
	for i := 0; i < 8; i++ {
		clk.Advance(500 * time.Millisecond)
		rejoin, err := c.Heartbeat("g", r1.MemberID)
		if err != nil {
			t.Fatalf("stale-generation member swept at step %d: %v", i, err)
		}
		if !rejoin {
			t.Fatalf("step %d: REJOIN bit clear for a behind member", i)
		}
		c.sweepOnce()
	}
}

func TestGroupAndMemberCaps(t *testing.T) {
	c, _ := newTestCoord(t)
	for i := 0; i < MaxGroups; i++ {
		mustJoin(t, c, uint64(i+1), "g"+string(rune('a'+i/26))+string(rune('a'+i%26)), "t", 1)
	}
	if _, err := c.Join(999, "onetoomany", "t", 1); !errors.Is(err, ErrTooManyGroups) {
		t.Fatalf("65th group err = %v, want ErrTooManyGroups", err)
	}

	c2, _ := newTestCoord(t)
	for i := 0; i < MaxMembersPerGroup; i++ {
		mustJoin(t, c2, uint64(i+1), "g", "t", 4)
	}
	if _, err := c2.Join(999, "g", "t", 4); !errors.Is(err, ErrTooManyMembers) {
		t.Fatalf("33rd member err = %v, want ErrTooManyMembers", err)
	}
}

func TestJoinTopicMismatchIsMalformed(t *testing.T) {
	c, _ := newTestCoord(t)
	mustJoin(t, c, 1, "g", "alpha", 2)
	_, err := c.Join(2, "g", "beta", 2)
	if !errors.Is(err, ErrTopicMismatch) {
		t.Fatalf("mismatched-topic join err = %v, want ErrTopicMismatch", err)
	}
}

// TestFencePrecedence13Before12 pins D-SL2-6: not-live beats stale — a dead
// member with a stale generation gets ErrUnknownMember, never
// ErrStaleGeneration; 12 is reachable only by a live member.
func TestFencePrecedence13Before12(t *testing.T) {
	c, _ := newTestCoord(t)
	r1 := mustJoin(t, c, 1, "g", "t", 4)
	mustJoin(t, c, 2, "g", "t", 4) // bump: r1's generation 1 is now stale

	// Live member, stale generation → 12.
	if _, err := c.ValidateFetch("g", r1.MemberID, r1.Generation); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("live+stale fetch err = %v, want ErrStaleGeneration", err)
	}
	// Unknown member with a stale generation → 13, not 12.
	if _, err := c.ValidateFetch("g", "m999", r1.Generation); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("unknown+stale fetch err = %v, want ErrUnknownMember", err)
	}
	// Dead member (left) with its old generation → 13, not 12.
	if err := c.Leave("g", r1.MemberID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ValidateFetch("g", r1.MemberID, r1.Generation); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("dead+stale fetch err = %v, want ErrUnknownMember", err)
	}
	// Unknown group → 13.
	if _, err := c.ValidateFetch("ghost", "m1", 1); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("unknown group fetch err = %v, want ErrUnknownMember", err)
	}
	// A current, live member passes and gets the bound topic.
	r2 := mustJoin(t, c, 2, "g", "t", 4)
	topic, err := c.ValidateFetch("g", r2.MemberID, r2.Generation)
	if err != nil || topic != "t" {
		t.Fatalf("valid fetch = (%q, %v), want (t, nil)", topic, err)
	}
}

// TestConnClosedIsImmediateDeath pins DD-10's control-conn-drop rule and
// D-SL2-3b's fresh-join-after-death: the dropped conn's member dies at
// once, and a new join from the same conn gets a NEW memberID.
func TestConnClosedIsImmediateDeath(t *testing.T) {
	c, _ := newTestCoord(t)
	r1 := mustJoin(t, c, 1, "g", "t", 4)
	r2 := mustJoin(t, c, 2, "g", "t", 4)

	c.ConnClosed(2)
	if _, err := c.Heartbeat("g", r2.MemberID); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("dead member heartbeat err = %v, want ErrUnknownMember", err)
	}
	// Survivor sees the bump and re-Joins into full ownership.
	r1b := mustJoin(t, c, 1, "g", "t", 4)
	if r1b.MemberID != r1.MemberID || len(r1b.Assigned) != 4 {
		t.Fatalf("survivor re-Join = %+v, want same member owning 4 partitions", r1b)
	}
	// The same conn joining again is a FRESH member — the old ID stays fenced.
	r2b := mustJoin(t, c, 2, "g", "t", 4)
	if r2b.MemberID == r2.MemberID {
		t.Fatalf("fresh join after death reused memberID %s", r2.MemberID)
	}
}
