// Exit-checklist item 3: a parked fetch wakes on FRONTIER ADVANCE, not on
// raw append. Proven with the blocking-Syncer gate, the AdvanceHook seam,
// and the parked-waiter observable (D-SL0-5).
package storage

import (
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestParkedFetchWakesOnFrontierAdvanceNotRawAppend(t *testing.T) {
	rec := newGateRecorder()
	p := openTestPartition(t, rec)
	t.Cleanup(rec.Release)

	var advanced atomic.Int64
	p.SetAdvanceHook(func(frontier int64) { advanced.Store(frontier) })

	type result struct {
		recs []Record
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		recs, err := p.Fetch(0, 1<<20, 5*time.Second, nil, nil)
		resCh <- result{recs, err}
	}()
	waitFor(t, func() bool { return p.ParkedWaiters() == 1 }, "fetch to park")

	// Gate the syncer, then append: the raw file write completes, the
	// covering fsync blocks on the gate.
	rec.Arm()
	go p.Append([]byte("wake-me"))
	rec.waitSyncEntered(t)

	// Raw append done, frontier NOT advanced: no wake, no data, no hook.
	time.Sleep(30 * time.Millisecond)
	if f := advanced.Load(); f != 0 {
		t.Fatalf("AdvanceHook fired (frontier %d) while the fsync was still gated", f)
	}
	select {
	case r := <-resCh:
		t.Fatalf("parked fetch returned (%v, %v) on raw append, before the covering fsync", r.recs, r.err)
	default:
	}
	if n := p.ParkedWaiters(); n != 1 {
		t.Fatalf("parked waiters = %d during gated fsync, want 1", n)
	}

	// Release the gate: fsync completes, frontier advances, fetch wakes.
	rec.Release()
	select {
	case r := <-resCh:
		if r.err != nil || len(r.recs) != 1 || string(r.recs[0].Payload) != "wake-me" || r.recs[0].Offset != 0 {
			t.Fatalf("woken fetch = (%v, %v), want the single record", r.recs, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fetch did not wake on frontier advance")
	}
	if advanced.Load() == 0 {
		t.Fatal("AdvanceHook never fired after the gate was released")
	}
}
