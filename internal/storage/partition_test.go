// LOG-1a: the ack-ordering recorder test — no produce ack may precede its
// covering fsync (and frontier advance). Seen red first against a flusher
// that acks early, per the SL0 exit checklist. Plus basic append/read
// behavior of one partition.
package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gateRecorder is the LOG-1a Syncer: it records completed syncs into a
// shared event list and, when armed, blocks Sync until released (blocking
// implementations are explicitly legal per D-SL0-5).
type gateRecorder struct {
	mu      sync.Mutex
	events  []string
	gate    chan struct{}
	entered chan struct{}
}

func newGateRecorder() *gateRecorder {
	return &gateRecorder{entered: make(chan struct{}, 64)}
}

func (g *gateRecorder) Sync(f File) error {
	g.mu.Lock()
	gate := g.gate
	g.mu.Unlock()
	g.entered <- struct{}{}
	if gate != nil {
		<-gate
	}
	if err := f.Sync(); err != nil {
		return err
	}
	g.add("sync")
	return nil
}

func (g *gateRecorder) add(e string) {
	g.mu.Lock()
	g.events = append(g.events, e)
	g.mu.Unlock()
}

func (g *gateRecorder) list() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.events...)
}

// Arm makes the next Sync calls block until Release.
func (g *gateRecorder) Arm() {
	g.mu.Lock()
	g.gate = make(chan struct{})
	g.mu.Unlock()
}

func (g *gateRecorder) Release() {
	g.mu.Lock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
	g.mu.Unlock()
}

func (g *gateRecorder) waitSyncEntered(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the flusher to reach Sync")
	}
}

func openTestPartition(t *testing.T, syncer Syncer) *Partition {
	t.Helper()
	p, err := openPartition(OSFS(), t.TempDir(), syncer)
	if err != nil {
		t.Fatalf("openPartition: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestAckNeverPrecedesCoveringSync(t *testing.T) {
	rec := newGateRecorder()
	p := openTestPartition(t, rec)
	t.Cleanup(rec.Release) // never leave the flusher wedged

	const payload = "payload-bytes"
	recSize := int64(8 + len(payload))

	for round := 0; round < 2; round++ {
		rec.Arm()
		var wg sync.WaitGroup
		var acks atomic.Int32
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				off, err := p.Append([]byte(payload))
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				// The heart of LOG-1a: at ack time the durable frontier
				// must already cover this record's last byte. The frontier
				// only grows, so reading it after Append returns can only
				// over-approximate — a violation seen here is real.
				if got, need := p.Frontier(), (int64(off)+1)*recSize; got < need {
					t.Errorf("ack for offset %d with frontier %d < %d (ack precedes covering sync)", off, got, need)
				}
				rec.add(fmt.Sprintf("ack:%d", off))
				acks.Add(1)
			}()
		}
		// The flusher has written the batch and reached its covering fsync,
		// which is blocked by the gate: no ack may exist yet.
		rec.waitSyncEntered(t)
		time.Sleep(20 * time.Millisecond) // let any (buggy) early acks land
		if n := acks.Load(); n != 0 {
			t.Fatalf("round %d: %d acks recorded while the covering fsync was still pending", round, n)
		}
		rec.Release()
		wg.Wait()
	}

	// Recorded order: a covering sync must precede every ack.
	events := rec.list()
	syncSeen := false
	for i, e := range events {
		if e == "sync" {
			syncSeen = true
		} else if !syncSeen {
			t.Fatalf("event %d = %q before any completed sync (events: %v)", i, e, events)
		}
	}
}

func TestAppendAssignsContiguousOffsetsAndFetchReturnsInOrder(t *testing.T) {
	p := openTestPartition(t, FileSyncer{})

	var wg sync.WaitGroup
	offsets := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			off, err := p.Append([]byte(fmt.Sprintf("m%d", i)))
			if err != nil {
				t.Errorf("Append: %v", err)
				return
			}
			offsets[i] = off
		}()
	}
	wg.Wait()

	seen := make(map[uint64]bool)
	for _, off := range offsets {
		if off > 4 || seen[off] {
			t.Fatalf("offsets not a contiguous 0..4 set: %v", offsets)
		}
		seen[off] = true
	}

	recs, err := p.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(recs) != 5 {
		t.Fatalf("Fetch returned %d records, want 5", len(recs))
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Errorf("record %d has offset %d, want %d (log order)", i, r.Offset, i)
		}
	}
}

func TestFetchAtTailTimesOutEmpty(t *testing.T) {
	p := openTestPartition(t, FileSyncer{})
	start := time.Now()
	recs, err := p.Fetch(0, 1<<20, 30*time.Millisecond, nil, nil)
	if err != nil || recs != nil {
		t.Fatalf("Fetch = %v, %v; want nil, nil (empty-at-timeout)", recs, err)
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("Fetch returned before maxWait with no data")
	}
}
