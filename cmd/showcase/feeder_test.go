package main

import (
	"testing"
	"time"
)

// waitFor polls cond until it holds or the window expires; the failure
// message is the caller's named claim.
func waitFor(t *testing.T, window time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal(msg)
}

// acceleratedConfig is the §F seam set for integration tests: fast tick,
// fast walk, a cap that is never hit.
func acceleratedConfig(t *testing.T) feederConfig {
	t.Helper()
	return feederConfig{
		tmpRoot:   t.TempDir(),
		tick:      10 * time.Millisecond,
		walkEvery: 50 * time.Millisecond,
		capBytes:  1 << 30,
	}
}

// TestBrokerConfig pins D-SL7-1's loopback literal on the SAME value
// production uses: no flag or env can rebind the broker (SHOW-3).
func TestBrokerConfig(t *testing.T) {
	cfg := brokerConfig("/data/dir")
	if cfg.Addr != "127.0.0.1:0" {
		t.Errorf(`brokerConfig("/data/dir").Addr = %q, want the hard-coded loopback literal "127.0.0.1:0" (D-SL7-1, SHOW-3 by construction)`, cfg.Addr)
	}
	if cfg.DataDir != "/data/dir" {
		t.Errorf(`brokerConfig("/data/dir").DataDir = %q, want the caller's dir`, cfg.DataDir)
	}
}

// TestFeederLive is the §F integration proof: the real feeder against a
// live loopback broker — records produced, consumed into the ring,
// frontiers advancing, assignment settled, walker reporting.
func TestFeederLive(t *testing.T) {
	holder := newSnapshotHolder(1<<30, time.Now())
	f := newFeeder(acceleratedConfig(t), holder)
	if err := f.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer f.stop()

	waitFor(t, 15*time.Second, "feeder produced nothing: produced still 0 after the wait window", func() bool {
		return holder.load().Produced > 0
	})
	waitFor(t, 15*time.Second, "no records reached the ring: recent still empty", func() bool {
		return len(holder.load().Recent) > 0
	})
	waitFor(t, 15*time.Second, "assignment never settled to the four partitions", func() bool {
		a := holder.load().Assignment
		return len(a) == 4 && a[0] == 0 && a[1] == 1 && a[2] == 2 && a[3] == 3
	})
	waitFor(t, 15*time.Second, "no consumed-offset frontier ever advanced past 0", func() bool {
		s := holder.load()
		if len(s.Partitions) != feedPartitions {
			return false
		}
		for _, p := range s.Partitions {
			if p.NextOffset > 0 {
				return true
			}
		}
		return false
	})
	waitFor(t, 15*time.Second, "the disk walker never reported bytes for the data dir", func() bool {
		return holder.load().DiskBytes > 0
	})

	s := holder.load()
	if s.Status != statusLive {
		t.Errorf("status = %q, want %q (cap 1 GiB is never hit here)", s.Status, statusLive)
	}
	if s.MemBytes == 0 {
		t.Errorf("memBytes = 0, want ReadMemStats HeapAlloc (§F walker)")
	}
	if len(s.Recent) > ringSize {
		t.Errorf("recent holds %d records, want ≤ the ring of %d", len(s.Recent), ringSize)
	}
	if _, err := time.Parse(time.RFC3339, s.StartedAt); err != nil {
		t.Errorf("startedAt = %q does not parse as RFC3339: %v", s.StartedAt, err)
	}
}

// TestFeederCleanStop pins the §F stop order: stop() returns within a
// bound (a naive join deadlocks behind a parked Poll) after real traffic.
func TestFeederCleanStop(t *testing.T) {
	holder := newSnapshotHolder(1<<30, time.Now())
	f := newFeeder(acceleratedConfig(t), holder)
	if err := f.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer f.stop()

	waitFor(t, 15*time.Second, "feeder produced nothing before the stop — nothing real was shut down", func() bool {
		return holder.load().Produced > 0 && len(holder.load().Recent) > 0
	})

	done := make(chan struct{})
	go func() {
		f.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("stop() did not return within 20 s — §F stop order violated (close stop → gc.Close → producer Close → join → broker Stop)")
	}
}
