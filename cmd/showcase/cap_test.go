package main

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/client"
)

// dirSize sums regular-file sizes under dir — the same observation the
// feeder's walker makes, taken independently by the test.
func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, ierr := d.Info()
			if ierr != nil {
				return ierr
			}
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	return total
}

// feedStatusOK asserts /feed still serves 200 and returns its body —
// reads keep working in every degraded state (D-SL7-4).
func feedStatusOK(t *testing.T, holder *snapshotHolder) string {
	t.Helper()
	rr := httptest.NewRecorder()
	newMux(holder).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/feed", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /feed while degraded: got %d, want 200 (reads keep serving, D-SL7-4)", rr.Code)
	}
	return rr.Body.String()
}

// TestPlateau is SHOW-4's sustained-run proof at a 64 KiB cap with
// accelerated seams: the status flips to paused-at-cap, the dir STOPS
// growing across further walks, produced stops counting, and /feed still
// serves.
func TestPlateau(t *testing.T) {
	const capBytes = 64 << 10
	holder := newSnapshotHolder(capBytes, time.Now())
	f := newFeeder(feederConfig{
		tmpRoot:   t.TempDir(),
		tick:      time.Millisecond,
		walkEvery: 20 * time.Millisecond,
		capBytes:  capBytes,
	}, holder)
	if err := f.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer f.stop()

	waitFor(t, 60*time.Second, "status never flipped to paused-at-cap: dir kept growing past the 64 KiB test cap (no pause logic)", func() bool {
		return holder.load().Status == statusPaused
	})

	// Let any in-flight produce land and the consumer drain to the tail
	// (an at-tail Poll makes no commits, so nothing else writes), then the
	// dir must be byte-stable across many further walks.
	time.Sleep(3 * time.Second)
	size1 := dirSize(t, f.dir)
	produced1 := holder.load().Produced
	time.Sleep(500 * time.Millisecond) // ≥ 25 further walk intervals
	if size2 := dirSize(t, f.dir); size2 != size1 {
		t.Errorf("data dir kept growing after the pause: %d → %d bytes (pause not sticky?)", size1, size2)
	}
	if produced2 := holder.load().Produced; produced2 != produced1 {
		t.Errorf("produced kept counting after the pause: %d → %d (producer must skip ticks while paused)", produced1, produced2)
	}

	s := holder.load()
	if s.Status != statusPaused {
		t.Errorf("status = %q after settling, want %q (sticky until restart, ledger 8)", s.Status, statusPaused)
	}
	if s.DiskBytes < capBytes {
		t.Errorf("diskBytes = %d below the %d cap yet paused-at-cap — the walk that fired the pause must be reflected", s.DiskBytes, int64(capBytes))
	}
	if body := feedStatusOK(t, holder); !strings.Contains(body, statusPaused) {
		t.Errorf("/feed body does not carry %q while paused: %.200s", statusPaused, body)
	}
}

// TestFreshBoot pins restart-fresh (D-SL7-4): every start MkdirTemps a
// NEW data dir; a second boot never adopts the first boot's records.
func TestFreshBoot(t *testing.T) {
	root := t.TempDir()
	cfg := feederConfig{tmpRoot: root, tick: 10 * time.Millisecond, walkEvery: 50 * time.Millisecond, capBytes: 1 << 30}

	holder1 := newSnapshotHolder(1<<30, time.Now())
	f1 := newFeeder(cfg, holder1)
	if err := f1.start(); err != nil {
		t.Fatalf("first start: %v", err)
	}
	defer f1.stop()
	waitFor(t, 15*time.Second, "first boot produced nothing", func() bool {
		return holder1.load().Produced > 0
	})
	dir1 := f1.dir
	f1.stop()
	if dir1 == "" {
		t.Fatal("first start never recorded a data dir")
	}
	if dirSize(t, dir1) == 0 {
		t.Errorf("first boot's dir %s holds zero bytes despite acked produces", dir1)
	}

	holder2 := newSnapshotHolder(1<<30, time.Now())
	f2 := newFeeder(cfg, holder2)
	if err := f2.start(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	defer f2.stop()
	if f2.dir == dir1 {
		t.Fatalf("second start reused the data dir %q — every boot must MkdirTemp a NEW dir (restart-fresh, D-SL7-4)", f2.dir)
	}
	if !strings.HasPrefix(filepath.Base(f2.dir), "minikafka-showcase-") {
		t.Errorf("data dir %q not created under the minikafka-showcase- MkdirTemp pattern", f2.dir)
	}
}

// failingProducer is the §F seam's injected fake: every produce is refused
// with the broker's WRITE_FAILED code at the client boundary.
type failingProducer struct{}

func (failingProducer) Produce(topic string, partition uint32, payload []byte) (uint64, error) {
	return 0, &client.Error{Code: client.CodeWriteFailed, Msg: "injected: append refused"}
}
func (failingProducer) Close() error { return nil }

// TestWriteFailedPause pins the defense-in-depth leg of D-SL7-4: a produce
// answered WRITE_FAILED flips the sticky pause while reads keep serving.
func TestWriteFailedPause(t *testing.T) {
	holder := newSnapshotHolder(1<<30, time.Now())
	cfg := acceleratedConfig(t)
	cfg.tick = 5 * time.Millisecond
	cfg.newProducer = func(addr string) (producerConn, error) { return failingProducer{}, nil }
	f := newFeeder(cfg, holder)
	if err := f.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer f.stop()

	waitFor(t, 15*time.Second, "a WRITE_FAILED produce never flipped status to paused-at-cap (D-SL7-4's defense-in-depth leg)", func() bool {
		return holder.load().Status == statusPaused
	})
	if produced := holder.load().Produced; produced != 0 {
		t.Errorf("produced = %d despite every produce failing — only acked produces count", produced)
	}
	time.Sleep(200 * time.Millisecond)
	if s := holder.load(); s.Status != statusPaused {
		t.Errorf("pause not sticky: status = %q after settling", s.Status)
	}
	if body := feedStatusOK(t, holder); !strings.Contains(body, statusPaused) {
		t.Errorf("/feed body does not carry %q while paused: %.200s", statusPaused, body)
	}
}

// TestCapEnv pins the SHOWCASE_DISK_CAP_MB parse: unset, garbage, or
// non-positive take the 200 MiB default (D-SL7-4).
func TestCapEnv(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int64
	}{
		{"", 200 << 20},
		{"garbage", 200 << 20},
		{"12.5", 200 << 20},
		{"-3", 200 << 20},
		{"0", 200 << 20},
		{"200", 200 << 20},
		{"50", 50 << 20},
		{"1", 1 << 20},
	} {
		if got := capBytesFromEnv(c.in); got != c.want {
			t.Errorf("capBytesFromEnv(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
