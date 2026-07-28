// LOG-5/DD-8 degrade suite (D-SL1-3): scripted append/short-write/sync/
// frontier faults → sticky ErrWriteRejected, reads keep serving, truncate-
// back to the frontier exactly when the on-disk frontier is provably
// current — and NEVER on a frontier-write failure. External test package:
// storagetest imports storage, so an in-package import would cycle.
package storage_test

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/storage/storagetest"
)

// baseline payloads acked before every scripted fault.
var degradeBaseline = []string{"base-a", "base-bb"}

// openDegradeStore opens dir over the given FS/Syncer with one
// single-partition topic and the baseline payloads acked, returning the
// partition and its durable frontier.
func openDegradeStore(t *testing.T, dir string, fsys storage.FS, syncer storage.Syncer) (*storage.Partition, int64) {
	t.Helper()
	s, err := storage.Open(dir, fsys, syncer)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// A failed partition still owns a live flusher and open file: Close is
	// mandatory or -race runs leak goroutines into later tests.
	t.Cleanup(func() { s.Close() })
	if err := s.CreateTopic("t", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	p, err := s.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range degradeBaseline {
		if _, err := p.Append([]byte(m)); err != nil {
			t.Fatalf("baseline append: %v", err)
		}
	}
	return p, p.Frontier()
}

// assertDegraded asserts the LOG-5 post-fault shape on p: the sticky reject
// on a fresh Append, reads still serving every baseline record, and the log
// file at exactly wantLogSize bytes.
func assertDegraded(t *testing.T, p *storage.Partition, dir string, wantLogSize int64) {
	t.Helper()
	if _, err := p.Append([]byte("after-failure")); !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("Append after fault = %v, want ErrWriteRejected (sticky)", err)
	}
	recs, err := p.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) != len(degradeBaseline) {
		t.Fatalf("Fetch on degraded partition = %d recs, %v; want the %d baseline records", len(recs), err, len(degradeBaseline))
	}
	for i, want := range degradeBaseline {
		if string(recs[i].Payload) != want || recs[i].Offset != uint64(i) {
			t.Errorf("rec %d = %q@%d, want %q@%d", i, recs[i].Payload, recs[i].Offset, want, i)
		}
	}
	logPath := filepath.Join(dir, "t", "0", "log")
	if info, err := os.Stat(logPath); err != nil || info.Size() != wantLogSize {
		t.Fatalf("log size after degrade = %v, %v; want %d", info.Size(), err, wantLogSize)
	}
}

// reopenHealthy proves the restart-heals half of LOG-5: boot dir with the
// REAL FS, baseline records served at their offsets, new appends accepted,
// zero manual repair.
func reopenHealthy(t *testing.T, dir string) {
	t.Helper()
	s, err := storage.Open(dir, storage.OSFS(), storage.FileSyncer{})
	if err != nil {
		t.Fatalf("restart on same dir: %v", err)
	}
	defer s.Close()
	p, err := s.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := p.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) < len(degradeBaseline) {
		t.Fatalf("Fetch after restart = %d recs, %v; want at least the %d baseline records", len(recs), err, len(degradeBaseline))
	}
	for i, want := range degradeBaseline {
		if string(recs[i].Payload) != want || recs[i].Offset != uint64(i) {
			t.Errorf("rec %d after restart = %q@%d, want %q@%d", i, recs[i].Payload, recs[i].Offset, want, i)
		}
	}
	if _, err := p.Append([]byte("post-restart")); err != nil {
		t.Fatalf("Append after restart = %v, want accepted", err)
	}
}

func TestDegradeShortWriteTruncatesBackToFrontier(t *testing.T) {
	dir := t.TempDir()
	ffs := storagetest.WrapFS(storage.OSFS())
	p, frontier := openDegradeStore(t, dir, ffs, storage.FileSyncer{})

	// A short write leaves 3 torn bytes on disk past the frontier; DD-8's
	// truncate-back must cut them (the on-disk frontier is provably current).
	ffs.FailWrite("log", 1, 3, syscall.ENOSPC)
	if _, err := p.Append([]byte("doomed")); !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("Append under short write = %v, want ErrWriteRejected", err)
	}
	assertDegraded(t, p, dir, frontier)
	reopenHealthy(t, dir)
}

func TestDegradeWriteErrorRejectsSticky(t *testing.T) {
	dir := t.TempDir()
	ffs := storagetest.WrapFS(storage.OSFS())
	p, frontier := openDegradeStore(t, dir, ffs, storage.FileSyncer{})

	// A plain write failure leaves nothing on disk; truncate-back is a no-op
	// but the sticky reject and serving reads still hold.
	ffs.FailWrite("log", 1, 0, syscall.EIO)
	if _, err := p.Append([]byte("doomed")); !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("Append under write error = %v, want ErrWriteRejected", err)
	}
	assertDegraded(t, p, dir, frontier)
	reopenHealthy(t, dir)
}

func TestDegradeSyncErrorTruncatesBack(t *testing.T) {
	dir := t.TempDir()
	syncer := &storagetest.FaultSyncer{Inner: storage.FileSyncer{}}
	p, frontier := openDegradeStore(t, dir, storage.OSFS(), syncer)

	// The covering fsync fails AFTER the batch's bytes hit the file: the
	// unsynced tail must be truncated back to the (still-current) frontier.
	syncer.FailNth(1, syscall.EIO)
	if _, err := p.Append([]byte("doomed")); !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("Append under sync error = %v, want ErrWriteRejected", err)
	}
	assertDegraded(t, p, dir, frontier)
	reopenHealthy(t, dir)
}

func TestDegradeFrontierWriteFailureDoesNotTruncate(t *testing.T) {
	dir := t.TempDir()
	ffs := storagetest.WrapFS(storage.OSFS())
	p, frontier := openDegradeStore(t, dir, ffs, storage.FileSyncer{})

	// D-SL1-3's scoping: the frontier atomicWrite may have failed AFTER its
	// rename installed the NEW value, so truncating to the old one could
	// leave frontier > log length — a boot refusal. Mark failed only; the
	// fully-written, fsynced batch stays (crash-walk row 2/3 territory).
	ffs.FailWriteFileAtomic("frontier", 1, syscall.ENOSPC)
	if _, err := p.Append([]byte("doomed")); !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("Append under frontier-write failure = %v, want ErrWriteRejected", err)
	}
	doomedRecSize := int64(8 + len("doomed"))
	assertDegraded(t, p, dir, frontier+doomedRecSize)
	reopenHealthy(t, dir)
}

func TestDegradeRepairTruncateFailureIsSafe(t *testing.T) {
	dir := t.TempDir()
	ffs := storagetest.WrapFS(storage.OSFS())
	p, frontier := openDegradeStore(t, dir, ffs, storage.FileSyncer{})

	// The repair's Truncate fails too: accepted — reads are frontier-capped
	// so the torn tail is never servable, and restart's scan re-truncates.
	ffs.FailWrite("log", 1, 3, syscall.ENOSPC)
	ffs.FailTruncate("log", 1, syscall.EIO)
	if _, err := p.Append([]byte("doomed")); !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("Append = %v, want ErrWriteRejected", err)
	}
	assertDegraded(t, p, dir, frontier+3)
	reopenHealthy(t, dir)
}

func TestDegradeRepairSyncFailureIsSafe(t *testing.T) {
	dir := t.TempDir()
	ffs := storagetest.WrapFS(storage.OSFS())
	p, frontier := openDegradeStore(t, dir, ffs, storage.FileSyncer{})

	// The truncate lands but the fresh-handle sync making it durable fails:
	// also accepted, same safety argument.
	ffs.FailWrite("log", 1, 3, syscall.ENOSPC)
	ffs.FailSync("log", 1, syscall.EIO)
	if _, err := p.Append([]byte("doomed")); !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("Append = %v, want ErrWriteRejected", err)
	}
	assertDegraded(t, p, dir, frontier)
	reopenHealthy(t, dir)
}
