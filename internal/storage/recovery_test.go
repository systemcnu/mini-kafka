// DD-4 boot-scan tests: fresh init, restart serving acked data, truncation
// of an invalid unacked tail, kept valid unacked records, and the
// missing-frontier refusal. Scripted-fault proofs (FS fakes, kill -9) are
// red-listed for SL1.
package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRecoveryFreshPartitionStartsAtZero(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("openPartition: %v", err)
	}
	defer p.Close()
	if got := p.Frontier(); got != 0 {
		t.Fatalf("fresh frontier = %d, want 0", got)
	}
	off, err := p.Append([]byte("first"))
	if err != nil || off != 0 {
		t.Fatalf("first append = %d, %v; want 0, nil", off, err)
	}
	// Fresh init must be idempotent on disk: the frontier file now exists.
	if _, err := os.Stat(filepath.Join(dir, "frontier")); err != nil {
		t.Fatalf("frontier file after fresh init: %v", err)
	}
}

func TestRecoveryRestartServesAckedDataAndContinuesOffsets(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range []string{"a", "bb", "ccc"} {
		if off, err := p.Append([]byte(m)); err != nil || off != uint64(i) {
			t.Fatalf("append %d = %d, %v", i, off, err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()

	recs, err := p2.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) != 3 {
		t.Fatalf("Fetch after restart = %d recs, %v; want 3, nil", len(recs), err)
	}
	for i, want := range []string{"a", "bb", "ccc"} {
		if string(recs[i].Payload) != want || recs[i].Offset != uint64(i) {
			t.Errorf("rec %d = %q@%d, want %q@%d", i, recs[i].Payload, recs[i].Offset, want, i)
		}
	}
	// LOG-2 at the storage level: offsets continue contiguously.
	if off, err := p2.Append([]byte("dddd")); err != nil || off != 3 {
		t.Fatalf("append after restart = %d, %v; want 3, nil", off, err)
	}
}

func TestRecoveryTruncatesInvalidTailPastFrontier(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("keep1"))
	p.Append([]byte("keep2"))
	frontier := p.Frontier()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// A torn tail: 3 bytes that cannot even hold a record header. Never
	// acked (past the frontier), so recovery must silently truncate it.
	logPath := filepath.Join(dir, "log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xDE, 0xAD, 0xBE})
	f.Close()

	p2, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	defer p2.Close()

	if info, err := os.Stat(logPath); err != nil || info.Size() != frontier {
		t.Fatalf("log size after recovery = %v, %v; want truncated to frontier %d", info.Size(), err, frontier)
	}
	recs, err := p2.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) != 2 {
		t.Fatalf("Fetch = %d recs, %v; want the 2 acked records", len(recs), err)
	}
	if off, err := p2.Append([]byte("next")); err != nil || off != 2 {
		t.Fatalf("append after truncation = %d, %v; want 2, nil", off, err)
	}
}

func TestRecoveryKeepsValidUnackedRecordsPastFrontier(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("acked"))
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// A fully valid record past the frontier (crash after fsync(log),
	// before the frontier write): DD-4 keeps it — it was never acked, and a
	// stale-low frontier only widens the safe zone — but DD-5 hides it
	// until a future frontier advance covers it.
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(encodeRecord([]byte("unacked")))
	f.Close()

	p2, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()

	// Not served yet: the frontier does not cover it.
	recs, err := p2.Fetch(0, 1<<20, 30*time.Millisecond, nil, nil)
	if err != nil || len(recs) != 1 || string(recs[0].Payload) != "acked" {
		t.Fatalf("pre-advance Fetch = %v, %v; want only the acked record", recs, err)
	}
	// The kept record occupies offset 1, so the next append gets 2, and the
	// flush's frontier advance makes everything visible.
	off, err := p2.Append([]byte("new"))
	if err != nil || off != 2 {
		t.Fatalf("append = %d, %v; want 2, nil", off, err)
	}
	recs, err = p2.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) != 3 {
		t.Fatalf("post-advance Fetch = %d recs, %v; want 3", len(recs), err)
	}
	if string(recs[1].Payload) != "unacked" {
		t.Errorf("offset 1 = %q, want the kept unacked record", recs[1].Payload)
	}
}

func TestRecoveryRefusesNonEmptyLogWithMissingFrontier(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("data"))
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "frontier")); err != nil {
		t.Fatal(err)
	}

	_, err = openPartition(OSFS(), dir, FileSyncer{})
	if err == nil || !strings.Contains(err.Error(), "missing frontier") {
		t.Fatalf("reopen = %v, want loud refusal about the missing frontier", err)
	}
}
