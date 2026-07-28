// DD-4 boot-scan tests: fresh init, restart serving acked data, truncation
// of an invalid unacked tail, kept valid unacked records, and the refusal
// matrix (SL1: short-write tail, CRC-corrupt tail, acked damage, straddle,
// frontier CRC-bad/beyond-length, double-boot idempotence) — all staged as
// real bytes on real files; corruption is a disk state, not an API result.
package storage

import (
	"bytes"
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

// stageAckedPartition creates a partition in dir, appends payloads (all
// acked), closes it, and returns the durable frontier.
func stageAckedPartition(t *testing.T, dir string, payloads ...string) int64 {
	t.Helper()
	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range payloads {
		if _, err := p.Append([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	frontier := p.Frontier()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	return frontier
}

// appendRawBytes stages crash damage: raw bytes appended straight to the
// log file, bypassing every seam.
func appendRawBytes(t *testing.T, dir string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// corruptByteAt flips one byte of the named partition file in place.
func corruptByteAt(t *testing.T, dir, name string, off int64) {
	t.Helper()
	path := filepath.Join(dir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[off] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryTruncatesShortWriteTailPastFrontier(t *testing.T) {
	dir := t.TempDir()
	frontier := stageAckedPartition(t, dir, "keep1", "keep2")

	// LOG-4's fourth fault: a full header claiming 16 payload bytes with only
	// 5 present — a distinct parse branch from the header-doesn't-fit torn
	// tail. Never acked (past the frontier), so recovery must truncate it.
	rec := encodeRecord(bytes.Repeat([]byte{0xAB}, 16))
	appendRawBytes(t, dir, rec[:8+5])

	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("reopen after short-write tail: %v", err)
	}
	defer p.Close()

	if info, err := os.Stat(filepath.Join(dir, "log")); err != nil || info.Size() != frontier {
		t.Fatalf("log size after recovery = %v, %v; want truncated to frontier %d", info.Size(), err, frontier)
	}
	recs, err := p.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) != 2 {
		t.Fatalf("Fetch = %d recs, %v; want the 2 acked records", len(recs), err)
	}
	if off, err := p.Append([]byte("next")); err != nil || off != 2 {
		t.Fatalf("append after truncation = %d, %v; want 2, nil", off, err)
	}
}

func TestRecoveryTruncatesCRCCorruptTailPastFrontier(t *testing.T) {
	dir := t.TempDir()
	frontier := stageAckedPartition(t, dir, "keep1", "keep2")

	// A complete record past the frontier whose payload no longer matches its
	// CRC: never acked, so recovery truncates rather than serving damage.
	rec := encodeRecord([]byte("bad-crc-payload"))
	rec[9] ^= 0xFF
	appendRawBytes(t, dir, rec)

	p, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("reopen after CRC-corrupt tail: %v", err)
	}
	defer p.Close()

	if info, err := os.Stat(filepath.Join(dir, "log")); err != nil || info.Size() != frontier {
		t.Fatalf("log size after recovery = %v, %v; want truncated to frontier %d", info.Size(), err, frontier)
	}
	recs, err := p.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) != 2 {
		t.Fatalf("Fetch = %d recs, %v; want the 2 acked records", len(recs), err)
	}
	if off, err := p.Append([]byte("next")); err != nil || off != 2 {
		t.Fatalf("append after truncation = %d, %v; want 2, nil", off, err)
	}
}

func TestRecoveryRefusesCorruptedAckedByte(t *testing.T) {
	dir := t.TempDir()
	stageAckedPartition(t, dir, "aaaa", "bbbb")

	// Flip a payload byte of the SECOND acked record (bytes 12..23; payload
	// at 20): acked damage means the durability promise is already broken —
	// refuse loudly, never guess (LOG-4).
	corruptByteAt(t, dir, "log", 21)

	_, err := openPartition(OSFS(), dir, FileSyncer{})
	if err == nil {
		t.Fatal("reopen succeeded over corrupted acked data, want loud refusal")
	}
	for _, want := range []string{dir, "at byte 12", "acked range"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
}

func TestRecoveryRefusesRecordStraddlingFrontier(t *testing.T) {
	dir := t.TempDir()
	stageAckedPartition(t, dir, "aaaa")

	// A frontier of 5 lands inside the 12-byte record: only corruption can
	// produce it (records tile from 0), so recovery refuses. This is also the
	// sole reachable witness of the exact-consume guard (slice design §1).
	if err := os.WriteFile(filepath.Join(dir, "frontier"), encodeFrontier(5), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := openPartition(OSFS(), dir, FileSyncer{})
	if err == nil {
		t.Fatal("reopen succeeded with a mid-record frontier, want loud refusal")
	}
	for _, want := range []string{dir, "straddles frontier"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
}

func TestRecoveryRefusesFrontierCRCBad(t *testing.T) {
	dir := t.TempDir()
	stageAckedPartition(t, dir, "data")

	// atomicWrite makes a benign torn frontier impossible, so a CRC-bad one
	// is real corruption: refuse. Flipping a length byte breaks the CRC.
	corruptByteAt(t, dir, "frontier", 0)

	_, err := openPartition(OSFS(), dir, FileSyncer{})
	if err == nil {
		t.Fatal("reopen succeeded with a CRC-bad frontier, want loud refusal")
	}
	for _, want := range []string{dir, "frontier file unreadable or CRC-bad"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
}

func TestRecoveryRefusesFrontierBeyondLogLength(t *testing.T) {
	dir := t.TempDir()
	frontier := stageAckedPartition(t, dir, "data")

	// A frontier claiming more bytes than the log holds would promise acked
	// data that does not exist: refuse.
	if err := os.WriteFile(filepath.Join(dir, "frontier"), encodeFrontier(frontier+100), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := openPartition(OSFS(), dir, FileSyncer{})
	if err == nil {
		t.Fatal("reopen succeeded with a beyond-length frontier, want loud refusal")
	}
	for _, want := range []string{dir, "beyond log length"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
}

func TestRecoveryDoubleBootIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	frontier := stageAckedPartition(t, dir, "a", "bb")

	// Stage a short-write tail, boot twice: both boots must reach the same
	// decisions and leave byte-identical state (recovery idempotence — a
	// crash DURING recovery's own truncation replays cleanly).
	rec := encodeRecord(bytes.Repeat([]byte{0xCD}, 16))
	appendRawBytes(t, dir, rec[:8+3])

	readState := func() ([]byte, []byte) {
		logB, err := os.ReadFile(filepath.Join(dir, "log"))
		if err != nil {
			t.Fatal(err)
		}
		frontB, err := os.ReadFile(filepath.Join(dir, "frontier"))
		if err != nil {
			t.Fatal(err)
		}
		return logB, frontB
	}

	p1, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("first boot: %v", err)
	}
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}
	log1, front1 := readState()
	if int64(len(log1)) != frontier {
		t.Fatalf("log size after first boot = %d, want frontier %d", len(log1), frontier)
	}

	p2, err := openPartition(OSFS(), dir, FileSyncer{})
	if err != nil {
		t.Fatalf("second boot: %v", err)
	}
	recs, err := p2.Fetch(0, 1<<20, time.Second, nil, nil)
	if err != nil || len(recs) != 2 || string(recs[0].Payload) != "a" || string(recs[1].Payload) != "bb" {
		t.Fatalf("Fetch after second boot = %v, %v; want the 2 acked records", recs, err)
	}
	if err := p2.Close(); err != nil {
		t.Fatal(err)
	}
	log2, front2 := readState()
	if !bytes.Equal(log1, log2) || !bytes.Equal(front1, front2) {
		t.Fatalf("second boot changed on-disk state: log %d->%d bytes, frontier %x->%x", len(log1), len(log2), front1, front2)
	}
}
