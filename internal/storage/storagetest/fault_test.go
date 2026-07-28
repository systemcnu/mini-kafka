// Contract self-tests for the fault fakes (D-SL1-1 post-states): a failed
// WriteFileAtomic leaves the target old OR new, never torn; a short write
// leaves the file really short — and File scripts fire through OpenAppend's
// wrapped handles (the PLAN's step-1 pitfall).
package storagetest

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/systemcnu/mini-kafka/internal/storage"
)

func TestWriteFileAtomicFailureLeavesOldOrNewNeverTorn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frontier")
	ffs := WrapFS(storage.OSFS())

	if err := ffs.WriteFileAtomic(path, []byte("old-content")); err != nil {
		t.Fatalf("unscripted WriteFileAtomic: %v", err)
	}
	ffs.FailWriteFileAtomic("frontier", 1, syscall.ENOSPC)
	ffs.FailWriteFileAtomic("frontier", 2, syscall.ENOSPC)

	// Two scripted failures: after each, the target must be exactly the
	// previous content or the attempted content — and across both, both
	// post-states must occur, so no caller can assume old-intact (the recipe
	// can fail after its rename).
	prev := "old-content"
	states := map[string]bool{}
	for i, attempt := range []string{"new-a", "new-b"} {
		err := ffs.WriteFileAtomic(path, []byte(attempt))
		if !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("scripted failure %d: err = %v, want ENOSPC", i+1, err)
		}
		got, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("target unreadable after failure %d: %v", i+1, rerr)
		}
		switch string(got) {
		case prev:
			states["old-kept"] = true
		case attempt:
			states["new-installed"] = true
			prev = attempt
		default:
			t.Fatalf("failure %d left torn content %q (want %q or %q)", i+1, got, prev, attempt)
		}
	}
	if !states["old-kept"] || !states["new-installed"] {
		t.Fatalf("post-states seen = %v, want both old-kept and new-installed", states)
	}
	// Spent scripts: the next write succeeds.
	if err := ffs.WriteFileAtomic(path, []byte("final")); err != nil {
		t.Fatalf("write after spent scripts: %v", err)
	}
}

func TestShortWriteLeavesFileReallyShort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	ffs := WrapFS(storage.OSFS())

	f, err := ffs.OpenAppend(path)
	if err != nil {
		t.Fatalf("OpenAppend: %v", err)
	}
	defer f.Close()

	ffs.FailWrite("log", 1, 3, syscall.ENOSPC)
	n, err := f.Write([]byte("0123456789"))
	if n != 3 || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("scripted short write = %d, %v; want 3, ENOSPC", n, err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 3 {
		t.Fatalf("on-disk size after short write = %v, %v; want 3 (file must be REALLY short)", info.Size(), err)
	}
	// Spent script: the next write lands whole.
	if n, err := f.Write([]byte("ab")); n != 2 || err != nil {
		t.Fatalf("write after spent script = %d, %v", n, err)
	}
	if info, _ := os.Stat(path); info.Size() != 5 {
		t.Fatalf("on-disk size = %d, want 5", info.Size())
	}
}

func TestWriteFailureWithZeroShortNWritesNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	ffs := WrapFS(storage.OSFS())

	f, err := ffs.OpenAppend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ffs.FailWrite("log", 1, 0, syscall.EIO)
	if n, err := f.Write([]byte("data")); n != 0 || !errors.Is(err, syscall.EIO) {
		t.Fatalf("scripted write = %d, %v; want 0, EIO", n, err)
	}
	if info, _ := os.Stat(path); info.Size() != 0 {
		t.Fatalf("on-disk size = %d, want 0", info.Size())
	}
}

func TestFileSyncScriptFiresOnNthMatchingCall(t *testing.T) {
	dir := t.TempDir()
	ffs := WrapFS(storage.OSFS())

	f, err := ffs.OpenAppend(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ffs.FailSync("log", 2, syscall.EIO)
	if err := f.Sync(); err != nil {
		t.Fatalf("sync 1 = %v, want nil (script armed for the 2nd)", err)
	}
	if err := f.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("sync 2 = %v, want EIO", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync 3 = %v, want nil (script spent)", err)
	}
}

func TestFSOpScriptsFireOncePerArming(t *testing.T) {
	dir := t.TempDir()
	ffs := WrapFS(storage.OSFS())
	logPath := filepath.Join(dir, "log")
	if f, err := ffs.OpenAppend(logPath); err != nil {
		t.Fatal(err)
	} else {
		f.Write([]byte("abc"))
		f.Close()
	}

	ffs.FailOpenAppend("log", 1, syscall.EMFILE)
	if _, err := ffs.OpenAppend(logPath); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("scripted OpenAppend = %v, want EMFILE", err)
	}
	if f, err := ffs.OpenAppend(logPath); err != nil {
		t.Fatalf("OpenAppend after spent script: %v", err)
	} else {
		f.Close()
	}

	ffs.FailTruncate("log", 1, syscall.EIO)
	if err := ffs.Truncate(logPath, 0); !errors.Is(err, syscall.EIO) {
		t.Fatalf("scripted Truncate = %v, want EIO", err)
	}
	if info, _ := os.Stat(logPath); info.Size() != 3 {
		t.Fatalf("failed Truncate touched the file: size %d, want 3", info.Size())
	}
	if err := ffs.Truncate(logPath, 0); err != nil {
		t.Fatalf("Truncate after spent script: %v", err)
	}

	ffs.FailSyncDir(filepath.Base(dir), 1, syscall.EIO)
	if err := ffs.SyncDir(dir); !errors.Is(err, syscall.EIO) {
		t.Fatalf("scripted SyncDir = %v, want EIO", err)
	}
	if err := ffs.SyncDir(dir); err != nil {
		t.Fatalf("SyncDir after spent script: %v", err)
	}

	// A script for another suffix never fires.
	ffs.FailTruncate("frontier", 1, syscall.EIO)
	if err := ffs.Truncate(logPath, 0); err != nil {
		t.Fatalf("Truncate with non-matching script = %v, want nil", err)
	}
}

func TestFaultSyncerFailsNthFromArming(t *testing.T) {
	dir := t.TempDir()
	ffs := WrapFS(storage.OSFS())
	f, err := ffs.OpenAppend(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	s := &FaultSyncer{Inner: storage.FileSyncer{}}
	if err := s.Sync(f); err != nil {
		t.Fatalf("unarmed Sync = %v", err)
	}
	s.FailNth(2, syscall.EIO)
	if err := s.Sync(f); err != nil {
		t.Fatalf("armed Sync 1 = %v, want nil", err)
	}
	if err := s.Sync(f); !errors.Is(err, syscall.EIO) {
		t.Fatalf("armed Sync 2 = %v, want EIO", err)
	}
	if err := s.Sync(f); err != nil {
		t.Fatalf("Sync after firing = %v, want nil", err)
	}
}

func TestNotFoundContractPassesThrough(t *testing.T) {
	// The fresh-vs-refuse boundary (DD-4/DD-9) keys on fs.ErrNotExist; the
	// seam contract requires fakes to honor it.
	ffs := WrapFS(storage.OSFS())
	missing := filepath.Join(t.TempDir(), "nope")
	if _, err := ffs.ReadFile(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadFile: err = %v, want fs.ErrNotExist", err)
	}
	if _, err := ffs.ReadDir(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadDir: err = %v, want fs.ErrNotExist", err)
	}
}
