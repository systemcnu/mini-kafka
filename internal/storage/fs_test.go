// WriteFileAtomic post-state tests plus the seam's not-found error contract
// (D-SL0-4). SL1 scripts the recipe's FAILURE post-states; SL0 proves the
// success post-states.
package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteFileAtomicCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frontier")
	fsys := OSFS()

	if err := fsys.WriteFileAtomic(path, []byte("v1")); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "v1" {
		t.Fatalf("post-state = %q, %v; want \"v1\", nil", got, err)
	}
}

func TestWriteFileAtomicReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frontier")
	fsys := OSFS()

	if err := fsys.WriteFileAtomic(path, []byte("old")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := fsys.WriteFileAtomic(path, []byte("new-longer")); err != nil {
		t.Fatalf("second write: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "new-longer" {
		t.Fatalf("post-state = %q, %v; want \"new-longer\", nil", got, err)
	}
}

func TestWriteFileAtomicLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	fsys := OSFS()
	if err := fsys.WriteFileAtomic(filepath.Join(dir, "meta.json"), []byte("{}")); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "meta.json" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("dir contents = %v, want only meta.json", strings.Join(names, ", "))
	}
}

func TestNotFoundSatisfiesErrNotExist(t *testing.T) {
	// The fresh-vs-refuse boundary (DD-4/DD-9) keys on fs.ErrNotExist, so
	// the real implementation must honor it (and SL1's fakes copy it).
	dir := t.TempDir()
	fsys := OSFS()
	missing := filepath.Join(dir, "nope")

	if _, err := fsys.ReadFile(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadFile: err = %v, want fs.ErrNotExist", err)
	}
	if _, err := fsys.OpenRead(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("OpenRead: err = %v, want fs.ErrNotExist", err)
	}
	if _, err := fsys.ReadDir(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadDir: err = %v, want fs.ErrNotExist", err)
	}
}

func TestOpenAppendCreatesAndReportsSize(t *testing.T) {
	dir := t.TempDir()
	fsys := OSFS()
	path := filepath.Join(dir, "log")

	f, err := fsys.OpenAppend(path)
	if err != nil {
		t.Fatalf("OpenAppend: %v", err)
	}
	defer f.Close()
	if n, err := f.Size(); err != nil || n != 0 {
		t.Fatalf("fresh Size = %d, %v; want 0, nil", n, err)
	}
	if _, err := f.Write([]byte("abcde")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n, err := f.Size(); err != nil || n != 5 {
		t.Fatalf("Size after write = %d, %v; want 5, nil", n, err)
	}
	buf := make([]byte, 3)
	if _, err := f.ReadAt(buf, 1); err != nil || string(buf) != "bcd" {
		t.Fatalf("ReadAt = %q, %v; want \"bcd\", nil", buf, err)
	}
}
