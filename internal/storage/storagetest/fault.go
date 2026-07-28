// Fault-scripted wrappers over the storage seams (D-SL1-1/2): FaultFS and
// FaultFile script API failures at the FS boundary, FaultSyncer at the
// flusher's covering fsync. Test-only by placement — imported ONLY from
// _test.go files; an import from production code is a review-visible defect.
package storagetest

import (
	"io/fs"
	"strings"
	"sync"

	"github.com/systemcnu/mini-kafka/internal/storage"
)

// script is one armed fault: fail the nth (1-based, counted from arming)
// call whose path ends in suffix. shortN applies to File.Write only — bytes
// really written before the error is reported.
type script struct {
	suffix string
	nth    int
	err    error
	shortN int
	seen   int
	fired  bool
}

// The scriptable seam calls (D-SL1-2).
const (
	opOpenAppend      = "OpenAppend"
	opWrite           = "Write"
	opSync            = "Sync"
	opWriteFileAtomic = "WriteFileAtomic"
	opTruncate        = "Truncate"
	opSyncDir         = "SyncDir"
)

// FaultFS wraps a real FS (usually storage.OSFS()) and fails scripted calls.
// Files returned by OpenAppend are wrapped so File.Write/File.Sync scripts
// fire on them; everything unscripted delegates untouched, so the seam's
// fs.ErrNotExist contract passes through.
type FaultFS struct {
	inner storage.FS

	mu      sync.Mutex
	scripts map[string][]*script
	// wfaNew alternates the WriteFileAtomic failure post-state between
	// old-intact (inner skipped) and new-installed (inner ran, error still
	// reported — the recipe can fail AFTER its rename, on the dir-fsync).
	// D-SL1-1 pins the post-state as old OR new, never torn, unspecified
	// which; alternating keeps callers from assuming either.
	wfaNew bool
}

// WrapFS wraps inner with an unarmed FaultFS.
func WrapFS(inner storage.FS) *FaultFS {
	return &FaultFS{inner: inner, scripts: make(map[string][]*script)}
}

// FailOpenAppend arms: the nth OpenAppend of a path ending in suffix fails
// with err.
func (f *FaultFS) FailOpenAppend(suffix string, nth int, err error) {
	f.arm(opOpenAppend, suffix, nth, err, 0)
}

// FailWrite arms: the nth File.Write on a file opened from a path ending in
// suffix writes shortN bytes for real, then reports (shortN, err). shortN 0
// writes nothing (a plain write failure).
func (f *FaultFS) FailWrite(suffix string, nth, shortN int, err error) {
	f.arm(opWrite, suffix, nth, err, shortN)
}

// FailSync arms: the nth File.Sync on a file opened from a path ending in
// suffix fails with err. Direct File.Sync sites — CreateTopic's file fsyncs,
// the truncate-back repair's fresh-handle sync — never pass through the
// Syncer seam, which is why this script exists alongside FaultSyncer.
func (f *FaultFS) FailSync(suffix string, nth int, err error) {
	f.arm(opSync, suffix, nth, err, 0)
}

// FailWriteFileAtomic arms: the nth WriteFileAtomic of a path ending in
// suffix fails with err, leaving the target old OR new — never torn,
// unspecified which (D-SL1-1).
func (f *FaultFS) FailWriteFileAtomic(suffix string, nth int, err error) {
	f.arm(opWriteFileAtomic, suffix, nth, err, 0)
}

// FailTruncate arms: the nth Truncate of a path ending in suffix fails with
// err, leaving the file untouched.
func (f *FaultFS) FailTruncate(suffix string, nth int, err error) {
	f.arm(opTruncate, suffix, nth, err, 0)
}

// FailSyncDir arms: the nth SyncDir of a path ending in suffix fails with
// err.
func (f *FaultFS) FailSyncDir(suffix string, nth int, err error) {
	f.arm(opSyncDir, suffix, nth, err, 0)
}

func (f *FaultFS) arm(op, suffix string, nth int, err error, shortN int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scripts[op] = append(f.scripts[op], &script{suffix: suffix, nth: nth, err: err, shortN: shortN})
}

// check counts a call of op on path against EVERY live matching script —
// each keeps its own nth counter — and returns the first fault that fires.
// A fired script is spent.
func (f *FaultFS) check(op, path string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	var shortN int
	for _, s := range f.scripts[op] {
		if s.fired || !strings.HasSuffix(path, s.suffix) {
			continue
		}
		s.seen++
		if s.seen == s.nth && err == nil {
			s.fired = true
			err, shortN = s.err, s.shortN
		}
	}
	return shortN, err
}

func (f *FaultFS) OpenAppend(path string) (storage.File, error) {
	if _, err := f.check(opOpenAppend, path); err != nil {
		return nil, err
	}
	file, err := f.inner.OpenAppend(path)
	if err != nil {
		return nil, err
	}
	return &FaultFile{File: file, fs: f, path: path}, nil
}

func (f *FaultFS) OpenRead(path string) (storage.File, error) { return f.inner.OpenRead(path) }
func (f *FaultFS) ReadFile(path string) ([]byte, error)       { return f.inner.ReadFile(path) }

func (f *FaultFS) WriteFileAtomic(path string, data []byte) error {
	_, err := f.check(opWriteFileAtomic, path)
	if err == nil {
		return f.inner.WriteFileAtomic(path, data)
	}
	f.mu.Lock()
	installNew := f.wfaNew
	f.wfaNew = !f.wfaNew
	f.mu.Unlock()
	if installNew {
		// Fail-after-rename shape: the new content IS installed and the
		// caller still sees the error. If the real recipe itself fails the
		// target is old-intact — still within the pinned post-states.
		_ = f.inner.WriteFileAtomic(path, data)
	}
	return err
}

func (f *FaultFS) Truncate(path string, size int64) error {
	if _, err := f.check(opTruncate, path); err != nil {
		return err
	}
	return f.inner.Truncate(path, size)
}

func (f *FaultFS) Rename(oldPath, newPath string) error       { return f.inner.Rename(oldPath, newPath) }
func (f *FaultFS) MkdirAll(path string) error                 { return f.inner.MkdirAll(path) }
func (f *FaultFS) RemoveAll(path string) error                { return f.inner.RemoveAll(path) }
func (f *FaultFS) ReadDir(path string) ([]fs.DirEntry, error) { return f.inner.ReadDir(path) }

func (f *FaultFS) SyncDir(path string) error {
	if _, err := f.check(opSyncDir, path); err != nil {
		return err
	}
	return f.inner.SyncDir(path)
}

// FaultFile is FaultFS's wrapped handle: Write and Sync consult the owning
// FS's scripts under the path the file was opened with.
type FaultFile struct {
	storage.File
	fs   *FaultFS
	path string
}

func (f *FaultFile) Write(p []byte) (int, error) {
	shortN, err := f.fs.check(opWrite, f.path)
	if err == nil {
		return f.File.Write(p)
	}
	if shortN > len(p) {
		shortN = len(p)
	}
	n := 0
	if shortN > 0 {
		// A short write leaves the file REALLY short (D-SL1-1): the prefix
		// hits the disk and the caller sees n < len(p).
		var werr error
		n, werr = f.File.Write(p[:shortN])
		if werr != nil {
			return n, werr
		}
	}
	return n, err
}

func (f *FaultFile) Sync() error {
	if _, err := f.fs.check(opSync, f.path); err != nil {
		return err
	}
	return f.File.Sync()
}

// FaultSyncer wraps the flusher's covering-fsync seam (D-SL1-2): fail the
// nth Sync (1-based, counted from arming) with the armed error.
type FaultSyncer struct {
	Inner storage.Syncer

	mu   sync.Mutex
	nth  int
	err  error
	seen int
}

// FailNth arms: the nth Sync from now fails with err.
func (s *FaultSyncer) FailNth(nth int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nth, s.err, s.seen = nth, err, 0
}

func (s *FaultSyncer) Sync(f storage.File) error {
	s.mu.Lock()
	var err error
	if s.err != nil {
		s.seen++
		if s.seen == s.nth {
			err = s.err
			s.err = nil
		}
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.Inner.Sync(f)
}
