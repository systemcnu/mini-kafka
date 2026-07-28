// FS seam (D-SL0-4): the injectable filesystem boundary, its real osFS
// implementation, and the DD-9 atomicWrite recipe. SL1's fault fakes script
// this interface's failure POST-STATES — the recipe itself stays opaque.
package storage

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// File is the seam's handle: append writes, positional reads, fsync, size.
type File interface {
	io.Writer
	io.ReaderAt
	Sync() error
	Close() error
	Size() (int64, error)
}

// FS is the filesystem seam. Contract: every not-found condition satisfies
// errors.Is(err, fs.ErrNotExist) — the fresh-vs-refuse recovery boundary
// (DD-4/DD-9) depends on it, and fakes must honor it.
type FS interface {
	// OpenAppend opens path for appending, creating it if absent.
	OpenAppend(path string) (File, error)
	// OpenRead opens path read-only.
	OpenRead(path string) (File, error)
	// ReadFile returns the whole content of path.
	ReadFile(path string) ([]byte, error)
	// WriteFileAtomic replaces path with data using the DD-9 recipe:
	// temp in the destination dir → write → fsync(temp) → rename →
	// fsync(dir). The live file is never torn.
	WriteFileAtomic(path string, data []byte) error
	// Truncate cuts path to size bytes.
	Truncate(path string, size int64) error
	// Rename atomically moves oldPath to newPath (same filesystem).
	Rename(oldPath, newPath string) error
	// MkdirAll creates path and any missing parents.
	MkdirAll(path string) error
	// RemoveAll deletes path and everything under it.
	RemoveAll(path string) error
	// ReadDir lists path's entries.
	ReadDir(path string) ([]fs.DirEntry, error)
	// SyncDir fsyncs the directory itself (makes renames/creates durable).
	SyncDir(path string) error
}

// OSFS returns the real operating-system implementation of the seam.
func OSFS() FS { return osFS{} }

type osFS struct{}

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (osFS) OpenAppend(path string) (File, error) {
	// O_RDWR (not O_WRONLY) because the same handle serves fetch ReadAt.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) OpenRead(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

func (o osFS) WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	// The temp file MUST live in the destination dir: rename is only atomic
	// within one filesystem, and macOS refuses cross-dir cases.
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return o.SyncDir(dir)
}

func (osFS) Truncate(path string, size int64) error     { return os.Truncate(path, size) }
func (osFS) Rename(oldPath, newPath string) error       { return os.Rename(oldPath, newPath) }
func (osFS) MkdirAll(path string) error                 { return os.MkdirAll(path, 0o755) }
func (osFS) RemoveAll(path string) error                { return os.RemoveAll(path) }
func (osFS) ReadDir(path string) ([]fs.DirEntry, error) { return os.ReadDir(path) }

func (osFS) SyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
