// Syncer seam (DD-6): the injectable fsync boundary LOG-1a's ordering
// recorder hooks into. Implementations MAY BLOCK — the flusher tolerates a
// gated Sync, which is what makes the wake and ordering tests legal.
package storage

// Syncer performs the covering fsync for a flushed batch.
type Syncer interface {
	Sync(f File) error
}

// FileSyncer is the real implementation: plain File.Sync on both OSes
// (fsync honesty per DD-7 — the macOS drive-cache caveat is documented, not
// worked around).
type FileSyncer struct{}

// Sync fsyncs the file.
func (FileSyncer) Sync(f File) error { return f.Sync() }
