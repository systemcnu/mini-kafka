// DD-4 boot scan, all three branches: valid parse to the frontier,
// refuse-loudly on acked damage or an unreadable frontier, truncate the
// invalid unacked tail past the frontier. SL1 owns the scripted-fault
// PROOFS; the full mechanism ships here.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"path/filepath"
)

// scanState is what recovery hands the partition: metadata for every valid
// record (including kept unacked records past the frontier), the durable
// frontier, and how many records it covers.
type scanState struct {
	metas    []recMeta
	durable  int
	frontier int64
	fileSize int64
}

// recoverPartition runs the DD-4 boot scan for one partition directory.
// Refusals are loud errors naming the partition; the store aborts boot on
// them rather than guessing at acked data.
func recoverPartition(fsys FS, dir string) (scanState, error) {
	logPath := filepath.Join(dir, "log")
	frontierPath := filepath.Join(dir, "frontier")

	fb, err := fsys.ReadFile(frontierPath)
	// A 0-byte frontier is equivalent to a missing one: topic creation
	// makes empty files first and only fresh partitions may look like that.
	frontierMissing := errors.Is(err, fs.ErrNotExist) || (err == nil && len(fb) == 0)
	if err != nil && !frontierMissing {
		return scanState{}, fmt.Errorf("partition %s: reading frontier: %w", dir, err)
	}

	var logBytes []byte
	if lb, err := fsys.ReadFile(logPath); err == nil {
		logBytes = lb
	} else if !errors.Is(err, fs.ErrNotExist) {
		return scanState{}, fmt.Errorf("partition %s: reading log: %w", dir, err)
	}
	logSize := int64(len(logBytes))

	if frontierMissing {
		if logSize == 0 {
			// Fresh partition: initialize the frontier to 0 on disk so a
			// re-scan reproduces the same decision (recovery idempotence).
			if err := fsys.WriteFileAtomic(frontierPath, encodeFrontier(0)); err != nil {
				return scanState{}, fmt.Errorf("partition %s: initializing frontier: %w", dir, err)
			}
			return scanState{}, nil
		}
		return scanState{}, fmt.Errorf("partition %s: refusing to load: non-empty log with missing frontier", dir)
	}

	frontier, ok := parseFrontier(fb)
	if !ok {
		// atomicWrite makes a benign torn frontier impossible, so an
		// unreadable one is real corruption — refuse, never guess.
		return scanState{}, fmt.Errorf("partition %s: refusing to load: frontier file unreadable or CRC-bad", dir)
	}
	if frontier > logSize {
		return scanState{}, fmt.Errorf("partition %s: refusing to load: frontier %d beyond log length %d", dir, frontier, logSize)
	}

	// Parse the log from 0. Records must tile [0, frontier) exactly; past
	// the frontier the first invalid record ends the log (never-acked data).
	var metas []recMeta
	pos := int64(0)
	truncated := false
	for pos < logSize {
		plen, valid := parseRecordAt(logBytes, pos)
		if !valid {
			if pos < frontier {
				return scanState{}, fmt.Errorf("partition %s: refusing to load: invalid record at byte %d inside acked range [0,%d)", dir, pos, frontier)
			}
			truncated = true
			break
		}
		end := pos + 8 + int64(plen)
		if pos < frontier && end > frontier {
			return scanState{}, fmt.Errorf("partition %s: refusing to load: record at byte %d straddles frontier %d", dir, pos, frontier)
		}
		metas = append(metas, recMeta{payloadPos: pos + 8, payloadLen: plen, end: end})
		pos = end
	}

	// Exact-consume check: the durable prefix must cover precisely the
	// frontier's bytes.
	durable := 0
	covered := int64(0)
	for _, m := range metas {
		if m.end > frontier {
			break
		}
		durable++
		covered = m.end
	}
	if covered != frontier {
		return scanState{}, fmt.Errorf("partition %s: refusing to load: parse consumed %d bytes of acked range, frontier says %d", dir, covered, frontier)
	}

	if truncated {
		if err := fsys.Truncate(logPath, pos); err != nil {
			return scanState{}, fmt.Errorf("partition %s: truncating unacked tail: %w", dir, err)
		}
		// Make the truncation itself durable before serving anything.
		f, err := fsys.OpenAppend(logPath)
		if err != nil {
			return scanState{}, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return scanState{}, err
		}
		if err := f.Close(); err != nil {
			return scanState{}, err
		}
	}

	return scanState{metas: metas, durable: durable, frontier: frontier, fileSize: pos}, nil
}

// parseRecordAt validates one DD-3 record ([u32 len][u32 crc32c][payload])
// at pos: valid iff its full byte range fits the buffer and the CRC matches.
func parseRecordAt(b []byte, pos int64) (payloadLen uint32, valid bool) {
	if pos+8 > int64(len(b)) {
		return 0, false
	}
	plen := binary.BigEndian.Uint32(b[pos:])
	end := pos + 8 + int64(plen)
	if end > int64(len(b)) {
		return 0, false
	}
	want := binary.BigEndian.Uint32(b[pos+4:])
	if crc32.Checksum(b[pos+8:end], castagnoli) != want {
		return 0, false
	}
	return plen, true
}
