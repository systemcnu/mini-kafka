package main

import (
	"runtime"
	"sync/atomic"
	"time"
)

// partRow is one per-partition consumed-offset frontier row in the feed (§J).
type partRow struct {
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"nextOffset"`
}

// recentRow is one consumed record in the feed's ring of 50 (§J).
type recentRow struct {
	Partition uint32 `json:"partition"`
	Offset    uint64 `json:"offset"`
	Payload   string `json:"payload"`
}

// snapshot is the immutable ten-field feed contract (D-SL7-3, PLAN §J).
// A stored snapshot is NEVER mutated afterward: every slice is freshly
// built (deep-copied) at swap time, so handlers can read it lock-free.
type snapshot struct {
	Status        string      `json:"status"` // statusLive | statusPaused
	UptimeSeconds int64       `json:"uptimeSeconds"`
	Produced      uint64      `json:"produced"`
	Partitions    []partRow   `json:"partitions"`
	Recent        []recentRow `json:"recent"`
	Assignment    []uint32    `json:"assignment"`
	DiskBytes     int64       `json:"diskBytes"`
	DiskCapBytes  int64       `json:"diskCapBytes"`
	MemBytes      uint64      `json:"memBytes"`
	StartedAt     string      `json:"startedAt"` // RFC3339
}

// snapshotHolder is the ONE atomic.Value of PLAN §S: every store is the
// same concrete type *snapshot (atomic.Value panics on a type change), and
// HTTP handlers do exactly one thing with it — load + marshal.
type snapshotHolder struct {
	v atomic.Value
}

// newSnapshotHolder seeds a valid zero-state snapshot BEFORE any listener
// starts, so a request racing startup still reads a complete snapshot (§S).
func newSnapshotHolder(capBytes int64, startedAt time.Time) *snapshotHolder {
	h := &snapshotHolder{}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	parts := make([]partRow, 0, feedPartitions)
	for p := uint32(0); p < feedPartitions; p++ {
		parts = append(parts, partRow{Partition: p})
	}
	h.store(&snapshot{
		Status:       statusLive,
		Partitions:   parts,
		Recent:       []recentRow{},
		Assignment:   []uint32{},
		DiskCapBytes: capBytes,
		MemBytes:     ms.HeapAlloc,
		StartedAt:    startedAt.UTC().Format(time.RFC3339),
	})
	return h
}

func (h *snapshotHolder) load() *snapshot   { return h.v.Load().(*snapshot) }
func (h *snapshotHolder) store(s *snapshot) { h.v.Store(s) }
