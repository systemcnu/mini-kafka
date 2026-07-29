// One partition: append queue, group-commit flusher, durable frontier,
// notify channel, frontier-capped reads with long-poll parking. The
// load-bearing invariant lives here: append → fsync → frontier atomicWrite →
// ack (DD-4/DD-6), with the notify swap and frontier update under the write
// lock (D-SL0-5).
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// flushWindow is DD-6's group-commit trigger: fsync when the oldest waiter
// is this old (time-only — a byte trigger would be dead machinery at our
// message sizes).
const flushWindow = 5 * time.Millisecond

var (
	// ErrWriteRejected is LOG-5's sticky degrade: a partition that saw an
	// append/fsync error rejects writes until restart; reads continue.
	ErrWriteRejected = errors.New("partition is write-rejecting until restart")
	// ErrStopped means the partition is shutting down.
	ErrStopped = errors.New("partition stopped")
	// ErrCanceled means a parked fetch was abandoned because its
	// connection went away.
	ErrCanceled = errors.New("fetch canceled")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is one stored record as served to fetch.
type Record struct {
	Offset  uint64
	Payload []byte
}

// recMeta locates one record inside the log file.
type recMeta struct {
	payloadPos int64
	payloadLen uint32
	end        int64 // one past the record; a frontier candidate
}

type waiter struct {
	payload []byte
	offset  uint64
	done    chan error
}

// Partition is one append-only log plus its flusher goroutine. Writers block
// in Append until their covering fsync and frontier advance complete;
// readers never see past the frontier (DD-5).
type Partition struct {
	fsys   FS
	dir    string
	file   File
	syncer Syncer

	// qmu guards the waiter queue. Offsets are assigned at APPEND time so
	// concurrent producers' ProduceResp offsets cannot misorder relative to
	// the log (PLAN pitfall #1).
	qmu        sync.Mutex
	queue      []waiter
	oldestAt   time.Time
	inflight   int
	nextOffset uint64
	stopped    bool

	kick chan struct{}
	quit chan struct{}
	dead chan struct{}

	// fileSize is flusher-owned: bytes physically appended to the log.
	fileSize int64

	// mu is the partition lock of the D-SL0-5 atomicity pin: frontier value
	// update + old-notify close + new-notify install happen under the write
	// lock; a fetch's tail-check + channel-capture happen under the read
	// lock. That pairing is the missed-wakeup proof.
	mu          sync.RWMutex
	index       []recMeta
	durable     int // records covered by the frontier
	frontier    int64
	notify      chan struct{}
	failed      bool
	advanceHook func(frontier int64)

	parked atomic.Int64
}

// openPartition recovers dir and starts the flusher.
func openPartition(fsys FS, dir string, syncer Syncer) (*Partition, error) {
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	st, err := recoverPartition(fsys, dir)
	if err != nil {
		return nil, err
	}
	file, err := fsys.OpenAppend(filepath.Join(dir, "log"))
	if err != nil {
		return nil, err
	}
	p := &Partition{
		fsys:       fsys,
		dir:        dir,
		file:       file,
		syncer:     syncer,
		nextOffset: uint64(len(st.metas)),
		kick:       make(chan struct{}, 1),
		quit:       make(chan struct{}),
		dead:       make(chan struct{}),
		fileSize:   st.fileSize,
		index:      st.metas,
		durable:    st.durable,
		frontier:   st.frontier,
		notify:     make(chan struct{}),
	}
	go p.flusher()
	return p, nil
}

// Append blocks until the payload is written, fsynced, and covered by the
// durable frontier, then returns its offset (PROD-2's ack).
func (p *Partition) Append(payload []byte) (uint64, error) {
	p.mu.RLock()
	failed := p.failed
	p.mu.RUnlock()
	if failed {
		return 0, ErrWriteRejected
	}
	w := waiter{payload: payload, done: make(chan error, 1)}
	p.qmu.Lock()
	if p.stopped {
		p.qmu.Unlock()
		return 0, ErrStopped
	}
	w.offset = p.nextOffset
	p.nextOffset++
	if len(p.queue) == 0 {
		p.oldestAt = time.Now()
	}
	p.queue = append(p.queue, w)
	p.qmu.Unlock()
	select {
	case p.kick <- struct{}{}:
	default:
	}
	if err := <-w.done; err != nil {
		return 0, err
	}
	return w.offset, nil
}

// Fetch returns records at/after offset, never past the durable frontier.
// If nothing is available it parks on the partition's notify channel until
// frontier advance, the maxWait timer, broker stop, or connection
// cancellation — recapturing the channel under the read lock each loop, which
// is what kills the missed-wakeup race (DD-25).
func (p *Partition) Fetch(offset uint64, maxBytes uint32, maxWait time.Duration, stop, cancel <-chan struct{}) ([]Record, error) {
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	for {
		p.mu.RLock()
		recs, err := p.readLocked(offset, maxBytes)
		notify := p.notify
		p.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		if len(recs) > 0 {
			return recs, nil
		}
		p.parked.Add(1)
		select {
		case <-notify:
			p.parked.Add(-1)
		case <-timer.C:
			p.parked.Add(-1)
			return nil, nil
		case <-stop:
			p.parked.Add(-1)
			return nil, nil
		case <-cancel:
			p.parked.Add(-1)
			return nil, ErrCanceled
		}
	}
}

// readLocked serves index entries below the frontier. Caller holds mu.R.
func (p *Partition) readLocked(offset uint64, maxBytes uint32) ([]Record, error) {
	if offset >= uint64(p.durable) {
		return nil, nil
	}
	var out []Record
	var total uint32
	for i := offset; i < uint64(p.durable); i++ {
		m := p.index[i]
		// Response-encoding size of one record: u64 offset + u32 blob len.
		sz := 12 + m.payloadLen
		// Always serve at least one record so a record larger than
		// maxBytes cannot wedge a consumer forever.
		if len(out) > 0 && total+sz > maxBytes {
			break
		}
		payload := make([]byte, m.payloadLen)
		if _, err := p.file.ReadAt(payload, m.payloadPos); err != nil {
			return nil, fmt.Errorf("partition %s: reading record %d: %w", p.dir, i, err)
		}
		out = append(out, Record{Offset: i, Payload: payload})
		total += sz
		if total >= maxBytes {
			break
		}
	}
	return out, nil
}

// TryFetch is the non-parking read for the broker's multi-entry fetch loop
// (D-SL2-7): records at/after offset (never past the durable frontier) plus
// the CURRENT notify channel, both captured in the SAME read-lock section —
// the missed-wakeup pin Fetch uses. The caller parks on notify itself.
func (p *Partition) TryFetch(offset uint64, maxBytes uint32) ([]Record, <-chan struct{}, error) {
	p.mu.RLock()
	recs, err := p.readLocked(offset, maxBytes)
	notify := p.notify
	p.mu.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	return recs, notify, nil
}

// TrackPark counts an EXTERNAL parked waiter (the broker's multi-entry park
// goroutines) in the ParkedWaiters observable; the returned func un-counts
// it. Keeps D-SL0-5's observable truthful for parks that happen outside
// Partition.Fetch.
func (p *Partition) TrackPark() (unpark func()) {
	p.parked.Add(1)
	return func() { p.parked.Add(-1) }
}

// Frontier returns the durable byte length of the log.
func (p *Partition) Frontier() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.frontier
}

// ParkedWaiters is the D-SL0-5 observable: fetches currently parked.
func (p *Partition) ParkedWaiters() int { return int(p.parked.Load()) }

// QueuedWaiters counts produce waiters not yet acked (queued or in the
// batch being flushed); the graceful-stop drain waits on it.
func (p *Partition) QueuedWaiters() int {
	p.qmu.Lock()
	defer p.qmu.Unlock()
	return len(p.queue) + p.inflight
}

// SetAdvanceHook installs the test seam fired after each frontier advance
// (DESIGN §9). Set before concurrent use.
func (p *Partition) SetAdvanceHook(fn func(frontier int64)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.advanceHook = fn
}

// Close stops accepting appends, lets the flusher drain what is already
// queued, joins it, and closes the log file (graceful-stop steps 5–6).
func (p *Partition) Close() error {
	p.qmu.Lock()
	alreadyStopped := p.stopped
	p.stopped = true
	p.qmu.Unlock()
	if !alreadyStopped {
		close(p.quit)
	}
	<-p.dead
	return p.file.Close()
}

// flusher is the per-partition group-commit goroutine (DD-6).
func (p *Partition) flusher() {
	defer close(p.dead)
	for {
		select {
		case <-p.kick:
		case <-p.quit:
			p.flushRemaining()
			return
		}
		for {
			p.qmu.Lock()
			if len(p.queue) == 0 {
				p.qmu.Unlock()
				break
			}
			wait := flushWindow - time.Since(p.oldestAt)
			if wait <= 0 {
				batch := p.queue
				p.queue = nil
				p.inflight = len(batch)
				p.qmu.Unlock()
				p.flush(batch)
				continue
			}
			p.qmu.Unlock()
			select {
			case <-time.After(wait):
			case <-p.quit:
				p.flushRemaining()
				return
			}
		}
	}
}

// flushRemaining flushes whatever is still queued at quit so already-queued
// waiters get their acks (graceful stop drains a snapshot, never chases
// quiescence).
func (p *Partition) flushRemaining() {
	p.qmu.Lock()
	batch := p.queue
	p.queue = nil
	p.inflight = len(batch)
	p.qmu.Unlock()
	if len(batch) > 0 {
		p.flush(batch)
	}
}

// flush is the invariant: write → fsync (Syncer seam) → frontier atomicWrite
// → index/frontier/notify swap under the write lock → ack.
func (p *Partition) flush(batch []waiter) {
	p.mu.RLock()
	failed := p.failed
	p.mu.RUnlock()
	if failed {
		p.failAll(batch, ErrWriteRejected)
		return
	}

	var body []byte
	metas := make([]recMeta, 0, len(batch))
	pos := p.fileSize
	for _, w := range batch {
		rec := encodeRecord(w.payload)
		body = append(body, rec...)
		metas = append(metas, recMeta{
			payloadPos: pos + 8,
			payloadLen: uint32(len(w.payload)),
			end:        pos + int64(len(rec)),
		})
		pos += int64(len(rec))
	}

	if _, err := p.file.Write(body); err != nil {
		p.degrade(true)
		p.failAll(batch, ErrWriteRejected)
		return
	}

	if err := p.syncer.Sync(p.file); err != nil {
		p.degrade(true)
		p.failAll(batch, ErrWriteRejected)
		return
	}
	if err := p.fsys.WriteFileAtomic(filepath.Join(p.dir, "frontier"), encodeFrontier(pos)); err != nil {
		// NO truncate-back here (D-SL1-3): the atomicWrite may have failed
		// AFTER its rename installed the NEW frontier, and truncating to the
		// old value could leave frontier > log length — a boot refusal.
		p.degrade(false)
		p.failAll(batch, ErrWriteRejected)
		return
	}
	p.fileSize = pos

	p.mu.Lock()
	p.index = append(p.index, metas...)
	p.durable = len(p.index)
	p.frontier = pos
	close(p.notify)
	p.notify = make(chan struct{})
	hook := p.advanceHook
	p.mu.Unlock()
	if hook != nil {
		hook(pos)
	}

	// Only now — after the covering fsync and frontier advance — do the
	// producers get their acks (the DD-4 invariant).
	for _, w := range batch {
		w.done <- nil
	}
	p.qmu.Lock()
	p.inflight = 0
	p.qmu.Unlock()
}

// degrade is DD-8's flush-failure path (D-SL1-3): mark the partition
// write-rejecting FIRST (sticky until restart — stops new appends racing the
// repair), then, only when truncateBack is set, best-effort truncate the log
// back to the frontier and make the cut durable via a fresh handle
// (recovery.go's pattern). truncateBack is legal ONLY on Write/Syncer.Sync
// failures, where the on-disk frontier provably still equals the in-memory
// one. A failed repair is accepted: reads are frontier-capped (DD-5) so the
// torn range is never servable, and restart's scan re-truncates.
func (p *Partition) degrade(truncateBack bool) {
	p.mu.Lock()
	p.failed = true
	frontier := p.frontier
	p.mu.Unlock()
	if !truncateBack {
		return
	}
	// Path-based Truncate while the O_APPEND handle stays open is safe ONLY
	// because the flusher — the goroutine running this repair — is the sole
	// writer. Do not close/reopen, and do not call this off the flusher.
	logPath := filepath.Join(p.dir, "log")
	if err := p.fsys.Truncate(logPath, frontier); err != nil {
		return
	}
	f, err := p.fsys.OpenAppend(logPath)
	if err != nil {
		return
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return
	}
	f.Close()
}

func (p *Partition) failAll(batch []waiter, err error) {
	for _, w := range batch {
		w.done <- err
	}
	p.qmu.Lock()
	p.inflight = 0
	p.qmu.Unlock()
}

// encodeRecord builds DD-3's [u32 len][u32 crc32c][payload], CRC over the
// payload, big-endian.
func encodeRecord(payload []byte) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(b, uint32(len(payload)))
	binary.BigEndian.PutUint32(b[4:], crc32.Checksum(payload, castagnoli))
	copy(b[8:], payload)
	return b
}

// encodeFrontier builds DD-4's [u64 length][u32 crc32c], CRC over the length
// bytes.
func encodeFrontier(length int64) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b, uint64(length))
	binary.BigEndian.PutUint32(b[8:], crc32.Checksum(b[:8], castagnoli))
	return b
}

// parseFrontier is encodeFrontier's strict inverse; ok is false on any size
// or CRC mismatch (the caller decides refuse-vs-fresh).
func parseFrontier(b []byte) (int64, bool) {
	if len(b) != 12 {
		return 0, false
	}
	if crc32.Checksum(b[:8], castagnoli) != binary.BigEndian.Uint32(b[8:]) {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(b)), true
}
