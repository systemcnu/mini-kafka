package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/systemcnu/mini-kafka/client"
	"github.com/systemcnu/mini-kafka/internal/broker"
)

const (
	statusLive   = "live"
	statusPaused = "paused-at-cap"

	feedTopic      = "showcase"
	feedGroup      = "showcase-watchers"
	feedPartitions = 4
	ringSize       = 50

	defaultTick      = 500 * time.Millisecond
	defaultWalkEvery = 30 * time.Second
	defaultCapMiB    = 200
)

// producerConn is the tiny in-package seam of PLAN §F — satisfied by
// *client.Producer, and by the failing fake the WRITE_FAILED test injects
// at the client boundary.
type producerConn interface {
	Produce(topic string, partition uint32, payload []byte) (uint64, error)
	Close() error
}

// feederConfig carries the §F test seams as struct fields — not flags, not
// env. Production main passes the pinned values (zero values take them).
type feederConfig struct {
	tmpRoot     string                                  // MkdirTemp root; "" → system temp (tests pass t.TempDir())
	tick        time.Duration                           // producer tick; 0 → 500 ms
	walkEvery   time.Duration                           // disk-walk interval; 0 → 30 s
	capBytes    int64                                   // disk cap; 0 → 200 MiB
	newProducer func(addr string) (producerConn, error) // nil → client.DialProducer
}

// feederState is the ONE mutex-guarded mutable state (§S): mutated ONLY by
// the producer, consumer, and walker goroutines. HTTP handlers never touch
// it — they read the holder's last complete snapshot.
type feederState struct {
	status     string
	paused     bool // sticky (ledger 8): set by cap-hit or WRITE_FAILED, never cleared
	produced   uint64
	next       [feedPartitions]uint64 // consumed-offset frontiers, all initialized 0
	ring       [ringSize]recentRow    // fixed circular buffer of consumed records
	ringLen    int
	ringPos    int // next write slot
	assignment []uint32
	diskBytes  int64
	memBytes   uint64
}

// feeder hosts the in-process broker and the producer/consumer/walker
// goroutines (D-SL7-1, PLAN §F). Everything flows through the public
// client surface; the broker is reached only via its loopback TCP addr.
type feeder struct {
	cfg    feederConfig
	holder *snapshotHolder

	dir       string // this boot's fresh MkdirTemp data dir
	srv       *broker.Server
	prod      producerConn
	gc        *client.GroupConsumer
	startedAt time.Time
	started   bool

	mu    sync.Mutex
	state feederState

	stopCh   chan struct{}
	stopOnce sync.Once
	prodDone chan struct{}
	consDone chan struct{}
	walkDone chan struct{}
}

func newFeeder(cfg feederConfig, holder *snapshotHolder) *feeder {
	if cfg.tick == 0 {
		cfg.tick = defaultTick
	}
	if cfg.walkEvery == 0 {
		cfg.walkEvery = defaultWalkEvery
	}
	if cfg.capBytes == 0 {
		cfg.capBytes = defaultCapMiB << 20
	}
	if cfg.newProducer == nil {
		cfg.newProducer = func(addr string) (producerConn, error) { return client.DialProducer(addr) }
	}
	return &feeder{
		cfg:      cfg,
		holder:   holder,
		stopCh:   make(chan struct{}),
		prodDone: make(chan struct{}),
		consDone: make(chan struct{}),
		walkDone: make(chan struct{}),
	}
}

// brokerConfig is the ONE place the loopback literal lives (D-SL7-1): no
// flag or env can rebind the broker (SHOW-3 by construction). The
// config-literal unit test asserts on this same value production uses.
func brokerConfig(dir string) broker.Config {
	return broker.Config{Addr: "127.0.0.1:0", DataDir: dir}
}

// start brings up broker → topic → producer → group consumer, seeds the
// live snapshot, and launches the three loops. Every boot MkdirTemps a
// NEW data dir — restart-fresh is the design (D-SL7-4).
func (f *feeder) start() error {
	dir, err := os.MkdirTemp(f.cfg.tmpRoot, "minikafka-showcase-")
	if err != nil {
		return fmt.Errorf("data dir: %w", err)
	}
	f.dir = dir

	srv, err := broker.New(brokerConfig(dir))
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	if err := srv.Start(); err != nil {
		return fmt.Errorf("broker listen: %w", err)
	}
	f.srv = srv
	addr := srv.Addr().String()

	admin, err := client.DialAdmin(addr)
	if err != nil {
		srv.Stop()
		return fmt.Errorf("admin dial: %w", err)
	}
	err = admin.CreateTopic(feedTopic, feedPartitions)
	admin.Close()
	if err != nil {
		srv.Stop()
		return fmt.Errorf("creating topic: %w", err)
	}

	prod, err := f.cfg.newProducer(addr)
	if err != nil {
		srv.Stop()
		return fmt.Errorf("producer dial: %w", err)
	}
	f.prod = prod

	gc, err := client.JoinGroup(addr, feedGroup, feedTopic)
	if err != nil {
		prod.Close()
		srv.Stop()
		return fmt.Errorf("joining group: %w", err)
	}
	f.gc = gc

	f.startedAt = time.Now()
	f.mu.Lock()
	f.state.status = statusLive
	f.state.assignment = gc.Assignment()
	f.publishLocked()
	f.mu.Unlock()

	f.started = true
	go f.producerLoop()
	go f.consumerLoop()
	go f.walkerLoop()
	return nil
}

// publishLocked rebuilds a FRESH immutable *snapshot from feederState and
// swaps it into the holder — called after EVERY mutation, under f.mu (§S).
// Every slice is freshly built; nothing stored is ever mutated afterward.
func (f *feeder) publishLocked() {
	parts := make([]partRow, 0, feedPartitions)
	for p := 0; p < feedPartitions; p++ {
		parts = append(parts, partRow{Partition: uint32(p), NextOffset: f.state.next[p]})
	}
	recent := make([]recentRow, 0, f.state.ringLen)
	start := (f.state.ringPos - f.state.ringLen + ringSize) % ringSize
	for i := 0; i < f.state.ringLen; i++ {
		recent = append(recent, f.state.ring[(start+i)%ringSize]) // oldest-first
	}
	assignment := make([]uint32, len(f.state.assignment))
	copy(assignment, f.state.assignment)
	f.holder.store(&snapshot{
		Status:        f.state.status,
		UptimeSeconds: int64(time.Since(f.startedAt) / time.Second),
		Produced:      f.state.produced,
		Partitions:    parts,
		Recent:        recent,
		Assignment:    assignment,
		DiskBytes:     f.state.diskBytes,
		DiskCapBytes:  f.cfg.capBytes,
		MemBytes:      f.state.memBytes,
		StartedAt:     f.startedAt.UTC().Format(time.RFC3339),
	})
}

// pushRecentLocked appends one consumed record to the ring of 50.
func (f *feeder) pushRecentLocked(r recentRow) {
	f.state.ring[f.state.ringPos] = r
	f.state.ringPos = (f.state.ringPos + 1) % ringSize
	if f.state.ringLen < ringSize {
		f.state.ringLen++
	}
}

// producerLoop ticks at cfg.tick, producing msg-<n> round-robin across the
// four partitions; produced counts acks only. While the sticky pause is
// set it skips ticks — the producer never un-pauses (ledger 8).
func (f *feeder) producerLoop() {
	defer close(f.prodDone)
	ticker := time.NewTicker(f.cfg.tick)
	defer ticker.Stop()
	var n uint64
	for {
		select {
		case <-f.stopCh:
			return
		case <-ticker.C:
		}
		f.mu.Lock()
		paused := f.state.paused
		f.mu.Unlock()
		if paused {
			continue
		}
		if _, err := f.prod.Produce(feedTopic, uint32(n%feedPartitions), []byte(fmt.Sprintf("msg-%d", n))); err != nil {
			// Transient produce errors skip the tick; a stop exits via
			// stopCh on the next select.
			continue
		}
		f.mu.Lock()
		f.state.produced++
		f.publishLocked()
		f.mu.Unlock()
		n++
	}
}

// consumerLoop polls with a short maxWait (fast shutdown, §F), feeds the
// ring, advances the consumed-offset frontiers, commits per non-empty
// batch, and refreshes the assignment. Any post-stop error is a clean
// exit — gc.Close() unblocking a parked Poll is the §F shutdown path.
func (f *feeder) consumerLoop() {
	defer close(f.consDone)
	for {
		select {
		case <-f.stopCh:
			return
		default:
		}
		recs, err := f.gc.Poll(1 * time.Second)
		if err != nil {
			select {
			case <-f.stopCh:
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		if len(recs) > 0 {
			f.mu.Lock()
			for _, r := range recs {
				f.pushRecentLocked(recentRow{Partition: r.Partition, Offset: r.Offset, Payload: string(r.Payload)})
				if next := r.Offset + 1; r.Partition < feedPartitions && next > f.state.next[r.Partition] {
					f.state.next[r.Partition] = next
				}
			}
			f.publishLocked()
			f.mu.Unlock()
			// A commit error surfaces through the next Poll's rejoin
			// machinery — never a crash here.
			_ = f.gc.Commit()
		}
		a := f.gc.Assignment()
		f.mu.Lock()
		f.state.assignment = a
		f.publishLocked()
		f.mu.Unlock()
	}
}

// walkerLoop walks the feeder's OWN data dir every cfg.walkEvery, summing
// file sizes, and reads HeapAlloc — the honest observable for the real
// long-run bound (D-SL7-4).
func (f *feeder) walkerLoop() {
	defer close(f.walkDone)
	ticker := time.NewTicker(f.cfg.walkEvery)
	defer ticker.Stop()
	for {
		select {
		case <-f.stopCh:
			return
		case <-ticker.C:
		}
		disk := f.walkDiskBytes()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		f.mu.Lock()
		f.state.diskBytes = disk
		f.state.memBytes = ms.HeapAlloc
		f.publishLocked()
		f.mu.Unlock()
	}
}

// walkDiskBytes sums regular-file sizes under the data dir; a mid-walk
// disappearance is skipped, never a crash — sizes catch up next walk.
func (f *feeder) walkDiskBytes() int64 {
	var total int64
	_ = filepath.WalkDir(f.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.Type().IsRegular() {
			if info, ierr := d.Info(); ierr == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

// stop is the §F leak-free shutdown, in load-bearing order: close the stop
// channel → gc.Close() (unblocks a Poll parked server-side — a naive join
// first would deadlock up to the maxWait) → producer Close → join all
// three goroutines → broker Stop last. Idempotent, so tests can both
// defer it and call it explicitly.
func (f *feeder) stop() {
	f.stopOnce.Do(func() {
		close(f.stopCh)
		if !f.started {
			if f.srv != nil {
				f.srv.Stop()
			}
			return
		}
		f.gc.Close()
		f.prod.Close()
		<-f.prodDone
		<-f.consDone
		<-f.walkDone
		f.srv.Stop()
	})
}
