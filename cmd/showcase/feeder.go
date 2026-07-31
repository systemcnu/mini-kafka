package main

import (
	"time"

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
// *client.Producer, and by the failing fake the WRITE_FAILED test injects.
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

// feeder hosts the in-process broker and the producer/consumer/walker
// goroutines (D-SL7-1, PLAN §F). SKELETON (PLAN row 1): start is a no-op —
// rows 4–5 land the wiring and the disk bound.
type feeder struct {
	cfg    feederConfig
	holder *snapshotHolder
	dir    string // this boot's fresh MkdirTemp data dir
}

func newFeeder(cfg feederConfig, holder *snapshotHolder) *feeder {
	return &feeder{cfg: cfg, holder: holder}
}

// brokerConfig is the ONE place the loopback literal lives (D-SL7-1) —
// asserted by the config-literal unit test on the same value production
// uses. SKELETON (PLAN row 1): literal missing — row 4 lands it.
func brokerConfig(dir string) broker.Config {
	return broker.Config{DataDir: dir}
}

// start brings up broker → topic → producer → consumer → walker.
// SKELETON (PLAN row 1): no-op.
func (f *feeder) start() error { return nil }

// stop is the §F leak-free shutdown. SKELETON (PLAN row 1): no-op.
func (f *feeder) stop() {}
