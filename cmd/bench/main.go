// Command bench is DD-22 realized (D-SL5-1): a closed-loop benchmark —
// broker in-process on loopback, C sync producers through the shipped
// client, one group consumer for end-to-end latency (one process = one
// clock) — emitting the D-SL5-2 report the README section is rendered
// from (-render-readme). It refuses to run without an operator-stated
// -hardware label or without VCS provenance (D-SL5-8).
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/systemcnu/mini-kafka/client"
	"github.com/systemcnu/mini-kafka/internal/broker"
)

const (
	topicName  = "bench"
	groupName  = "bench"
	partitions = 4
	msgSize    = 1024 // payload bytes — the message-size label (the payload the broker caps, not the frame)

	// groupCommitWindowMs mirrors the broker flusher's window
	// (internal/storage flushWindow) — the BENCH-2 batching label; ack
	// latency floors here (D-SL5-2/F4).
	groupCommitWindowMs = 5

	// pollWait is short so iteration boundaries are never held hostage by
	// a parked fetch (plan pitfall).
	pollWait = 250 * time.Millisecond
)

// benchConfig carries the D-SL5-3 flag set into the run.
type benchConfig struct {
	hardware, storage string
	iters             int
	duration, warmup  time.Duration
	c                 int
	out               string
}

func main() {
	hardware := flag.String("hardware", "", "operator-stated hardware label — REQUIRED for a benchmark run (D-SL5-3)")
	storage := flag.String("storage", "local SSD (unverified)", "operator-stated storage label")
	iters := flag.Int("iters", 3, "measured iterations (test seam)")
	duration := flag.Duration("duration", 10*time.Second, "per-iteration duration (test seam)")
	warmup := flag.Duration("warmup", 2*time.Second, "warm-up before iteration 1, measured and discarded (test seam)")
	c := flag.Int("c", 8, "closed-loop sync producer count (test seam)")
	out := flag.String("out", "benchmarks/reports", "report directory (run from the repo root)")
	renderReadme := flag.String("render-readme", "", "render the README bench section from this report file instead of benchmarking")
	readme := flag.String("readme", "README.md", "README to splice under -render-readme; the default assumes cwd = repo root")
	flag.Parse()

	if *renderReadme != "" {
		_ = *readme
		return // SKELETON: the renderer lands at its build row (D-SL5-4)
	}

	// A report-emitting run refuses to start unlabeled or untraceable:
	// hardware is operator-stated (D-SL5-3), provenance enforced (D-SL5-8).
	if *hardware == "" {
		fatalf("-hardware is required: a report must state the machine it ran on (D-SL5-3); refusing to run")
	}
	commit, err := vcsCommit()
	if err != nil {
		fatalf("%v", err)
	}
	if *iters < 1 || *c < 1 {
		fatalf("-iters and -c must be >= 1")
	}

	cfg := benchConfig{hardware: *hardware, storage: *storage, iters: *iters,
		duration: *duration, warmup: *warmup, c: *c, out: *out}
	path, err := run(cfg, commit)
	if err != nil {
		fatalf("%v", err)
	}
	fmt.Println("bench: report written to", path)
}

// boundarySnap is one iteration-boundary snapshot. ReadMemStats is
// stop-the-world, so it runs at boundaries ONLY (plan pitfall); GC rows
// are deltas between consecutive snapshots.
type boundarySnap struct {
	t       time.Time
	pauseNs uint64
	numGC   uint32
}

func snap() boundarySnap {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return boundarySnap{t: time.Now(), pauseNs: m.PauseTotalNs, numGC: m.NumGC}
}

// producerStats is one producer's per-bucket tallies — written only by its
// own goroutine, read after the join (bucket 0 = warm-up, discarded).
type producerStats struct {
	ackMs  [][]float64
	acked  []int
	errors []int
}

// run executes one broker lifetime (D-SL5-1): warm-up then cfg.iters
// measured iterations under a closed loop of C sync producers (producer i
// → partition i mod 4) and one group consumer, then writes the report and
// returns its path.
func run(cfg benchConfig, commit string) (string, error) {
	dataDir, err := os.MkdirTemp("", "minikafka-bench-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dataDir)
	srv, err := broker.New(broker.Config{Addr: "127.0.0.1:0", DataDir: dataDir})
	if err != nil {
		return "", err
	}
	if err := srv.Start(); err != nil {
		return "", err
	}
	defer srv.Stop()
	addr := srv.Addr().String()

	admin, err := client.DialAdmin(addr)
	if err != nil {
		return "", err
	}
	if err := admin.CreateTopic(topicName, partitions); err != nil {
		return "", err
	}
	admin.Close()

	// The consumer joins before any record flows, so e2e coverage starts
	// at offset 0 of every partition.
	gc, err := client.JoinGroup(addr, groupName, topicName)
	if err != nil {
		return "", err
	}

	prods := make([]*client.Producer, cfg.c)
	for i := range prods {
		if prods[i], err = client.DialProducer(addr); err != nil {
			return "", err
		}
	}
	defer func() {
		for _, p := range prods {
			p.Close()
		}
	}()

	// The iteration index, published atomically (D-SL5-1/F8): 0 = warm-up
	// (measured, discarded), 1..iters measured, stopIdx = stopped — a
	// sample completing then belongs to no bucket and is dropped.
	var iterIdx atomic.Int64
	stopIdx := int64(cfg.iters + 1)

	// Producers: sync Produce in-flight 1/conn IS the closed loop. Payload
	// = (producerID u64, seq u64, sendUnixNano u64) + padding to 1 KiB;
	// ack samples bucket at ack-COMPLETION read.
	var wg sync.WaitGroup
	stats := make([]*producerStats, cfg.c)
	for i := 0; i < cfg.c; i++ {
		st := &producerStats{
			ackMs:  make([][]float64, cfg.iters+1),
			acked:  make([]int, cfg.iters+1),
			errors: make([]int, cfg.iters+1),
		}
		stats[i] = st
		wg.Add(1)
		go func(id int, prod *client.Producer, st *producerStats) {
			defer wg.Done()
			payload := make([]byte, msgSize)
			binary.BigEndian.PutUint64(payload[0:8], uint64(id))
			part := uint32(id % partitions)
			var seq uint64
			for iterIdx.Load() < stopIdx {
				binary.BigEndian.PutUint64(payload[8:16], seq)
				seq++
				start := time.Now()
				binary.BigEndian.PutUint64(payload[16:24], uint64(start.UnixNano()))
				_, err := prod.Produce(topicName, part, payload)
				idx := iterIdx.Load()
				if idx >= stopIdx {
					return // post-final ack: no bucket — drop, don't panic
				}
				if err != nil {
					st.errors[idx]++
					continue
				}
				st.acked[idx]++
				st.ackMs[idx] = append(st.ackMs[idx], float64(time.Since(start))/1e6)
			}
		}(i, prods[i], st)
	}

	// One consumer goroutine: e2e = receipt wall clock − embedded send
	// time (same process, no skew; G-SL5-2 accepted), bucketed at the
	// RECEIPT read. Dedupe on (producerID, seq) BEFORE sampling — a
	// re-delivery carries its original send time and would poison p99, so
	// it only increments duplicates (D-SL5-2/F7). The map is never
	// cleared: redeliveries cross iteration boundaries by definition.
	consStop := make(chan struct{})
	consDone := make(chan struct{})
	var consErr error
	e2eMs := make([][]float64, cfg.iters+1)
	dups := make([]int, cfg.iters+1)
	go func() {
		defer close(consDone)
		seen := make(map[[2]uint64]struct{}, 1<<16)
		for {
			select {
			case <-consStop:
				return
			default:
			}
			recs, err := gc.Poll(pollWait)
			if err != nil {
				select {
				case <-consStop: // teardown racing a poll: not an error
				default:
					consErr = err
				}
				return
			}
			now := time.Now().UnixNano() // this batch's receipt time
			for _, r := range recs {
				idx := iterIdx.Load()
				if idx >= stopIdx || len(r.Payload) < 24 {
					continue
				}
				id := [2]uint64{binary.BigEndian.Uint64(r.Payload[0:8]), binary.BigEndian.Uint64(r.Payload[8:16])}
				if _, dup := seen[id]; dup {
					dups[idx]++
					continue
				}
				seen[id] = struct{}{}
				send := int64(binary.BigEndian.Uint64(r.Payload[16:24]))
				e2eMs[idx] = append(e2eMs[idx], float64(now-send)/1e6)
			}
		}
	}()

	// The iteration clock — the single owner of phase transitions: at each
	// boundary advance the index FIRST, then snapshot, then the
	// once-per-boundary Commit outside any sampled path (D-SL5-1/F6).
	now := time.Now().UTC()
	bounds := make([]boundarySnap, 0, cfg.iters+1)
	time.Sleep(cfg.warmup)
	iterIdx.Store(1)
	bounds = append(bounds, snap())
	if err := gc.Commit(); err != nil {
		return "", fmt.Errorf("commit at warm-up boundary: %w", err)
	}
	for k := 1; k <= cfg.iters; k++ {
		time.Sleep(cfg.duration)
		if k < cfg.iters {
			iterIdx.Store(int64(k + 1))
			bounds = append(bounds, snap())
			if err := gc.Commit(); err != nil {
				return "", fmt.Errorf("commit at boundary %d: %w", k, err)
			}
			continue
		}
		// Final boundary: producers signaled and JOINED before the
		// snapshot (plan pitfall — the closed loop must fully stop).
		iterIdx.Store(stopIdx)
		wg.Wait()
		bounds = append(bounds, snap())
	}
	close(consStop)
	<-consDone
	if consErr != nil {
		return "", fmt.Errorf("consumer: %w", consErr)
	}
	if err := gc.Commit(); err != nil {
		return "", fmt.Errorf("final commit: %w", err)
	}
	gc.Close()

	return writeReport(cfg.out, buildReport(cfg, commit, now, stats, e2eMs, dups, bounds))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "bench: "+format+"\n", args...)
	os.Exit(1)
}

// vcsCommit enforces D-SL5-8: refuse without a VCS revision in the build
// info — plain `go run` embeds none, and the report would carry an empty
// commit label under a <date>-.json filename. A modified tree stamps
// "-dirty" into the commit (and so the filename).
func vcsCommit() (string, error) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", fmt.Errorf("no build info in this binary: use `go run -buildvcs=true ./cmd/bench` or a built binary (D-SL5-8)")
	}
	var rev string
	var dirty bool
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return "", fmt.Errorf("no VCS revision in build info — plain `go run` embeds none; use `go run -buildvcs=true ./cmd/bench` or a built binary (D-SL5-8)")
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		rev += "-dirty"
	}
	return rev, nil
}
