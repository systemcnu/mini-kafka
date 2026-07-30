// Command bench is DD-22 realized (D-SL5-1): a closed-loop benchmark —
// broker in-process on loopback, C sync producers through the shipped
// client, one group consumer for end-to-end latency (one process = one
// clock) — emitting the D-SL5-2 report the README section is rendered
// from (-render-readme). It refuses to run without an operator-stated
// -hardware label or without VCS provenance (D-SL5-8).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	topicName  = "bench"
	partitions = 4
	msgSize    = 1024 // payload bytes — the message-size label (the payload the broker caps, not the frame)
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

	// SKELETON (PLAN row 1): flags + report shape only — no refusals, no
	// provenance, no harness. The smoke's two assertion classes are seen
	// RED against this empty-but-well-formed report before any row lands.
	cfg := benchConfig{hardware: *hardware, storage: *storage, iters: *iters,
		duration: *duration, warmup: *warmup, c: *c, out: *out}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		fatalf("%v", err)
	}
	date := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(cfg.out, date+"-"+".json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
		fatalf("%v", err)
	}
	fmt.Println("bench: report written to", path)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "bench: "+format+"\n", args...)
	os.Exit(1)
}
