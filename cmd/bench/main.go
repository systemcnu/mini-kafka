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
	"runtime/debug"
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

	// SKELETON (PLAN row 1, rows 3-4 pending): the harness and real report
	// land at their build rows; the stub writes an empty-but-well-formed
	// report so the smoke's field assertions stay RED.
	cfg := benchConfig{hardware: *hardware, storage: *storage, iters: *iters,
		duration: *duration, warmup: *warmup, c: *c, out: *out}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		fatalf("%v", err)
	}
	date := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(cfg.out, date+"-"+commit+".json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
		fatalf("%v", err)
	}
	fmt.Println("bench: report written to", path)
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
