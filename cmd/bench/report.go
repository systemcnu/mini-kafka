// The Report struct — the BENCH-2 label checklist in one schema (D-SL5-2) —
// plus percentile/spread math and JSON writing. The report is the single
// source of truth: the README bench section is a pure render of it.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

// Pinned label texts (D-SL5-2).
const (
	reportTitle           = "closed-loop response latency" // DD-22's title
	fsyncModeLabel        = "fsync (DD-7: plain File.Sync; macOS fsync may not flush the drive cache)"
	percentileMethodLabel = "nearest-rank on sorted samples"
)

// The caveats block (D-SL5-2): closed-loop tails, the batching window,
// the fsync platform limit, no capacity claims.
var reportCaveats = []string{
	"closed-loop load understates the queueing tails an open-loop arrival process would show",
	"ack latency includes the broker's 5 ms group-commit window",
	"fsync durability is platform-qualified: macOS fsync may not flush the drive cache (DD-7)",
	"no capacity claims: fixed closed-loop response numbers, not a maximum",
}

// The method note (D-SL5-1's stated measurement choices).
var reportMethodNotes = []string{
	"e2e latency is wall-clock, same process both ends; a clock step mid-run lands in the numbers (G-SL5-2)",
	"the consumer commits once per iteration boundary, outside the sampled path",
	"the warm-up bucket is measured and discarded",
	"e2e is sampled only for records received in-window; consumer lag inflates latency honestly and shows in e2e_samples (G-SL5-3)",
}

// Report is one benchmark run's machine-written record. Every honesty
// label BENCH-2 demands lives here (D-SL5-2); nothing is added at render
// time. Zero-legal counters (errors, duplicates, GC deltas) carry no
// omitempty — presence in the JSON text is part of the contract (F2/CF3).
type Report struct {
	Title               string      `json:"title"`     // "closed-loop response latency" (DD-22)
	Commit              string      `json:"commit"`    // short vcs.revision, "-dirty" when the tree was modified (D-SL5-8)
	Timestamp           string      `json:"timestamp"` // RFC3339 UTC; its date prefix names the report file
	Hardware            string      `json:"hardware"`  // operator-stated via -hardware (G-SL5-1)
	OS                  string      `json:"os"`
	Arch                string      `json:"arch"`
	GoVersion           string      `json:"go_version"`
	GOMAXPROCS          int         `json:"gomaxprocs"`
	Storage             string      `json:"storage"`                // operator-stated via -storage; the default says "(unverified)"
	FsyncMode           string      `json:"fsync_mode"`             // "fsync" + the DD-7 platform caveat
	GroupCommitWindowMs int         `json:"group_commit_window_ms"` // broker flusher window — ack latency floors here (F4/CF2)
	LoadModel           string      `json:"load_model"`
	MessageSize         int         `json:"message_size"` // payload bytes (1024), not the frame
	Partitions          int         `json:"partitions"`
	WarmupSeconds       float64     `json:"warmup_seconds"` // measured and discarded
	RunSeconds          float64     `json:"run_seconds"`    // configured per-iteration duration
	PercentileMethod    string      `json:"percentile_method"`
	MethodNotes         []string    `json:"method_notes"` // D-SL5-1's stated measurement choices
	Iterations          []Iteration `json:"iterations"`
	Spread              Spread      `json:"spread"`
	Caveats             []string    `json:"caveats"`
}

// Iteration is one measured iteration's row (the warm-up bucket never
// becomes a row — measured and discarded, D-SL5-1).
type Iteration struct {
	DurationSeconds float64 `json:"duration_seconds"` // measured wall time
	MsgsAcked       int     `json:"msgs_acked"`
	MsgsPerSec      float64 `json:"msgs_per_sec"`
	MBPerSec        float64 `json:"mb_per_sec"`
	AckP50Ms        float64 `json:"ack_p50_ms"`
	AckP99Ms        float64 `json:"ack_p99_ms"`
	E2eP50Ms        float64 `json:"e2e_p50_ms"`
	E2eP99Ms        float64 `json:"e2e_p99_ms"`
	E2eSamples      int     `json:"e2e_samples"`       // shrinks honestly when the consumer lags (G-SL5-3)
	GCPauseDeltaMs  float64 `json:"gc_pause_delta_ms"` // boundary-snapshot delta
	GCCountDelta    int     `json:"gc_count_delta"`
	ProduceErrors   int     `json:"produce_errors"`
	Duplicates      int     `json:"duplicates"` // re-deliveries: counted, never sampled (F7)
}

// Spread is the cross-iteration min/max/mean block (D-SL5-2).
type Spread struct {
	MsgsPerSec MinMaxMean `json:"msgs_per_sec"`
	AckP99Ms   MinMaxMean `json:"ack_p99_ms"`
	E2eP99Ms   MinMaxMean `json:"e2e_p99_ms"`
}

// MinMaxMean is one spread row.
type MinMaxMean struct {
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

// reportFileName derives <utc-date>-<commit>.json from the report's own
// fields (D-SL5-2), so the written file and the README's named path can
// never drift.
func reportFileName(r Report) string {
	date := r.Timestamp
	if len(date) > 10 {
		date = date[:10]
	}
	return date + "-" + r.Commit + ".json"
}

// nearestRank is the pinned percentile method (D-SL5-1): the value at rank
// ceil(p/100·N) of the full sorted sample.
func nearestRank(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}

func minMaxMean(vals []float64) MinMaxMean {
	m := MinMaxMean{Min: vals[0], Max: vals[0]}
	var sum float64
	for _, v := range vals {
		m.Min = math.Min(m.Min, v)
		m.Max = math.Max(m.Max, v)
		sum += v
	}
	m.Mean = sum / float64(len(vals))
	return m
}

// buildReport assembles the D-SL5-2 schema from the raw per-bucket
// tallies. Bucket 0 (warm-up) never becomes a row.
func buildReport(cfg benchConfig, commit string, now time.Time, stats []*producerStats, e2eMs [][]float64, dups []int, bounds []boundarySnap) Report {
	rows := make([]Iteration, 0, cfg.iters)
	var rates, ackP99s, e2eP99s []float64
	for k := 1; k <= cfg.iters; k++ {
		var acked, errs int
		var ack []float64
		for _, st := range stats {
			acked += st.acked[k]
			errs += st.errors[k]
			ack = append(ack, st.ackMs[k]...)
		}
		sort.Float64s(ack)
		e2e := append([]float64(nil), e2eMs[k]...)
		sort.Float64s(e2e)
		secs := bounds[k].t.Sub(bounds[k-1].t).Seconds()
		row := Iteration{
			DurationSeconds: secs,
			MsgsAcked:       acked,
			MsgsPerSec:      float64(acked) / secs,
			MBPerSec:        float64(acked) * msgSize / secs / 1e6,
			AckP50Ms:        nearestRank(ack, 50),
			AckP99Ms:        nearestRank(ack, 99),
			E2eP50Ms:        nearestRank(e2e, 50),
			E2eP99Ms:        nearestRank(e2e, 99),
			E2eSamples:      len(e2e),
			GCPauseDeltaMs:  float64(bounds[k].pauseNs-bounds[k-1].pauseNs) / 1e6,
			GCCountDelta:    int(bounds[k].numGC - bounds[k-1].numGC),
			ProduceErrors:   errs,
			Duplicates:      dups[k],
		}
		rows = append(rows, row)
		rates = append(rates, row.MsgsPerSec)
		ackP99s = append(ackP99s, row.AckP99Ms)
		e2eP99s = append(e2eP99s, row.E2eP99Ms)
	}
	return Report{
		Title:               reportTitle,
		Commit:              commit,
		Timestamp:           now.Format(time.RFC3339),
		Hardware:            cfg.hardware,
		OS:                  runtime.GOOS,
		Arch:                runtime.GOARCH,
		GoVersion:           runtime.Version(),
		GOMAXPROCS:          runtime.GOMAXPROCS(0),
		Storage:             cfg.storage,
		FsyncMode:           fsyncModeLabel,
		GroupCommitWindowMs: groupCommitWindowMs,
		LoadModel:           fmt.Sprintf("closed-loop, C=%d sync producers, in-flight 1/conn", cfg.c),
		MessageSize:         msgSize,
		Partitions:          partitions,
		WarmupSeconds:       cfg.warmup.Seconds(),
		RunSeconds:          cfg.duration.Seconds(),
		PercentileMethod:    percentileMethodLabel,
		MethodNotes:         reportMethodNotes,
		Caveats:             reportCaveats,
		Iterations:          rows,
		Spread: Spread{
			MsgsPerSec: minMaxMean(rates),
			AckP99Ms:   minMaxMean(ackP99s),
			E2eP99Ms:   minMaxMean(e2eP99s),
		},
	}
}

// writeReport marshals r into dir/<utc-date>-<commit>.json.
func writeReport(dir string, r Report) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, reportFileName(r))
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return "", err
	}
	return path, nil
}
