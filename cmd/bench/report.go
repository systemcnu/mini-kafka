// The Report struct — the BENCH-2 label checklist in one schema (D-SL5-2) —
// plus percentile/spread math and JSON writing. The report is the single
// source of truth: the README bench section is a pure render of it.
package main

// Report is one benchmark run's machine-written record. Every honesty
// label BENCH-2 demands lives here (D-SL5-2); nothing is added at render
// time. Zero-legal counters (errors, duplicates, GC deltas) carry no
// omitempty — presence in the JSON text is part of the contract (F2/CF3).
type Report struct {
	Title               string      `json:"title"`                  // "closed-loop response latency" (DD-22)
	Commit              string      `json:"commit"`                 // short vcs.revision, "-dirty" when the tree was modified (D-SL5-8)
	Timestamp           string      `json:"timestamp"`              // RFC3339 UTC; its date prefix names the report file
	Hardware            string      `json:"hardware"`               // operator-stated via -hardware (G-SL5-1)
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
	E2eSamples      int     `json:"e2e_samples"` // shrinks honestly when the consumer lags (G-SL5-3)
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
