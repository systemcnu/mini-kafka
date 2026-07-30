// Black-box smoke for the bench (SL5 §4): TestMain builds the real binary
// once (demo precedent) — plus a -buildvcs=false twin for the D-SL5-8
// refusal leg — and every test execs a binary, never run() in-process
// (package main only for Report/reportFileName reuse). Report fields are
// asserted in TWO classes (F2/CF3): must-be-positive on the unmarshaled
// struct vs present-may-be-zero on the raw JSON text — an unmarshaled zero
// and a missing key are indistinguishable.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var benchBin, benchBinNoVCS string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "bench-build-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	benchBin = filepath.Join(dir, "bench")
	benchBinNoVCS = filepath.Join(dir, "bench-novcs")
	// The VCS-refusal subtest needs a binary WITHOUT vcs info (plan
	// pitfall): -buildvcs=false stages exactly what plain `go run` ships.
	builds := [][]string{
		{"build", "-o", benchBin, "."},
		{"build", "-buildvcs=false", "-o", benchBinNoVCS, "."},
	}
	for _, args := range builds {
		build := exec.Command("go", args...)
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "building bench binary (%v): %v\n", args, err)
			os.RemoveAll(dir)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// runBench execs bin with the short seams plus extra args and returns
// combined output and the error.
func runBench(t *testing.T, bin string, extra ...string) (string, error) {
	t.Helper()
	args := append([]string{"-iters", "2", "-duration", "1s", "-warmup", "200ms", "-c", "2"}, extra...)
	var out bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

// TestBenchSmoke runs the short-seams benchmark end to end and asserts the
// report against the §4 checklist: exit 0, filename shape, both assertion
// classes, healthy-run sanity, spread, and the pinned caveat texts.
func TestBenchSmoke(t *testing.T) {
	outDir := t.TempDir()
	out, err := runBench(t, benchBin, "-hardware", "test rig", "-out", outDir)
	if err != nil {
		t.Fatalf("bench failed: %v\noutput:\n%s", err, out)
	}
	reports, err := filepath.Glob(filepath.Join(outDir, "*.json"))
	if err != nil || len(reports) != 1 {
		t.Fatalf("want exactly one report in -out, got %v (err %v)\noutput:\n%s", reports, err, out)
	}
	raw, err := os.ReadFile(reports[0])
	if err != nil {
		t.Fatal(err)
	}
	var r Report
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("report does not unmarshal: %v", err)
	}

	// Filename: <utc-date>-<commit>.json, commit possibly -dirty on a dev
	// tree (D-SL5-2/8), and derived from the report's own fields.
	name := filepath.Base(reports[0])
	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}-[0-9a-f]{12}(-dirty)?\.json$`).MatchString(name) {
		t.Errorf("report filename %q, want <utc-date>-<commit>.json (D-SL5-2/8)", name)
	}
	if want := reportFileName(r); name != want {
		t.Errorf("filename %q does not derive from the report's own timestamp+commit (%q)", name, want)
	}

	// Class 1: must-be-positive fields and exact labels.
	if r.Title != "closed-loop response latency" {
		t.Errorf("title = %q, want the DD-22 title", r.Title)
	}
	if r.Hardware != "test rig" {
		t.Errorf("hardware = %q, want the -hardware value", r.Hardware)
	}
	labels := map[string]string{
		"commit": r.Commit, "timestamp": r.Timestamp, "os": r.OS, "arch": r.Arch,
		"go_version": r.GoVersion, "storage": r.Storage, "fsync_mode": r.FsyncMode,
		"load_model": r.LoadModel, "percentile_method": r.PercentileMethod,
	}
	for k, v := range labels {
		if v == "" {
			t.Errorf("label %s empty, want non-empty (BENCH-2)", k)
		}
	}
	if r.GOMAXPROCS <= 0 {
		t.Errorf("gomaxprocs = %d, want > 0", r.GOMAXPROCS)
	}
	if r.GroupCommitWindowMs != 5 {
		t.Errorf("group_commit_window_ms = %d, want the broker's 5 (F4/CF2)", r.GroupCommitWindowMs)
	}
	if r.MessageSize != 1024 {
		t.Errorf("message_size = %d, want 1024", r.MessageSize)
	}
	if r.Partitions != 4 {
		t.Errorf("partitions = %d, want 4", r.Partitions)
	}
	if r.WarmupSeconds <= 0 || r.RunSeconds <= 0 {
		t.Errorf("warmup_seconds/run_seconds = %v/%v, want both > 0", r.WarmupSeconds, r.RunSeconds)
	}
	if len(r.Iterations) != 2 {
		t.Errorf("iterations = %d rows, want 2 (-iters 2)", len(r.Iterations))
	}
	for i, it := range r.Iterations {
		pos := map[string]float64{
			"duration_seconds": it.DurationSeconds, "msgs_acked": float64(it.MsgsAcked),
			"msgs_per_sec": it.MsgsPerSec, "mb_per_sec": it.MBPerSec,
			"ack_p50_ms": it.AckP50Ms, "ack_p99_ms": it.AckP99Ms,
			"e2e_p50_ms": it.E2eP50Ms, "e2e_p99_ms": it.E2eP99Ms,
			"e2e_samples": float64(it.E2eSamples),
		}
		for k, v := range pos {
			if v <= 0 {
				t.Errorf("iteration %d: %s = %v, want > 0", i+1, k, v)
			}
		}
		if it.AckP50Ms > it.AckP99Ms {
			t.Errorf("iteration %d: ack p50 %v > p99 %v", i+1, it.AckP50Ms, it.AckP99Ms)
		}
		if it.E2eP50Ms > it.E2eP99Ms {
			t.Errorf("iteration %d: e2e p50 %v > p99 %v", i+1, it.E2eP50Ms, it.E2eP99Ms)
		}
		// Healthy-run sanity (§4): a loopback run has no errors and no
		// redeliveries.
		if it.ProduceErrors != 0 || it.Duplicates != 0 {
			t.Errorf("iteration %d: errors=%d duplicates=%d, want 0/0 on a healthy run", i+1, it.ProduceErrors, it.Duplicates)
		}
	}
	// Spread block present and sane.
	for k, s := range map[string]MinMaxMean{
		"msgs_per_sec": r.Spread.MsgsPerSec, "ack_p99_ms": r.Spread.AckP99Ms, "e2e_p99_ms": r.Spread.E2eP99Ms,
	} {
		if s.Mean <= 0 {
			t.Errorf("spread %s mean = %v, want > 0", k, s.Mean)
		}
		if s.Min > s.Max {
			t.Errorf("spread %s min %v > max %v", k, s.Min, s.Max)
		}
	}

	// Class 2: present-may-be-zero via RAW-JSON key presence (F2/CF3).
	for _, key := range []string{`"produce_errors"`, `"duplicates"`, `"gc_pause_delta_ms"`, `"gc_count_delta"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("raw report lacks key %s — zero-legal fields must be PRESENT (F2/CF3)", key)
		}
	}

	// Caveats carry the pinned texts (D-SL5-2).
	for _, want := range []string{
		"understates the queueing tails",
		"5 ms group-commit window",
		"may not flush the drive cache",
		"no capacity claims",
	} {
		found := false
		for _, c := range r.Caveats {
			if strings.Contains(c, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no caveat contains %q (D-SL5-2)", want)
		}
	}
}

// TestHardwareRefusal: no -hardware → distinct refusal naming the flag,
// non-zero exit, no report written (D-SL5-3).
func TestHardwareRefusal(t *testing.T) {
	outDir := t.TempDir()
	out, err := runBench(t, benchBin, "-out", outDir)
	if err == nil {
		t.Fatalf("bench ran without -hardware, want refusal (D-SL5-3)\noutput:\n%s", out)
	}
	if !strings.Contains(out, "-hardware") {
		t.Fatalf("refusal does not name -hardware:\n%s", out)
	}
	if reports, _ := filepath.Glob(filepath.Join(outDir, "*.json")); len(reports) != 0 {
		t.Fatalf("refusal still wrote a report: %v", reports)
	}
}

// TestNoVCSRefusal: a binary without vcs.revision (what plain `go run`
// ships) must refuse, naming -buildvcs=true, and write nothing (D-SL5-8).
func TestNoVCSRefusal(t *testing.T) {
	outDir := t.TempDir()
	out, err := runBench(t, benchBinNoVCS, "-hardware", "test rig", "-out", outDir)
	if err == nil {
		t.Fatalf("VCS-less binary emitted a report, want refusal (D-SL5-8)\noutput:\n%s", out)
	}
	if !strings.Contains(out, "-buildvcs=true") {
		t.Fatalf("refusal must name the fix -buildvcs=true (D-SL5-8):\n%s", out)
	}
	if reports, _ := filepath.Glob(filepath.Join(outDir, "*.json")); len(reports) != 0 {
		t.Fatalf("refusal still wrote a report: %v", reports)
	}
}
