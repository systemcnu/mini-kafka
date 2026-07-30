// -render-readme: the pure report→section function and the separate README
// marker splice (D-SL5-4). The section carries the headline table with
// error/duplicate counts inline, the FULL label block, every caveat, and
// the source report path — all from the Report, nothing added here.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// The marker-delimited README section the renderer owns (D-SL5-4).
const (
	markerBegin = "<!-- bench:begin -->"
	markerEnd   = "<!-- bench:end -->"
)

// render is D-SL5-4's pure function: the whole marker-delimited section
// from one Report — fixed layout, strconv formatting only (never %v), so
// identical input is byte-identical output on any platform. The report
// path is derived from the report's own fields, repo-root-relative (F9).
func render(r Report) string {
	f1 := func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
	f2 := func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
	var b strings.Builder
	b.WriteString(markerBegin + "\n")
	b.WriteString("## Benchmarks — " + r.Title + "\n\n")
	b.WriteString("Machine-rendered from the committed report by `go run ./cmd/bench -render-readme <report>`;\n")
	b.WriteString("a repo test re-renders and byte-compares, so a hand-edited number is a build failure.\n\n")
	b.WriteString("| iteration | msgs/s | MB/s | ack p50 ms | ack p99 ms | e2e p50 ms | e2e p99 ms | e2e samples | produce errors | duplicates |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for i, it := range r.Iterations {
		b.WriteString("| " + strconv.Itoa(i+1) + " | " + f1(it.MsgsPerSec) + " | " + f2(it.MBPerSec) +
			" | " + f2(it.AckP50Ms) + " | " + f2(it.AckP99Ms) +
			" | " + f2(it.E2eP50Ms) + " | " + f2(it.E2eP99Ms) +
			" | " + strconv.Itoa(it.E2eSamples) + " | " + strconv.Itoa(it.ProduceErrors) +
			" | " + strconv.Itoa(it.Duplicates) + " |\n")
	}
	b.WriteString("\nSpread across iterations (min / max / mean):\n\n")
	b.WriteString("- msgs/s: " + f1(r.Spread.MsgsPerSec.Min) + " / " + f1(r.Spread.MsgsPerSec.Max) + " / " + f1(r.Spread.MsgsPerSec.Mean) + "\n")
	b.WriteString("- ack p99 ms: " + f2(r.Spread.AckP99Ms.Min) + " / " + f2(r.Spread.AckP99Ms.Max) + " / " + f2(r.Spread.AckP99Ms.Mean) + "\n")
	b.WriteString("- e2e p99 ms: " + f2(r.Spread.E2eP99Ms.Min) + " / " + f2(r.Spread.E2eP99Ms.Max) + " / " + f2(r.Spread.E2eP99Ms.Mean) + "\n")
	b.WriteString("\nSetup:\n\n")
	b.WriteString("- hardware: " + r.Hardware + "\n")
	b.WriteString("- OS/arch: " + r.OS + "/" + r.Arch + "\n")
	b.WriteString("- Go: " + r.GoVersion + "\n")
	b.WriteString("- GOMAXPROCS: " + strconv.Itoa(r.GOMAXPROCS) + "\n")
	b.WriteString("- commit: " + r.Commit + "\n")
	b.WriteString("- storage: " + r.Storage + "\n")
	b.WriteString("- fsync mode: " + r.FsyncMode + "\n")
	b.WriteString("- group-commit window: " + strconv.Itoa(r.GroupCommitWindowMs) + " ms\n")
	b.WriteString("- load model: " + r.LoadModel + "\n")
	b.WriteString("- message size: " + strconv.Itoa(r.MessageSize) + " bytes\n")
	b.WriteString("- partitions: " + strconv.Itoa(r.Partitions) + "\n")
	b.WriteString("- run: " + strconv.Itoa(len(r.Iterations)) + " iterations × " + f1(r.RunSeconds) + " s\n")
	b.WriteString("- warm-up: " + f1(r.WarmupSeconds) + " s (measured, discarded)\n")
	b.WriteString("- percentile method: " + r.PercentileMethod + "\n")
	b.WriteString("\nCaveats:\n\n")
	for _, c := range r.Caveats {
		b.WriteString("- " + c + "\n")
	}
	b.WriteString("\nSource report: `benchmarks/reports/" + reportFileName(r) + "`\n")
	b.WriteString(markerEnd)
	return b.String()
}

// spliceReadme replaces the marker-delimited section with the freshly
// rendered one (which carries its own markers), or appends it at the
// pinned anchor — EOF after one blank line — on the first render.
func spliceReadme(readme []byte, section string) ([]byte, error) {
	s := string(readme)
	begin := strings.Index(s, markerBegin)
	end := strings.Index(s, markerEnd)
	switch {
	case begin < 0 && end < 0:
		if !strings.HasSuffix(s, "\n") {
			s += "\n"
		}
		return []byte(s + "\n" + section + "\n"), nil
	case begin < 0 || end < begin:
		return nil, fmt.Errorf("bench markers malformed in README (begin at %d, end at %d)", begin, end)
	}
	return []byte(s[:begin] + section + s[end+len(markerEnd):]), nil
}

// renderMode is the -render-readme path: load the named report, splice its
// rendered section into the README. The only writer of the block.
func renderMode(reportPath, readmePath string) error {
	data, err := os.ReadFile(reportPath)
	if err != nil {
		return err
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		return fmt.Errorf("parsing %s: %w", reportPath, err)
	}
	readme, err := os.ReadFile(readmePath)
	if err != nil {
		return err
	}
	out, err := spliceReadme(readme, render(r))
	if err != nil {
		return err
	}
	if err := os.WriteFile(readmePath, out, 0o644); err != nil {
		return err
	}
	fmt.Printf("bench: README section rendered from %s into %s\n", reportPath, readmePath)
	return nil
}
