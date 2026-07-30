// Render-gate tests (D-SL5-4). The real-README leg's bootstrap rule: while
// benchmarks/reports/*.json is EMPTY the leg SKIPS with a named reason (the
// pre-reference state — the builder ships it skipping); once ANY report is
// committed the leg demands markers in README.md, the section's named
// report on disk, a clean (never -dirty) commit stamp, and a byte-identical
// re-render. The skip condition is the report glob ONLY — reports present
// with the markers deleted is a FAILURE, not a skip.
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// loadFixture reads the committed testdata report.
func loadFixture(t *testing.T) Report {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatalf("fixture does not unmarshal: %v", err)
	}
	return r
}

// TestRenderFixture pins the section's content: markers, headline table
// with error/duplicate counts beside the numbers they qualify, spread, the
// FULL inline label set, every caveat, and the source report path (CF1).
func TestRenderFixture(t *testing.T) {
	r := loadFixture(t)
	got := render(r)
	if !strings.HasPrefix(got, markerBegin) || !strings.HasSuffix(got, markerEnd) {
		t.Errorf("section is not marker-delimited:\n%s", got)
	}
	for _, want := range []string{
		"## Benchmarks — closed-loop response latency",
		// headline table rows: msgs/s, MB/s, ack p50/p99, e2e p50/p99,
		// samples, then errors and duplicates inline (CF1)
		"| 1 | 1502.1 | 1.54 | 5.21 | 9.87 | 11.02 | 19.44 | 14980 | 7 | 3 |",
		"| 2 | 1533.2 | 1.57 | 5.18 | 10.11 | 10.88 | 21.07 | 15290 | 0 | 0 |",
		// spread
		"- msgs/s: 1502.1 / 1533.2 / 1517.6",
		"- ack p99 ms: 9.87 / 10.11 / 9.99",
		"- e2e p99 ms: 19.44 / 21.07 / 20.25",
		// the FULL label block (D-SL5-4)
		"- hardware: FixtureBook Pro M9, 64 GB RAM",
		"- OS/arch: darwin/arm64",
		"- Go: go1.24.0",
		"- GOMAXPROCS: 10",
		"- commit: abc123def456",
		"- storage: fixture NVMe (stated)",
		"- fsync mode: fsync (DD-7: plain File.Sync; macOS fsync may not flush the drive cache)",
		"- group-commit window: 5 ms",
		"- load model: closed-loop, C=8 sync producers, in-flight 1/conn",
		"- message size: 1024 bytes",
		"- partitions: 4",
		"- run: 2 iterations × 10.0 s",
		"- warm-up: 2.0 s (measured, discarded)",
		"- percentile method: nearest-rank on sorted samples",
		// both caveat blocks' texts, verbatim from the report
		"- closed-loop load understates the queueing tails an open-loop arrival process would show",
		"- ack latency includes the broker's 5 ms group-commit window",
		"- fsync durability is platform-qualified: macOS fsync may not flush the drive cache (DD-7)",
		"- no capacity claims: fixed closed-loop response numbers, not a maximum",
		// the source report path, repo-root-relative (F9)
		"Source report: `benchmarks/reports/2026-07-30-abc123def456.json`",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered section lacks %q", want)
		}
	}
}

// TestRenderDeterminism: same report → byte-identical section (D-SL5-4).
func TestRenderDeterminism(t *testing.T) {
	r := loadFixture(t)
	if a, b := render(r), render(r); a != b {
		t.Fatalf("render is not deterministic:\n--- first ---\n%s\n--- second ---\n%s", a, b)
	}
}

// TestSpliceReadme covers the separate splice: replace between markers, or
// append at the pinned anchor (EOF, one blank line) on first render — on
// in-memory copies only, never the real README.
func TestSpliceReadme(t *testing.T) {
	section := markerBegin + "\nNEW\n" + markerEnd
	t.Run("replaces between markers", func(t *testing.T) {
		in := []byte("intro\n\n" + markerBegin + "\nOLD\n" + markerEnd + "\n\ntail\n")
		got, err := spliceReadme(in, section)
		if err != nil {
			t.Fatal(err)
		}
		want := "intro\n\n" + section + "\n\ntail\n"
		if string(got) != want {
			t.Fatalf("splice = %q, want %q", got, want)
		}
	})
	t.Run("appends at the EOF anchor when markers absent", func(t *testing.T) {
		got, err := spliceReadme([]byte("intro\n"), section)
		if err != nil {
			t.Fatal(err)
		}
		want := "intro\n\n" + section + "\n"
		if string(got) != want {
			t.Fatalf("splice = %q, want %q", got, want)
		}
	})
}

// repoRoot resolves ../../ from the package dir (the pinned F9 rule).
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// TestReadmeBenchSectionMatchesCommittedReport is the BENCH-3 gate with
// D-SL5-4's bootstrap rule (see the file header): a hand-edited README
// number, a deleted section, a missing or dirty-stamped reference report —
// each is a build failure once any report is committed.
func TestReadmeBenchSectionMatchesCommittedReport(t *testing.T) {
	root := repoRoot(t)
	reports, err := filepath.Glob(filepath.Join(root, "benchmarks", "reports", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) == 0 {
		t.Skip("bootstrap (D-SL5-4): no committed report in benchmarks/reports/ — gate arms when the reference report lands")
	}
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(readme)
	begin := strings.Index(s, markerBegin)
	end := strings.Index(s, markerEnd)
	if begin < 0 || end < begin {
		t.Fatalf("reports are committed but the README bench markers are missing/malformed — deleting the section is a failure, not a skip (D-SL5-4)")
	}
	section := s[begin : end+len(markerEnd)]

	m := regexp.MustCompile("`(benchmarks/reports/[^`]+\\.json)`").FindStringSubmatch(section)
	if m == nil {
		t.Fatalf("the bench section names no source report path:\n%s", section)
	}
	data, err := os.ReadFile(filepath.Join(root, m[1]))
	if err != nil {
		t.Fatalf("the section's named report is not on disk: %v", err)
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatalf("named report does not unmarshal: %v", err)
	}
	if strings.Contains(r.Commit, "-dirty") || strings.Contains(filepath.Base(m[1]), "-dirty") {
		t.Fatalf("the committed reference report is dirty-stamped (%q) — a published number must trace to a real commit (D-SL5-8)", r.Commit)
	}
	if got := render(r); got != section {
		t.Fatalf("README bench section does not byte-match the re-render of %s — restore with:\n  go run ./cmd/bench -render-readme %s", m[1], m[1])
	}
}
