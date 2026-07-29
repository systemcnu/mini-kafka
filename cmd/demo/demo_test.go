// Black-box smoke test for the demo (SL3 §4): builds the real binary once,
// runs it twice — default and -ci — with the short -flow override and a
// test-owned TMPDIR, then asserts the pinned marker order, an empty TMPDIR
// after exit, and byte-identical narration between the two modes (F5/CF1:
// the gated surface is the tested surface).
package main_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var demoBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "demo-build-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	demoBin = filepath.Join(dir, "demo")
	build := exec.Command("go", "build", "-o", demoBin, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "building demo binary: %v\n", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// runDemo runs the built binary with -flow 2s (the test seam — no visitor
// or CI run takes the short path) under a test-owned TMPDIR, and returns
// the stdout transcript. It asserts exit 0 and that the demo's MkdirTemp
// data dir was removed (F8: TMPDIR-owned makes "temp dir removed"
// assertable).
func runDemo(t *testing.T, ci bool) string {
	t.Helper()
	tmp := t.TempDir()
	args := []string{"-flow", "2s"}
	if ci {
		args = append(args, "-ci")
	}
	cmd := exec.Command(demoBin, args...)
	cmd.Env = append(os.Environ(), "TMPDIR="+tmp)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("demo (ci=%v) failed: %v\nstderr:\n%s\nstdout:\n%s", ci, err, stderr.String(), stdout.String())
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("demo (ci=%v) left %v in TMPDIR — the temp data dir must be removed on exit (D-SL3-1)", ci, names)
	}
	return stdout.String()
}

// lineIndex returns the index of the first line equal to want at or after
// from, or -1.
func lineIndex(lines []string, want string, from int) int {
	for i := from; i < len(lines); i++ {
		if lines[i] == want {
			return i
		}
	}
	return -1
}

// linePrefixIndex returns the index of the first line starting with prefix
// at or after from, or -1.
func linePrefixIndex(lines []string, prefix string, from int) int {
	for i := from; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], prefix) {
			return i
		}
	}
	return -1
}

// assertMarkers pins the D-SL3-3/F4 transcript order: act one header ·
// both consumers' ownership lines · #event first-flow · act two header ·
// a takeover line showing consumer-1 owning all 4 anchored AFTER the
// act-two header (the act-one transient owns-all-4 moment must not
// satisfy it) · #event done.
func assertMarkers(t *testing.T, transcript string) {
	t.Helper()
	lines := strings.Split(transcript, "\n")

	actOne := lineIndex(lines, "— act one —", 0)
	if actOne < 0 {
		t.Fatalf("no act one header in transcript:\n%s", transcript)
	}
	own1 := linePrefixIndex(lines, "consumer-1 owns partitions ", actOne)
	own2 := linePrefixIndex(lines, "consumer-2 owns partitions ", actOne)
	firstFlow := lineIndex(lines, "#event first-flow", actOne)
	if firstFlow < 0 {
		t.Fatalf("no #event first-flow line in transcript:\n%s", transcript)
	}
	if own1 < 0 || own1 > firstFlow {
		t.Fatalf("consumer-1 ownership line missing or after first-flow (idx %d vs %d)", own1, firstFlow)
	}
	if own2 < 0 || own2 > firstFlow {
		t.Fatalf("consumer-2 ownership line missing or after first-flow (idx %d vs %d)", own2, firstFlow)
	}
	actTwo := lineIndex(lines, "— act two —", firstFlow)
	if actTwo < 0 {
		t.Fatalf("no act two header after first-flow in transcript:\n%s", transcript)
	}
	takeover := lineIndex(lines, "consumer-1 owns partitions 0,1,2,3", actTwo)
	if takeover < 0 {
		t.Fatalf("no takeover line (consumer-1 owns all 4) AFTER the act two header:\n%s", transcript)
	}
	done := lineIndex(lines, "#event done", takeover)
	if done < 0 {
		t.Fatalf("no #event done line after the takeover in transcript:\n%s", transcript)
	}
}

// TestDemoTranscript runs the demo in both modes and pins the marker
// order, exit/cleanup behavior, and default-vs-ci byte identity (D-SL3-3).
func TestDemoTranscript(t *testing.T) {
	def := runDemo(t, false)
	ci := runDemo(t, true)
	assertMarkers(t, def)
	if def != ci {
		t.Fatalf("default and -ci narration differ (D-SL3-3 demands byte-identical lines)\n--- default ---\n%s\n--- ci ---\n%s", def, ci)
	}
}
