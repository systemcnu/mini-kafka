// Commit-path unit suite (D-SL2-5): durable reload across boots, the
// two-committer lost-update guard under -race, commit-outside-assignment,
// re-fence-at-install, corrupt-file refusal, and the once-per-boot lazy
// load (a live group never re-reads disk).
package group

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/systemcnu/mini-kafka/internal/storage"
)

// hookFS wraps the FS seam to count ReadFile calls and fire a callback
// inside WriteFileAtomic — the window D-SL2-5's re-fence exists for.
type hookFS struct {
	storage.FS
	mu      sync.Mutex
	reads   map[string]int
	onWrite func(path string)
}

func newHookFS() *hookFS {
	return &hookFS{FS: storage.OSFS(), reads: make(map[string]int)}
}

func (h *hookFS) ReadFile(path string) ([]byte, error) {
	h.mu.Lock()
	h.reads[path]++
	h.mu.Unlock()
	return h.FS.ReadFile(path)
}

func (h *hookFS) readCount(path string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reads[path]
}

func (h *hookFS) WriteFileAtomic(path string, data []byte) error {
	h.mu.Lock()
	hook := h.onWrite
	h.mu.Unlock()
	if hook != nil {
		hook(path)
	}
	return h.FS.WriteFileAtomic(path, data)
}

func (h *hookFS) setOnWrite(fn func(path string)) {
	h.mu.Lock()
	h.onWrite = fn
	h.mu.Unlock()
}

// nextOf pulls one partition's resume offset out of a join result.
func nextOf(t *testing.T, res JoinResult, partition uint32) uint64 {
	t.Helper()
	for _, a := range res.Assigned {
		if a.Partition == partition {
			return a.Next
		}
	}
	t.Fatalf("partition %d not in assignment %+v", partition, res.Assigned)
	return 0
}

// TestCommitPersistsAndReloadsAcrossBoot: an acked commit is durable — a
// new coordinator on the same dir serves it at join (CONS-3's unit half;
// the kill -9 harness owns the process-level half).
func TestCommitPersistsAndReloadsAcrossBoot(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()
	c1, err := New(Config{Clock: clk}, dir, storage.OSFS())
	if err != nil {
		t.Fatal(err)
	}
	r := mustJoin(t, c1, 1, "g", "t", 2)
	if err := c1.Commit("g", r.MemberID, r.Generation, map[uint32]uint64{0: 5, 1: 3}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	c2, err := New(Config{Clock: clk}, dir, storage.OSFS())
	if err != nil {
		t.Fatal(err)
	}
	r2 := mustJoin(t, c2, 1, "g", "t", 2)
	if got := nextOf(t, r2, 0); got != 5 {
		t.Fatalf("partition 0 resume offset after reboot = %d, want 5", got)
	}
	if got := nextOf(t, r2, 1); got != 3 {
		t.Fatalf("partition 1 resume offset after reboot = %d, want 3", got)
	}
}

// TestTwoCommittersCannotEraseEachOther is the BOTH-seats lost-update
// catch: two members committing disjoint partitions concurrently — no
// committed partition may regress, and the file must agree with the map.
// Run under -race.
func TestTwoCommittersCannotEraseEachOther(t *testing.T) {
	dir := t.TempDir()
	c, err := New(Config{Clock: newFakeClock()}, dir, storage.OSFS())
	if err != nil {
		t.Fatal(err)
	}
	mustJoin(t, c, 1, "g", "t", 4)
	r2 := mustJoin(t, c, 2, "g", "t", 4)
	r1 := mustJoin(t, c, 1, "g", "t", 4) // both members at the current generation

	const rounds = 40
	commitLoop := func(res JoinResult, errCh chan<- error) {
		parts := partitionsOf(res)
		for i := 1; i <= rounds; i++ {
			offs := make(map[uint32]uint64, len(parts))
			for _, p := range parts {
				offs[p] = uint64(i)
			}
			if err := c.Commit("g", res.MemberID, res.Generation, offs); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}
	errCh := make(chan error, 2)
	go commitLoop(r1, errCh)
	go commitLoop(r2, errCh)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent commit: %v", err)
		}
	}

	// Every partition must hold the final round — an erased partition means
	// one committer's snapshot clobbered the other's installed commits.
	for p := uint32(0); p < 4; p++ {
		c.mu.Lock()
		got := c.groups["g"].committed[p]
		c.mu.Unlock()
		if got != rounds {
			t.Errorf("partition %d committed = %d, want %d (lost update)", p, got, rounds)
		}
	}
	// File and map agree.
	data, err := os.ReadFile(c.commitPath("g"))
	if err != nil {
		t.Fatal(err)
	}
	_, fromDisk, perr := parseCommitFile(c.commitPath("g"), data)
	if perr != nil {
		t.Fatal(perr)
	}
	for p := uint32(0); p < 4; p++ {
		if fromDisk[p] != rounds {
			t.Errorf("file partition %d = %d, want %d (file/map disagree)", p, fromDisk[p], rounds)
		}
	}
}

// TestCommitOutsideAssignmentIsStale: committing a partition the member
// does not currently own → STALE_GENERATION, zero state change (D-SL2-5).
func TestCommitOutsideAssignmentIsStale(t *testing.T) {
	c, _ := newTestCoord(t)
	mustJoin(t, c, 1, "g", "t", 4)
	r2 := mustJoin(t, c, 2, "g", "t", 4)
	r1 := mustJoin(t, c, 1, "g", "t", 4)

	foreign := partitionsOf(r2)[0]
	err := c.Commit("g", r1.MemberID, r1.Generation, map[uint32]uint64{foreign: 9})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("commit outside assignment err = %v, want ErrStaleGeneration", err)
	}
	c.mu.Lock()
	got := c.groups["g"].committed[foreign]
	c.mu.Unlock()
	if got != 0 {
		t.Fatalf("rejected commit changed state: partition %d = %d", foreign, got)
	}
}

// TestCommitFencedMidWriteInstallsNothing exercises the re-fence-at-install
// window: the member dies (conn drop) while its atomicWrite is in flight —
// the commit must return 13 and install nothing in memory.
func TestCommitFencedMidWriteInstallsNothing(t *testing.T) {
	hfs := newHookFS()
	c, err := New(Config{Clock: newFakeClock()}, t.TempDir(), hfs)
	if err != nil {
		t.Fatal(err)
	}
	r1 := mustJoin(t, c, 1, "g", "t", 2)
	hfs.setOnWrite(func(string) {
		// Mid-write, the committing member's control conn drops: immediate
		// death + rebalance (DD-10). Legal here because the coordinator
		// mutex is never held across the write (D-SL2-5).
		c.ConnClosed(1)
	})
	err = c.Commit("g", r1.MemberID, r1.Generation, map[uint32]uint64{0: 7})
	if !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("fenced-mid-write commit err = %v, want ErrUnknownMember", err)
	}
	c.mu.Lock()
	got := c.groups["g"].committed[0]
	c.mu.Unlock()
	if got != 0 {
		t.Fatalf("fenced commit installed state: partition 0 = %d, want 0", got)
	}
}

// TestCorruptCommitFileRefusesJoinLoudly: an unreadable commit file refuses
// the join naming the file — positions are never guessed — and the refusal
// is sticky for the boot.
func TestCorruptCommitFileRefusesJoinLoudly(t *testing.T) {
	dir := t.TempDir()
	c, err := New(Config{Clock: newFakeClock()}, dir, storage.OSFS())
	if err != nil {
		t.Fatal(err)
	}
	path := c.commitPath("bad")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, jerr := c.Join(1, "bad", "t", 2)
	if !errors.Is(jerr, ErrCorruptCommits) {
		t.Fatalf("join over corrupt file err = %v, want ErrCorruptCommits", jerr)
	}
	if !strings.Contains(jerr.Error(), "bad.json") {
		t.Fatalf("refusal %q does not name the file", jerr)
	}
	if _, again := c.Join(2, "bad", "t", 2); !errors.Is(again, ErrCorruptCommits) {
		t.Fatalf("second join err = %v, want the sticky ErrCorruptCommits", again)
	}
}

// TestLiveGroupNeverReReadsDisk pins the once-per-boot rule (F8): after the
// first join loaded the file, later joins and commits serve memory — the
// file on disk can change underneath without effect, and ReadFile fired
// exactly once.
func TestLiveGroupNeverReReadsDisk(t *testing.T) {
	dir := t.TempDir()
	hfs := newHookFS()
	c, err := New(Config{Clock: newFakeClock()}, dir, hfs)
	if err != nil {
		t.Fatal(err)
	}
	path := c.commitPath("g")
	if err := os.WriteFile(path, []byte(`{"topic":"t","offsets":{"0":7}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	r := mustJoin(t, c, 1, "g", "t", 1)
	if got := nextOf(t, r, 0); got != 7 {
		t.Fatalf("loaded resume offset = %d, want 7", got)
	}
	// Rewrite the file behind the coordinator's back; re-Join must serve the
	// in-memory truth.
	if err := os.WriteFile(path, []byte(`{"topic":"t","offsets":{"0":99}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	r = mustJoin(t, c, 1, "g", "t", 1)
	if got := nextOf(t, r, 0); got != 7 {
		t.Fatalf("re-Join resume offset = %d, want the in-memory 7 (disk re-read!)", got)
	}
	if n := hfs.readCount(path); n != 1 {
		t.Fatalf("commit file read %d times this boot, want exactly 1", n)
	}
}

// TestConcurrentFirstJoinsShareOneLoad: two racing first joins — one loads,
// the other blocks on the latch; both see the loaded state, disk is read
// once (two-phase load, F8).
func TestConcurrentFirstJoinsShareOneLoad(t *testing.T) {
	dir := t.TempDir()
	hfs := newHookFS()
	c, err := New(Config{Clock: newFakeClock()}, dir, hfs)
	if err != nil {
		t.Fatal(err)
	}
	path := c.commitPath("g")
	if err := os.WriteFile(path, []byte(`{"topic":"t","offsets":{"0":4,"1":6}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	results := make(chan JoinResult, 2)
	for conn := uint64(1); conn <= 2; conn++ {
		go func(conn uint64) {
			res, jerr := c.Join(conn, "g", "t", 2)
			if jerr != nil {
				t.Errorf("concurrent join (conn %d): %v", conn, jerr)
			}
			results <- res
		}(conn)
	}
	seen := make(map[string]bool)
	for i := 0; i < 2; i++ {
		res := <-results
		seen[res.MemberID] = true
	}
	if len(seen) != 2 {
		t.Fatalf("concurrent joins produced %d distinct members, want 2", len(seen))
	}
	// Both members between them resume from the loaded offsets.
	r1 := mustJoin(t, c, 1, "g", "t", 2)
	r2 := mustJoin(t, c, 2, "g", "t", 2)
	total := map[uint32]uint64{}
	for _, res := range []JoinResult{r1, r2} {
		for _, a := range res.Assigned {
			total[a.Partition] = a.Next
		}
	}
	if total[0] != 4 || total[1] != 6 {
		t.Fatalf("loaded offsets = %v, want {0:4 1:6}", total)
	}
	if n := hfs.readCount(path); n != 1 {
		t.Fatalf("commit file read %d times, want exactly 1", n)
	}
}

// TestJoinTopicBindingComesFromDisk: the durable file binds the group to
// its topic across restarts — a joiner claiming another topic is refused
// with the binding named (D15 across boots).
func TestJoinTopicBindingComesFromDisk(t *testing.T) {
	dir := t.TempDir()
	c, err := New(Config{Clock: newFakeClock()}, dir, storage.OSFS())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.commitPath("g"), []byte(`{"topic":"alpha","offsets":{"0":2}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, jerr := c.Join(1, "g", "beta", 2)
	if !errors.Is(jerr, ErrTopicMismatch) {
		t.Fatalf("cross-topic join err = %v, want ErrTopicMismatch", jerr)
	}
	if !strings.Contains(jerr.Error(), "bound to topic alpha") {
		t.Fatalf("refusal %q does not name the binding", jerr)
	}
	r := mustJoin(t, c, 2, "g", "alpha", 2)
	if got := nextOf(t, r, 0); got != 2 {
		t.Fatalf("correct-topic join resume offset = %d, want 2", got)
	}
}
