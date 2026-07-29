// Group e2e over a real broker process (D-SL2-10's deterministic legs):
// the kill-one-of-two takeover (GRP-2's e2e complement — control-conn drop
// is immediate death) and the NAMED GRP-3 union test — union of records
// processed across both members' lifetimes == all produced; dupes legal,
// loss fatal. The killed member's conns run through a local TCP proxy so
// the "SIGKILL" is a real connection drop without touching the client API.
package e2e

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/client"
)

// proxy forwards TCP to target; kill drops the listener and EVERY proxied
// conn at once — the broker sees the member's control-conn drop (DD-10).
type proxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	conns  []net.Conn
	closed bool
}

func startProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target}
	go p.acceptLoop()
	t.Cleanup(p.kill)
	return p
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

func (p *proxy) acceptLoop() {
	for {
		down, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			down.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			down.Close()
			up.Close()
			return
		}
		p.conns = append(p.conns, down, up)
		p.mu.Unlock()
		go func() { io.Copy(up, down); up.Close(); down.Close() }()
		go func() { io.Copy(down, up); down.Close(); up.Close() }()
	}
}

// kill is idempotent so t.Cleanup can double up.
func (p *proxy) kill() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	p.ln.Close()
	for _, c := range conns {
		c.Close()
	}
}

func groupSetup(t *testing.T, topicName string, partitions uint32) (addr string) {
	t.Helper()
	bin := buildBroker(t)
	b := startBroker(t, bin, t.TempDir())
	admin, err := client.DialAdmin(b.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topicName, partitions); err != nil {
		t.Fatal(err)
	}
	return b.addr
}

// TestGroupTakeoverOnMemberDeath: two members split four partitions; the
// broker sees one member's conns drop (proxy kill = SIGKILL-equivalent);
// the survivor's next polls deliver records from ALL four partitions —
// takeover proven by delivery, not by inspecting internals.
func TestGroupTakeoverOnMemberDeath(t *testing.T) {
	addr := groupSetup(t, "takeover", 4)

	px := startProxy(t, addr)
	a, err := client.JoinGroup(px.addr(), "grp", "takeover")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close() // reaps goroutines after the kill; LeaveGroup fails silently
	b, err := client.JoinGroup(addr, "grp", "takeover")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// One poll each so both members are settled in the current generation.
	if _, err := a.Poll(200 * time.Millisecond); err != nil {
		t.Fatalf("member A poll: %v", err)
	}
	if _, err := b.Poll(200 * time.Millisecond); err != nil {
		t.Fatalf("member B poll: %v", err)
	}

	px.kill() // member A is dead to the broker: immediate rebalance (DD-10)

	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for part := uint32(0); part < 4; part++ {
		if _, err := prod.Produce("takeover", part, []byte(fmt.Sprintf("post-kill-%d", part))); err != nil {
			t.Fatal(err)
		}
	}

	seen := make(map[uint32]bool)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && len(seen) < 4 {
		recs, err := b.Poll(300 * time.Millisecond)
		if err != nil {
			t.Fatalf("survivor poll: %v", err)
		}
		for _, r := range recs {
			seen[r.Partition] = true
		}
	}
	if len(seen) != 4 {
		t.Fatalf("survivor received from partitions %v, want all 4 — takeover did not happen", seen)
	}
}

// TestGRP3UnionAcrossMemberCrash is the NAMED union test (GRP-3): member A
// commits its first batch, processes a second batch, and is killed BEFORE
// committing it; the survivor consumes on from the committed positions.
// The union of records processed across BOTH lifetimes must equal ALL
// records produced — duplicates legal, loss fatal.
func TestGRP3UnionAcrossMemberCrash(t *testing.T) {
	const perPhase = 40 // per phase, round-robin over 4 partitions
	addr := groupSetup(t, "union", 4)

	px := startProxy(t, addr)
	a, err := client.JoinGroup(px.addr(), "grp", "union")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := client.JoinGroup(addr, "grp", "union")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	all := make(map[string]bool)
	prod, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	producePhase := func(phase int) {
		for i := 0; i < perPhase; i++ {
			payload := fmt.Sprintf("u%d-%d", phase, i)
			if _, err := prod.Produce("union", uint32(i%4), []byte(payload)); err != nil {
				t.Fatal(err)
			}
			all[payload] = true
		}
	}

	union := make(map[string]bool)
	collect := func(recs []client.PartRecord) {
		for _, r := range recs {
			union[string(r.Payload)] = true
		}
	}

	// Phase 1: A polls (rejoining internally as needed) and COMMITS.
	producePhase(1)
	deadline := time.Now().Add(15 * time.Second)
	committed := false
	for time.Now().Before(deadline) && !committed {
		recs, perr := a.Poll(300 * time.Millisecond)
		if perr != nil {
			t.Fatalf("member A phase-1 poll: %v", perr)
		}
		collect(recs)
		if len(recs) > 0 {
			if cerr := a.Commit(); cerr != nil {
				t.Fatalf("member A phase-1 commit: %v", cerr)
			}
			committed = true
		}
	}
	if !committed {
		t.Fatal("member A never received phase-1 records")
	}

	// Phase 2: A processes a batch and is killed BEFORE its commit.
	producePhase(2)
	deadline = time.Now().Add(15 * time.Second)
	got := false
	for time.Now().Before(deadline) && !got {
		recs, perr := a.Poll(300 * time.Millisecond)
		if perr != nil {
			t.Fatalf("member A phase-2 poll: %v", perr)
		}
		collect(recs)
		got = len(recs) > 0
	}
	if !got {
		t.Fatal("member A never received phase-2 records")
	}
	px.kill() // mid-batch, pre-commit: the uncommitted work must be redelivered

	// The survivor consumes on (committing per batch) until the union of
	// both lifetimes covers everything produced.
	deadline = time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) && len(union) < len(all) {
		recs, perr := b.Poll(300 * time.Millisecond)
		if perr != nil {
			t.Fatalf("survivor poll: %v", perr)
		}
		collect(recs)
		if len(recs) > 0 {
			if cerr := b.Commit(); cerr != nil {
				// A 12/13 here is the pinned D-SL2-8 contract, not a bug:
				// the death bump can land between a served poll and its
				// commit, and Commit surfaces the fence — the next Poll
				// re-joins. Anything else is fatal.
				var typed *client.Error
				if !errors.As(cerr, &typed) ||
					(typed.Code != client.CodeStaleGeneration && typed.Code != client.CodeUnknownMember) {
					t.Fatalf("survivor commit: %v", cerr)
				}
			}
		}
	}
	if len(union) != len(all) {
		var missing []string
		for p := range all {
			if !union[p] {
				missing = append(missing, p)
				if len(missing) == 5 {
					break
				}
			}
		}
		t.Fatalf("union has %d of %d produced records — LOSS (e.g. %v)", len(union), len(all), missing)
	}
}
