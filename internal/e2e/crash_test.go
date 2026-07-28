// kill -9 harness (D-SL1-5/6): the real broker binary SIGKILLed mid-load,
// restarted on the same data dir, compared against the harness's ack
// journal — LOG-1/PROD-2 half (b) by construction: real OS, real process,
// no fakes. Assertions are one-directional: every journaled ack must be
// fetched at its exact offset; the fetched set MAY hold more (acks the kill
// outran, kept-but-hidden records a later flush surfaced).
package e2e

import (
	"bufio"
	"fmt"
	"math/rand"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/systemcnu/mini-kafka/client"
)

const (
	producers  = 4
	partitions = 2
	cycles     = 3
	topic      = "crash"
)

// payloadShape is the only claim made about fetched-but-unjournaled records.
var payloadShape = regexp.MustCompile(`^p[0-3]-[0-9]+$`)

// journal is the harness's ack memory: it survives the kills the broker
// does not. Entries are added ONLY after Produce returns an offset.
type journal struct {
	mu   sync.Mutex
	acks map[uint32]map[uint64]string
}

func newJournal() *journal {
	j := &journal{acks: make(map[uint32]map[uint64]string)}
	for p := uint32(0); p < partitions; p++ {
		j.acks[p] = make(map[uint64]string)
	}
	return j
}

func (j *journal) add(t *testing.T, part uint32, off uint64, payload string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if prev, ok := j.acks[part][off]; ok && prev != payload {
		t.Errorf("offset %d of partition %d acked twice: %q then %q", off, part, prev, payload)
	}
	j.acks[part][off] = payload
}

// snapshot copies one partition's journaled acks for lock-free comparison.
func (j *journal) snapshot(part uint32) map[uint64]string {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make(map[uint64]string, len(j.acks[part]))
	for off, p := range j.acks[part] {
		out[off] = p
	}
	return out
}

type brokerProc struct {
	cmd  *exec.Cmd
	addr string
	once sync.Once
}

// kill SIGKILLs the broker and reaps it — Wait() after the kill or the pid
// lingers as a zombie (PLAN pitfall). Idempotent so t.Cleanup can double up.
func (b *brokerProc) kill() {
	b.once.Do(func() {
		b.cmd.Process.Kill()
		b.cmd.Wait()
	})
}

// parseListeningAddr extracts the resolved address from cmd/minikafka's
// startup line ("minikafka listening on 127.0.0.1:NNN (data: ...)").
func parseListeningAddr(line string) (string, bool) {
	const marker = " listening on "
	i := strings.Index(line, marker)
	if i < 0 {
		return "", false
	}
	rest := line[i+len(marker):]
	j := strings.Index(rest, " (data:")
	if j < 0 {
		return "", false
	}
	return rest[:j], true
}

func buildBroker(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "minikafka")
	out, err := exec.Command("go", "build", "-o", bin, "github.com/systemcnu/mini-kafka/cmd/minikafka").CombinedOutput()
	if err != nil {
		t.Fatalf("building broker: %v\n%s", err, out)
	}
	return bin
}

// startBroker launches the binary on an ephemeral port and parses the
// resolved address from its STDERR log line (D-SL1-6) — re-done every start,
// since :0 re-binds.
func startBroker(t *testing.T, bin, dataDir string) *brokerProc {
	t.Helper()
	cmd := exec.Command(bin, "--addr", "127.0.0.1:0", "--data", dataDir)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	b := &brokerProc{cmd: cmd}
	t.Cleanup(b.kill)
	addrCh := make(chan string, 1)
	go func() {
		// Keep draining past the match so the child never blocks on a full
		// stderr pipe.
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			if a, ok := parseListeningAddr(sc.Text()); ok {
				select {
				case addrCh <- a:
				default:
				}
			}
		}
	}()
	select {
	case a := <-addrCh:
		b.addr = a
		return b
	case <-time.After(5 * time.Second):
		b.kill()
		t.Fatal("timed out waiting for the broker's listening line on stderr")
		return nil
	}
}

// runLoadAndKill drives 4 sequenced producers round-robin over the
// partitions, SIGKILLs the broker under load after 50–250 ms, reaps it, and
// joins the producers. seqs persists per-producer sequence numbers across
// cycles so every payload is unique.
func runLoadAndKill(t *testing.T, b *brokerProc, j *journal, seqs *[producers]int) {
	var wg sync.WaitGroup
	for pid := 0; pid < producers; pid++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			prod, err := client.DialProducer(b.addr)
			if err != nil {
				return // the kill can outrun the dial; nothing was acked
			}
			defer prod.Close()
			for {
				seq := seqs[pid]
				payload := fmt.Sprintf("p%d-%d", pid, seq)
				part := uint32(seq % partitions)
				off, err := prod.Produce(topic, part, []byte(payload))
				if err != nil {
					return // in-flight produce the kill outran: never journaled
				}
				// Journal ONLY after Produce returned its offset — journaling
				// at send would fabricate acks and unsound the assertion.
				j.add(t, part, off, payload)
				seqs[pid]++
			}
		}(pid)
	}
	time.Sleep(time.Duration(50+rand.Intn(201)) * time.Millisecond)
	b.kill()
	wg.Wait()
}

// fetchToTail reads one partition from offset 0 until an empty batch.
func fetchToTail(t *testing.T, addr string, part uint32) []client.Record {
	t.Helper()
	cons, err := client.DialConsumer(addr)
	if err != nil {
		t.Fatalf("dial consumer: %v", err)
	}
	defer cons.Close()
	var out []client.Record
	off := uint64(0)
	for {
		recs, err := cons.Fetch(topic, part, off, 1, 0)
		if err != nil {
			t.Fatalf("fetch partition %d at %d: %v", part, off, err)
		}
		if len(recs) == 0 {
			return out
		}
		out = append(out, recs...)
		off = recs[len(recs)-1].Offset + 1
	}
}

func TestKill9CrashCycles(t *testing.T) {
	start := time.Now()
	bin := buildBroker(t)
	dataDir := t.TempDir()
	j := newJournal()
	var seqs [producers]int

	for cycle := 0; cycle < cycles; cycle++ {
		victim := startBroker(t, bin, dataDir)
		if cycle == 0 {
			admin, err := client.DialAdmin(victim.addr)
			if err != nil {
				t.Fatal(err)
			}
			if err := admin.CreateTopic(topic, partitions); err != nil {
				t.Fatalf("create topic: %v", err)
			}
			admin.Close()
		}
		runLoadAndKill(t, victim, j, &seqs)

		verifier := startBroker(t, bin, dataDir)
		for part := uint32(0); part < partitions; part++ {
			recs := fetchToTail(t, verifier.addr, part)
			// Receipt evidence: what this cycle actually proved, in numbers.
			t.Logf("cycle %d partition %d: %d journaled acks, %d fetched records (%d unjournaled surplus)",
				cycle, part, len(j.snapshot(part)), len(recs), len(recs)-len(j.snapshot(part)))
			// Density is asserted of the FETCHED offsets only — dense by
			// recovery construction — never of the journal.
			fetched := make(map[uint64]string, len(recs))
			for i, r := range recs {
				if r.Offset != uint64(i) {
					t.Fatalf("cycle %d partition %d: fetched offsets not dense at index %d (offset %d)", cycle, part, i, r.Offset)
				}
				fetched[r.Offset] = string(r.Payload)
			}
			journaled := j.snapshot(part)
			// One-directional: ack ⇒ frontier-covered ⇒ refuse-protected.
			for off, want := range journaled {
				if got, ok := fetched[off]; !ok || got != want {
					t.Errorf("cycle %d partition %d: journaled ack %q@%d fetched as %q (present=%v)", cycle, part, want, off, got, ok)
				}
			}
			// Fetched ⊇ journaled, never equality: the surplus (outran acks,
			// kept-but-hidden records a later flush surfaced) is legal, but
			// must look like something a producer wrote.
			for off, got := range fetched {
				if _, ok := journaled[off]; !ok && !payloadShape.MatchString(got) {
					t.Errorf("cycle %d partition %d: unjournaled record %q@%d fails the shape check", cycle, part, got, off)
				}
			}
		}

		// Post-restart produces must still succeed (LOG-5's healed half).
		prod, err := client.DialProducer(verifier.addr)
		if err != nil {
			t.Fatal(err)
		}
		for part := uint32(0); part < partitions; part++ {
			// Consume producer <part>'s sequence space so the payload stays
			// unique and shape-valid if it surfaces unjournaled later.
			seq := seqs[part]
			payload := fmt.Sprintf("p%d-%d", part, seq)
			off, err := prod.Produce(topic, part, []byte(payload))
			if err != nil {
				t.Fatalf("cycle %d: post-restart produce on partition %d: %v", cycle, part, err)
			}
			j.add(t, part, off, payload)
			seqs[part]++
		}
		prod.Close()
		verifier.kill()
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Errorf("harness took %v, budget is 20s (D-SL1-5)", elapsed)
	}
}
