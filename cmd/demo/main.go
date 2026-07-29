// Command demo is scenario A: a two-act narrated tour (D-SL3-1) — broker
// in-process on an ephemeral loopback port, topic demo×4, one producer and
// two GroupConsumers over real TCP via the shipped client (PROT-2: no
// privileged path). Narration rules (D-SL3-3): startup order pinned
// (create topic → both consumers → settled 2-member assignment narrated →
// only then the producer), one line per ownership change diffed via
// Assignment(), first-record-per-partition lines, then per-second
// aggregates; act headers and the two #event lines are exact, greppable,
// on their own lines; every line is deterministic (no ports, paths, or
// timing-dependent content), so a default and a -ci run emit
// byte-identical transcripts. The producer paces ~20 msg/s in one-second
// ticks, and the narrator finishes a tick only after every record of that
// tick arrived — which is what makes the printed counts deterministic.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/systemcnu/mini-kafka/client"
	"github.com/systemcnu/mini-kafka/internal/broker"
)

const (
	topicName  = "demo"
	partitions = 4
	groupName  = "demo"

	msgsPerTick = 20                    // one tick = one second of flow at ~20 msg/s (D-SL3-1)
	produceGap  = 50 * time.Millisecond // paces the producer inside a tick

	stallGuard = 30 * time.Second // bound on any wait that should take ~1 s
)

func main() {
	// -ci is reserved for terminal frills; none exist today, so narration
	// is byte-identical to the default by construction (D-SL3-3).
	ci := flag.Bool("ci", false, "CI mode (narration is identical to the default)")
	flow := flag.Duration("flow", 5*time.Second, "visible-flow duration per act (test seam; visitors and CI take the default)")
	flag.Parse()
	_ = *ci
	ticks := int(flow.Seconds())
	if ticks < 1 {
		ticks = 1
	}

	dataDir, err := os.MkdirTemp("", "minikafka-demo-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "demo: %v\n", err)
		os.Exit(1)
	}
	// removeData is shared by the normal exit, every fatal path, and the
	// signal handler: the temp dir is cleaned on any self-terminating exit
	// (D-SL3-1/4; a SIGKILL legally leaves it — accepted in §3).
	removeData := sync.OnceFunc(func() { os.RemoveAll(dataDir) })
	fatalf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "demo: "+format+"\n", args...)
		removeData()
		os.Exit(1)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	go func() {
		<-sig
		removeData()
		os.Exit(1)
	}()

	srv, err := broker.New(broker.Config{Addr: "127.0.0.1:0", DataDir: dataDir})
	if err != nil {
		fatalf("starting broker: %v", err)
	}
	if err := srv.Start(); err != nil {
		fatalf("starting broker: %v", err)
	}
	addr := srv.Addr().String()

	fmt.Println("mini-kafka demo — one broker, one producer, two group consumers, real TCP")
	fmt.Println()
	fmt.Println("— act one —")
	fmt.Println("broker up (in-process, loopback, temp data dir)")

	admin, err := client.DialAdmin(addr)
	if err != nil {
		fatalf("dialing admin: %v", err)
	}
	if err := admin.CreateTopic(topicName, partitions); err != nil {
		fatalf("creating topic: %v", err)
	}
	fmt.Printf("created topic %s (%d partitions)\n", topicName, partitions)

	// Startup order pinned by D-SL3-3: both consumers join and the
	// 2-member assignment settles BEFORE the producer starts, so the
	// ownership lines legitimately precede #event first-flow and the
	// transient consumer-1-owns-all-4 moment is settled before any record
	// flows. The wait is on the settled assignment, never on sleeps.
	c1 := newWatcher("consumer-1", addr, fatalf)
	c2 := newWatcher("consumer-2", addr, fatalf)
	guard := time.Now().Add(stallGuard)
	for len(c1.owned) != partitions/2 {
		if time.Now().After(guard) {
			fatalf("2-member assignment never settled (consumer-1 owns %v)", c1.owned)
		}
		c1.poll(200 * time.Millisecond)
	}

	prod, err := client.DialProducer(addr)
	if err != nil {
		fatalf("dialing producer: %v", err)
	}
	// The one producer goroutine (D-SL3-1): msg-<n> round-robin across
	// partitions, one msgsPerTick batch per request, paced to ~20 msg/s.
	tickReq := make(chan struct{})
	go func() {
		n := 0
		for range tickReq {
			for i := 0; i < msgsPerTick; i++ {
				payload := fmt.Sprintf("msg-%d", n)
				if _, err := prod.Produce(topicName, uint32(n%partitions), []byte(payload)); err != nil {
					fatalf("producing %s: %v", payload, err)
				}
				n++
				time.Sleep(produceGap)
			}
		}
	}()
	fmt.Println("producing msg-<n> at ~20 msg/s across all 4 partitions")

	// Act one flow: both consumers drain each tick completely before the
	// aggregate lines print in fixed order — determinism by construction.
	firstFlow := false
	for tick := 1; tick <= ticks; tick++ {
		tickReq <- struct{}{}
		c1.target += msgsPerTick / 2
		c2.target += msgsPerTick / 2
		guard = time.Now().Add(stallGuard)
		for c1.total < c1.target || c2.total < c2.target {
			if time.Now().After(guard) {
				fatalf("act one tick %d stalled (consumer-1 %d/%d, consumer-2 %d/%d)",
					tick, c1.total, c1.target, c2.total, c2.target)
			}
			for _, w := range []*watcher{c1, c2} {
				if w.total >= w.target {
					continue
				}
				recs := w.poll(300 * time.Millisecond)
				if !firstFlow && len(recs) > 0 {
					fmt.Println("#event first-flow")
					firstFlow = true
				}
			}
		}
		if tick == 1 {
			c1.printFirsts()
			c2.printFirsts()
		}
		c1.printAggregate()
		c2.printAggregate()
		c1.commit()
		c2.commit()
	}

	fmt.Println()
	fmt.Println("— act two —")
	fmt.Println("killing consumer-2: dropping its connections mid-flight, no goodbye")
	c2.c.Abandon()

	// Takeover: the control-conn drop is the death event (DD-10); polling
	// consumer-1 drives its re-join, and the ownership diff prints the
	// takeover line — anchored after the act-two header (F4).
	guard = time.Now().Add(stallGuard)
	for len(c1.owned) != partitions {
		if time.Now().After(guard) {
			fatalf("takeover never settled (consumer-1 owns %v)", c1.owned)
		}
		c1.poll(300 * time.Millisecond)
	}
	fmt.Println("consumer-1 resumes partitions 2,3 from consumer-2's last committed offsets")

	for tick := 1; tick <= ticks; tick++ {
		tickReq <- struct{}{}
		c1.target += msgsPerTick
		guard = time.Now().Add(stallGuard)
		for c1.total < c1.target {
			if time.Now().After(guard) {
				fatalf("act two tick %d stalled (consumer-1 %d/%d)", tick, c1.total, c1.target)
			}
			c1.poll(300 * time.Millisecond)
		}
		c1.printAggregate()
		c1.commit()
	}

	close(tickReq)
	c1.c.Close()
	c2.c.Close() // no-op: Abandon shares the closeOnce (D-SL3-2)
	prod.Close()
	admin.Close()
	srv.Stop()
	removeData()
	fmt.Println()
	fmt.Println("demo complete: two acts, one takeover, nothing lost")
	fmt.Println("#event done")
}

// watcher wraps one GroupConsumer with the narration state the main loop
// needs: last-narrated ownership, counts, and first record per partition.
type watcher struct {
	name   string
	c      *client.GroupConsumer
	fatalf func(string, ...any)

	owned    []uint32
	total    int
	target   int
	prev     int // total at the last aggregate line
	tickPart map[uint32]bool
	firsts   map[uint32]client.PartRecord
}

// newWatcher joins the group and narrates the initial ownership line.
func newWatcher(name, addr string, fatalf func(string, ...any)) *watcher {
	c, err := client.JoinGroup(addr, groupName, topicName)
	if err != nil {
		fatalf("%s joining: %v", name, err)
	}
	w := &watcher{
		name: name, c: c, fatalf: fatalf,
		tickPart: make(map[uint32]bool),
		firsts:   make(map[uint32]client.PartRecord),
	}
	w.narrateOwnership()
	return w
}

// poll runs one Poll, narrates any ownership change (the D-SL3-3 diff via
// Assignment()), and accumulates counts.
func (w *watcher) poll(maxWait time.Duration) []client.PartRecord {
	recs, err := w.c.Poll(maxWait)
	if err != nil {
		w.fatalf("%s polling: %v", w.name, err)
	}
	w.narrateOwnership()
	for _, r := range recs {
		w.total++
		w.tickPart[r.Partition] = true
		if _, seen := w.firsts[r.Partition]; !seen {
			w.firsts[r.Partition] = r
		}
	}
	return recs
}

// narrateOwnership prints one line per ownership change.
func (w *watcher) narrateOwnership() {
	cur := w.c.Assignment()
	if partsEqual(cur, w.owned) {
		return
	}
	w.owned = cur
	fmt.Printf("%s owns partitions %s\n", w.name, partsList(cur))
}

// printFirsts prints the first-record-per-partition lines in partition
// order (deterministic: partition p's first record is always msg-p at
// offset 0).
func (w *watcher) printFirsts() {
	for _, p := range sortedKeys(w.firsts) {
		r := w.firsts[p]
		fmt.Printf("%s: partition %d first record %s (offset %d)\n", w.name, r.Partition, r.Payload, r.Offset)
	}
}

// printAggregate prints the per-second (per-tick) aggregate line naming
// the partitions that actually delivered this tick.
func (w *watcher) printAggregate() {
	fmt.Printf("%s: +%d records (total %d) on partitions %s\n",
		w.name, w.total-w.prev, w.total, partsList(sortedKeys(w.tickPart)))
	w.prev = w.total
	w.tickPart = make(map[uint32]bool)
}

func (w *watcher) commit() {
	if err := w.c.Commit(); err != nil {
		w.fatalf("%s committing: %v", w.name, err)
	}
}

func partsEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedKeys[V any](m map[uint32]V) []uint32 {
	out := make([]uint32, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func partsList(parts []uint32) string {
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", p)
	}
	return b.String()
}
