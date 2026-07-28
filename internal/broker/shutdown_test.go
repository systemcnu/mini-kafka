// Graceful-stop drain test (D-SL0-6): a parked fetch must come back with
// its empty-at-timeout shape when the broker stops — long before its
// maxWait — and Stop itself must return.
package broker

import (
	"testing"
	"time"

	"mini-kafka/internal/wire"
)

func waitForCond(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestParkedFetchReturnsEmptyOnGracefulStop(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "drainy", 1)

	// Park a fetch at the empty tail with a deliberately long maxWait.
	req := wire.Fetch{Topic: "drainy", Entries: []wire.FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 25_000}
	if err := wire.WriteFrame(conn, wire.TypeFetch, req.Encode()); err != nil {
		t.Fatal(err)
	}
	p, err := s.store.Partition("drainy", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool { return p.ParkedWaiters() == 1 }, "fetch to park")

	start := time.Now()
	stopped := make(chan struct{})
	go func() {
		s.Stop() // stands in for SIGTERM: main calls exactly this
		close(stopped)
	}()

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	typ, body, err := wire.ReadFrame(conn, wire.MaxResponseFrame)
	if err != nil {
		t.Fatalf("reading the drained fetch response: %v", err)
	}
	if typ != wire.TypeFetchResp {
		t.Fatalf("response type %d, want FetchResp", typ)
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		t.Fatal(werr)
	}
	if len(resp.Groups) != 1 || resp.Groups[0].Partition != 0 || len(resp.Groups[0].Recs) != 0 {
		t.Fatalf("drained fetch resp = %+v, want one zero-rec group (the empty-at-timeout shape)", resp)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("drained fetch took %v — it waited out its own maxWait instead of being released", elapsed)
	}

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return")
	}
}
