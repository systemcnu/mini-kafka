// Client tests against a live in-process broker over real loopback TCP.
// The broker import is test-only: the production client package speaks
// wire frames alone.
package client_test

import (
	"errors"
	"testing"

	"github.com/systemcnu/mini-kafka/client"
	"github.com/systemcnu/mini-kafka/internal/broker"
)

func startBroker(t *testing.T) string {
	t.Helper()
	s, err := broker.New(broker.Config{Addr: "127.0.0.1:0", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	return s.Addr().String()
}

func TestProduceConsumeRoundtrip(t *testing.T) {
	addr := startBroker(t)

	admin, err := client.DialAdmin(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic("demo", 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	topics, err := admin.Topics()
	if err != nil || len(topics) != 1 || topics[0].Name != "demo" || topics[0].Partitions != 2 {
		t.Fatalf("Topics() = %+v, %v", topics, err)
	}

	p, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i, m := range []string{"one", "two", "three"} {
		off, err := p.Produce("demo", 1, []byte(m))
		if err != nil || off != uint64(i) {
			t.Fatalf("Produce(%q) = %d, %v; want %d, nil", m, off, err, i)
		}
	}

	c, err := client.DialConsumer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	recs, err := c.Fetch("demo", 1, 0, 1000, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("fetched %d records, want 3", len(recs))
	}
	for i, want := range []string{"one", "two", "three"} {
		if recs[i].Offset != uint64(i) || string(recs[i].Payload) != want {
			t.Errorf("rec %d = %q@%d, want %q@%d", i, recs[i].Payload, recs[i].Offset, want, i)
		}
	}
}

func TestBrokerErrorSurfacesAsTypedCode(t *testing.T) {
	addr := startBroker(t)
	p, err := client.DialProducer(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Produce("ghost", 0, []byte("x"))
	var cerr *client.Error
	if !errors.As(err, &cerr) || cerr.Code != client.CodeUnknownTopic {
		t.Fatalf("err = %v, want *client.Error{CodeUnknownTopic}", err)
	}
	// The connection stays usable after a served error.
	if _, err := p.Produce("ghost", 0, []byte("y")); !errors.As(err, &cerr) {
		t.Fatalf("second produce after error: %v", err)
	}
}
