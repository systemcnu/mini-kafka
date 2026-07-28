// Integration tests over real loopback TCP: PROD-1 (produce/fetch
// roundtrip), LOG-2 (contiguous offsets across restart), LOG-3 (fetch
// preserves append order). Tests speak the raw wire protocol; the public
// client package layers on the same frames.
package broker

import (
	"net"
	"testing"

	"github.com/systemcnu/mini-kafka/internal/wire"
)

// startBroker runs a broker on an ephemeral loopback port over dataDir.
func startBroker(t *testing.T, dataDir string) *Server {
	t.Helper()
	s, err := New(Config{Addr: "127.0.0.1:0", DataDir: dataDir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(s.Stop)
	return s
}

func dialBroker(t *testing.T, s *Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// roundtrip sends one request frame and decodes one response frame.
func roundtrip(t *testing.T, conn net.Conn, typ byte, body []byte) (byte, []byte) {
	t.Helper()
	if err := wire.WriteFrame(conn, typ, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	respType, respBody, err := wire.ReadFrame(conn, wire.MaxResponseFrame)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return respType, respBody
}

func mustCreateTopic(t *testing.T, conn net.Conn, topic string, partitions uint32) {
	t.Helper()
	typ, body := roundtrip(t, conn, wire.TypeCreateTopic, wire.CreateTopic{Topic: topic, Partitions: partitions}.Encode())
	if typ != wire.TypeCreateTopicResp {
		t.Fatalf("create-topic response type %d, body %x", typ, body)
	}
}

func mustProduce(t *testing.T, conn net.Conn, topic string, partition uint32, payload string) uint64 {
	t.Helper()
	typ, body := roundtrip(t, conn, wire.TypeProduce, wire.Produce{Topic: topic, Partition: partition, Payload: []byte(payload)}.Encode())
	if typ != wire.TypeProduceResp {
		t.Fatalf("produce response type %d, body %x", typ, body)
	}
	resp, werr := wire.DecodeProduceResp(body)
	if werr != nil {
		t.Fatalf("decode produce resp: %v", werr)
	}
	return resp.Offset
}

func mustFetch(t *testing.T, conn net.Conn, topic string, partition uint32, offset uint64, maxWaitMs uint32) []wire.Rec {
	t.Helper()
	req := wire.Fetch{
		Topic:     topic,
		Entries:   []wire.FetchEntry{{Partition: partition, Offset: offset}},
		MaxWaitMs: maxWaitMs,
		MaxBytes:  0, // default
	}
	typ, body := roundtrip(t, conn, wire.TypeFetch, req.Encode())
	if typ != wire.TypeFetchResp {
		t.Fatalf("fetch response type %d, body %x", typ, body)
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		t.Fatalf("decode fetch resp: %v", werr)
	}
	if len(resp.Groups) != 1 || resp.Groups[0].Partition != partition {
		t.Fatalf("fetch resp groups = %+v, want one group for partition %d", resp.Groups, partition)
	}
	return resp.Groups[0].Recs
}

func expectError(t *testing.T, conn net.Conn, reqType byte, body []byte, code wire.Code) {
	t.Helper()
	typ, respBody := roundtrip(t, conn, reqType, body)
	if typ != wire.TypeError {
		t.Fatalf("response type %d, want Error frame", typ)
	}
	em, werr := wire.DecodeErrorMsg(respBody)
	if werr != nil {
		t.Fatalf("decode error frame: %v", werr)
	}
	if em.Code != uint16(code) {
		t.Fatalf("error code %d (%s), want %d", em.Code, em.Msg, code)
	}
}

func TestProduceFetchRoundtrip(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)

	mustCreateTopic(t, conn, "demo", 2)

	// PROD-1: produce returns the assigned offsets.
	for i, m := range []string{"first", "second", "third"} {
		if off := mustProduce(t, conn, "demo", 1, m); off != uint64(i) {
			t.Fatalf("produce %q offset = %d, want %d", m, off, i)
		}
	}

	// LOG-3: fetch returns them in append order with the same offsets.
	recs := mustFetch(t, conn, "demo", 1, 0, 1000)
	if len(recs) != 3 {
		t.Fatalf("fetched %d records, want 3", len(recs))
	}
	for i, want := range []string{"first", "second", "third"} {
		if recs[i].Offset != uint64(i) || string(recs[i].Payload) != want {
			t.Errorf("rec %d = %q@%d, want %q@%d", i, recs[i].Payload, recs[i].Offset, want, i)
		}
	}

	// The other partition is untouched.
	if recs := mustFetch(t, conn, "demo", 0, 0, 1); len(recs) != 0 {
		t.Fatalf("partition 0 has %d records, want 0", len(recs))
	}
}

func TestOffsetsContiguousAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s := startBroker(t, dir)
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "logtwo", 1)
	for i := 0; i < 3; i++ {
		mustProduce(t, conn, "logtwo", 0, "pre")
	}
	s.Stop()

	s2 := startBroker(t, dir)
	conn2 := dialBroker(t, s2)
	// LOG-2: offsets continue exactly where the pre-restart log ended.
	if off := mustProduce(t, conn2, "logtwo", 0, "post-a"); off != 3 {
		t.Fatalf("first post-restart offset = %d, want 3", off)
	}
	if off := mustProduce(t, conn2, "logtwo", 0, "post-b"); off != 4 {
		t.Fatalf("second post-restart offset = %d, want 4", off)
	}
	recs := mustFetch(t, conn2, "logtwo", 0, 0, 1000)
	if len(recs) != 5 {
		t.Fatalf("fetched %d records, want 5", len(recs))
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Errorf("rec %d offset = %d, want %d", i, r.Offset, i)
		}
	}
}

func TestProduceToUnknownTopicAndBadPartition(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "ghost", Partition: 0, Payload: []byte("x")}.Encode(),
		wire.CodeUnknownTopic)

	mustCreateTopic(t, conn, "narrow", 1)
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "narrow", Partition: 1, Payload: []byte("x")}.Encode(),
		wire.CodeBadPartition)
}

func TestListTopics(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "bravo", 2)
	mustCreateTopic(t, conn, "alpha", 1)

	typ, body := roundtrip(t, conn, wire.TypeListTopics, nil)
	if typ != wire.TypeListTopicsResp {
		t.Fatalf("list response type %d", typ)
	}
	resp, werr := wire.DecodeListTopicsResp(body)
	if werr != nil {
		t.Fatal(werr)
	}
	if len(resp.Topics) != 2 || resp.Topics[0].Name != "alpha" || resp.Topics[1].Name != "bravo" ||
		resp.Topics[0].Partitions != 1 || resp.Topics[1].Partitions != 2 {
		t.Fatalf("topics = %+v, want sorted alpha/1 bravo/2", resp.Topics)
	}
}

func TestDuplicateCreateTopicOverWire(t *testing.T) {
	s := startBroker(t, t.TempDir())
	conn := dialBroker(t, s)
	mustCreateTopic(t, conn, "once", 1)
	expectError(t, conn, wire.TypeCreateTopic,
		wire.CreateTopic{Topic: "once", Partitions: 1}.Encode(),
		wire.CodeTopicExists)
}
