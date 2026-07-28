// Wire-level LOG-5/PROT-3 proof over the D-SL1-4 seam: a FLUSH-path storage
// fault surfaces as code 11 (WRITE_FAILED) on produce, stays sticky, and the
// same broker keeps serving fetches.
package broker

import (
	"syscall"
	"testing"

	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/storage/storagetest"
	"github.com/systemcnu/mini-kafka/internal/wire"
)

// startFaultBroker is startBroker over the injectable seam.
func startFaultBroker(t *testing.T, dataDir string, fsys storage.FS) *Server {
	t.Helper()
	s, err := newWithFS(Config{Addr: "127.0.0.1:0", DataDir: dataDir}, fsys, storage.FileSyncer{})
	if err != nil {
		t.Fatalf("newWithFS: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(s.Stop)
	return s
}

func TestProduceOnFaultedStoreGetsWriteFailedAndFetchStillServes(t *testing.T) {
	ffs := storagetest.WrapFS(storage.OSFS())
	s := startFaultBroker(t, t.TempDir(), ffs)
	conn := dialBroker(t, s)

	mustCreateTopic(t, conn, "wf", 1)
	mustProduce(t, conn, "wf", 0, "durable-1")
	mustProduce(t, conn, "wf", 0, "durable-2")

	// Script the FLUSH path (the broker maps ANY storage error on produce to
	// code 11, so tripping a validation error would pass for the wrong
	// reason): the next File.Write on the partition log fails.
	ffs.FailWrite("log", 1, 0, syscall.ENOSPC)
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "wf", Partition: 0, Payload: []byte("doomed")}.Encode(),
		wire.CodeWriteFailed)

	// Sticky until restart (LOG-5): the fault script is spent, yet the
	// partition still rejects.
	expectError(t, conn, wire.TypeProduce,
		wire.Produce{Topic: "wf", Partition: 0, Payload: []byte("still-doomed")}.Encode(),
		wire.CodeWriteFailed)

	// The SAME broker serves every durable record throughout.
	recs := mustFetch(t, conn, "wf", 0, 0, 1000)
	if len(recs) != 2 {
		t.Fatalf("fetched %d records on the degraded broker, want 2", len(recs))
	}
	for i, want := range []string{"durable-1", "durable-2"} {
		if recs[i].Offset != uint64(i) || string(recs[i].Payload) != want {
			t.Errorf("rec %d = %q@%d, want %q@%d", i, recs[i].Payload, recs[i].Offset, want, i)
		}
	}
}
