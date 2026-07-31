package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestConcurrentLoad is §S's proof shape, run under -race with the whole
// battery: the REAL feeder swaps snapshots while 16 goroutines hammer
// /feed and /, fully decoding every feed body (forcing reads of every
// snapshot field). Every response must be 200; then stop() must return
// within a bound with no goroutine left running. A torn snapshot, shared
// map, or unjoined goroutine dies loudly here.
func TestConcurrentLoad(t *testing.T) {
	holder := newSnapshotHolder(1<<30, time.Now())
	f := newFeeder(acceleratedConfig(t), holder)
	if err := f.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer f.stop()

	ts := httptest.NewServer(newMux(holder))
	defer ts.Close()

	const workers, gets = 16, 100
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < gets; i++ {
				path := "/feed"
				if i%2 == 1 {
					path = "/"
				}
				resp, err := http.Get(ts.URL + path)
				if err != nil {
					errs <- fmt.Errorf("worker %d GET %s: %v", w, path, err)
					return
				}
				body, rerr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if rerr != nil {
					errs <- fmt.Errorf("worker %d GET %s: reading body: %v", w, path, rerr)
					return
				}
				if resp.StatusCode != http.StatusOK {
					errs <- fmt.Errorf("worker %d GET %s: status %d (body %.120q) — every response must be 200 under load (§S)", w, path, resp.StatusCode, body)
					return
				}
				if path != "/feed" {
					continue
				}
				var s snapshot
				if err := json.Unmarshal(body, &s); err != nil {
					errs <- fmt.Errorf("worker %d: /feed body undecodable under load: %v", w, err)
					return
				}
				// Touch every field so the race detector sees the reads.
				var sink uint64
				sink += uint64(len(s.Status)) + uint64(s.UptimeSeconds) + s.Produced + s.MemBytes
				sink += uint64(s.DiskBytes) + uint64(s.DiskCapBytes) + uint64(len(s.StartedAt))
				for _, p := range s.Partitions {
					sink += uint64(p.Partition) + p.NextOffset
				}
				for _, r := range s.Recent {
					sink += uint64(r.Partition) + r.Offset + uint64(len(r.Payload))
				}
				for _, a := range s.Assignment {
					sink += uint64(a)
				}
				_ = sink
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Clean stop under load's aftermath: bounded, joining every goroutine.
	done := make(chan struct{})
	go func() {
		f.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("stop() did not return within 20 s after the load — leaked feeder goroutine (§F stop order)")
	}
}
