package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"
)

// unmarshalField decodes one raw feed field into out, failing loudly with
// the field's name on a type mismatch.
func unmarshalField(t *testing.T, name string, raw json.RawMessage, out any) {
	t.Helper()
	if raw == nil {
		t.Fatalf("feed field %q absent — the ten-field contract (§J)", name)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Errorf("feed field %q: wrong type (%v) — §J pins its shape", name, err)
	}
}

// TestFeedShape pins D-SL7-3/§J against an INJECTED snapshot: exactly ten
// fields, each type-checked; assignment present; NO members key (the
// shipped client exposes no member enumeration).
func TestFeedShape(t *testing.T) {
	started := time.Now().UTC().Format(time.RFC3339)
	holder := newSnapshotHolder(200<<20, time.Now())
	holder.store(&snapshot{
		Status:        statusLive,
		UptimeSeconds: 42,
		Produced:      99,
		Partitions:    []partRow{{0, 25}, {1, 25}, {2, 25}, {3, 24}},
		Recent:        []recentRow{{Partition: 2, Offset: 24, Payload: "msg-98"}},
		Assignment:    []uint32{0, 1, 2, 3},
		DiskBytes:     4096,
		DiskCapBytes:  200 << 20,
		MemBytes:      1 << 20,
		StartedAt:     started,
	})
	mux := newMux(holder)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/feed", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /feed: got %d (body %q) — want 200 carrying the ten-field feed contract (§J)", rr.Code, rr.Body.String())
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("GET /feed: body is not a JSON object (%v) — want the ten-field feed contract (§J)", err)
	}

	want := []string{"status", "uptimeSeconds", "produced", "partitions", "recent",
		"assignment", "diskBytes", "diskCapBytes", "memBytes", "startedAt"}
	for _, k := range want {
		if _, ok := m[k]; !ok {
			t.Errorf("feed missing field %q — the ten-field contract (§J)", k)
		}
	}
	if _, ok := m["members"]; ok {
		t.Errorf("feed carries a members field — §J pins NO members key (no member enumeration exists in the shipped client)")
	}
	if len(m) != len(want) {
		got := make([]string, 0, len(m))
		for k := range m {
			got = append(got, k)
		}
		sort.Strings(got)
		t.Errorf("feed has %d fields, want exactly %d — got keys %v (§J: no more, no fewer)", len(m), len(want), got)
	}

	var status string
	unmarshalField(t, "status", m["status"], &status)
	if status != statusLive && status != statusPaused {
		t.Errorf("status = %q, want %q or %q", status, statusLive, statusPaused)
	}
	var uptime int64
	unmarshalField(t, "uptimeSeconds", m["uptimeSeconds"], &uptime)
	if uptime != 42 {
		t.Errorf("uptimeSeconds = %d, want the injected 42 (carried by the snapshot — the handler computes nothing)", uptime)
	}
	var produced uint64
	unmarshalField(t, "produced", m["produced"], &produced)
	if produced != 99 {
		t.Errorf("produced = %d, want the injected 99", produced)
	}
	var parts []partRow
	unmarshalField(t, "partitions", m["partitions"], &parts)
	if len(parts) != feedPartitions {
		t.Errorf("partitions has %d rows, want %d", len(parts), feedPartitions)
	} else {
		for i, p := range parts {
			if p.Partition != uint32(i) {
				t.Errorf("partitions[%d].partition = %d, want %d (sorted)", i, p.Partition, i)
			}
		}
		if parts[3].NextOffset != 24 {
			t.Errorf("partitions[3].nextOffset = %d, want the injected 24", parts[3].NextOffset)
		}
	}
	var recent []recentRow
	unmarshalField(t, "recent", m["recent"], &recent)
	if len(recent) != 1 || recent[0].Payload != "msg-98" || recent[0].Partition != 2 || recent[0].Offset != 24 {
		t.Errorf("recent = %+v, want the injected [{2 24 msg-98}]", recent)
	}
	var assignment []uint32
	unmarshalField(t, "assignment", m["assignment"], &assignment)
	if len(assignment) != 4 {
		t.Errorf("assignment = %v, want the injected [0 1 2 3]", assignment)
	}
	var diskBytes, diskCapBytes int64
	unmarshalField(t, "diskBytes", m["diskBytes"], &diskBytes)
	unmarshalField(t, "diskCapBytes", m["diskCapBytes"], &diskCapBytes)
	if diskBytes != 4096 || diskCapBytes != 200<<20 {
		t.Errorf("diskBytes/diskCapBytes = %d/%d, want the injected 4096/%d", diskBytes, diskCapBytes, int64(200<<20))
	}
	var memBytes uint64
	unmarshalField(t, "memBytes", m["memBytes"], &memBytes)
	if memBytes != 1<<20 {
		t.Errorf("memBytes = %d, want the injected %d", memBytes, uint64(1<<20))
	}
	var startedAt string
	unmarshalField(t, "startedAt", m["startedAt"], &startedAt)
	if _, err := time.Parse(time.RFC3339, startedAt); err != nil {
		t.Errorf("startedAt = %q does not parse as RFC3339: %v", startedAt, err)
	}
}
