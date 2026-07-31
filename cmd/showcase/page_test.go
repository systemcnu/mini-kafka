package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestPageServed: the embedded page comes back on GET / — 200, HTML,
// non-empty, naming the project (D-SL7-5).
func TestPageServed(t *testing.T) {
	mux := newMux(newSnapshotHolder(200<<20, time.Now()))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /: got %d, want 200 with the embedded page", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("GET / Content-Type = %q, want text/html", ct)
	}
	if rr.Body.Len() == 0 {
		t.Errorf("GET /: empty body")
	}
	if !strings.Contains(rr.Body.String(), "mini-kafka") {
		t.Errorf("served page never names mini-kafka")
	}
}

// TestPageStates: all four §P states and the watch-only copy are present
// in the embedded source.
func TestPageStates(t *testing.T) {
	page := string(pageHTML)
	for _, marker := range []string{
		`"connecting"`,     // first load, before the first /feed succeeds
		`"live"`,           // records flowing
		`"paused-at-cap"`,  // the honest banner state
		"feed unreachable", // the narrated restart state
		"restart",          // ...naming free-instance restarts out loud
		"self-driving",     // watch-only copy
		"no way to write",  // watch-only copy
	} {
		if !strings.Contains(page, marker) {
			t.Errorf("page.html missing marker %q — the four states + watch-only copy, §P", marker)
		}
	}
}

// TestPageConstant: the 10 s poll constant (DD-23's number — also the
// keep-awake signal) appears literally.
func TestPageConstant(t *testing.T) {
	if !strings.Contains(string(pageHTML), "10000") {
		t.Errorf("page.html missing the 10 s poll constant 10000 (DD-23, §P)")
	}
}

// TestPageNoExternalAssets: inline-everything — no external hosts; the
// ONLY permitted absolute URL is the repo link (§P).
func TestPageNoExternalAssets(t *testing.T) {
	page := string(pageHTML)
	if strings.Contains(page, `src="http`) {
		t.Errorf(`page.html contains src="http — external asset; the page must be inline-everything (§P)`)
	}
	stripped := strings.ReplaceAll(page, "https://github.com/systemcnu/mini-kafka", "")
	for _, scheme := range []string{"http://", "https://"} {
		if strings.Contains(stripped, scheme) {
			t.Errorf("page.html references %s beyond the repo link — no external hosts (§P)", scheme)
		}
	}
}
