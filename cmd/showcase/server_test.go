package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testHolder is a zero-snapshot holder (no feeder running), so responses
// are deterministic within a test.
func testHolder(t *testing.T) *snapshotHolder {
	t.Helper()
	return newSnapshotHolder(200<<20, time.Now())
}

// TestRoutes pins D-SL7-2's write-path enumeration: exactly two GET
// routes; non-GET → 405; unknown path → 404 (provable only because the
// page pattern is "GET /{$}", never a bare "/"); /feed carries JSON +
// CORS; a query string influences nothing. HEAD is deliberately NOT
// asserted → 405: the stdlib mux matches it with GET (PLAN Pitfalls).
func TestRoutes(t *testing.T) {
	mux := newMux(testHolder(t))
	get := func(path string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr
	}

	feed := get("/feed")
	if feed.Code != http.StatusOK {
		t.Fatalf("GET /feed: got %d, want 200 with the JSON feed", feed.Code)
	}
	if ct := feed.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("GET /feed Content-Type = %q, want application/json", ct)
	}
	if cors := feed.Header().Get("Access-Control-Allow-Origin"); cors != "*" {
		t.Errorf("GET /feed Access-Control-Allow-Origin = %q, want * (ledger 2 — the Pages shim polls cross-origin)", cors)
	}

	page := get("/")
	if page.Code != http.StatusOK {
		t.Errorf("GET /: got %d, want 200 with the embedded page", page.Code)
	}
	if ct := page.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("GET / Content-Type = %q, want text/html", ct)
	}
	if page.Body.Len() == 0 {
		t.Errorf("GET /: empty body, want the embedded page")
	}

	if rr := get("/nope"); rr.Code != http.StatusNotFound {
		t.Errorf(`GET /nope: got %d, want 404 (the "GET /{$}" pattern, never a catch-all "/")`, rr.Code)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		for _, path := range []string{"/", "/feed"} {
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
			if rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: got %d, want 405 (GET-only surface, D-SL7-2)", method, path, rr.Code)
			}
		}
	}

	// No request influence: a query-string GET returns the identical body
	// (zero snapshot, no feeder — the bytes must match exactly).
	withQ := get("/feed?x=1&debug=true")
	if withQ.Code != http.StatusOK {
		t.Errorf("GET /feed?x=1&debug=true: got %d, want 200", withQ.Code)
	} else if withQ.Body.String() != feed.Body.String() {
		t.Errorf("GET /feed with a query string returned a DIFFERENT body — no query parameter may influence the response (D-SL7-2)")
	}
}

// TestBind pins the ledger-6 bind rule's two literals (pure function).
func TestBind(t *testing.T) {
	if got := listenAddr(""); got != "127.0.0.1:8080" {
		t.Errorf(`listenAddr(""): got %q, want "127.0.0.1:8080" (local runs stay loopback, NFR-4)`, got)
	}
	if got := listenAddr("10000"); got != "0.0.0.0:10000" {
		t.Errorf(`listenAddr("10000"): got %q, want "0.0.0.0:10000" (hosted bind only under the platform's PORT signal)`, got)
	}
}

// TestTimeouts pins §H's four non-zero timeouts on the SAME constructor
// production uses — a zero-valued http.Server is Slowloris-open.
func TestTimeouts(t *testing.T) {
	srv := newServer(listenAddr(""), newMux(testHolder(t)))
	for _, c := range []struct {
		name string
		d    time.Duration
	}{
		{"ReadHeaderTimeout", srv.ReadHeaderTimeout},
		{"ReadTimeout", srv.ReadTimeout},
		{"WriteTimeout", srv.WriteTimeout},
		{"IdleTimeout", srv.IdleTimeout},
	} {
		if c.d <= 0 {
			t.Errorf("http.Server.%s = %v — all four timeouts must be non-zero (§H)", c.name, c.d)
		}
	}
}
