package main

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"time"
)

// pageHTML is the D-SL7-5 embedded page: one file, all CSS/JS inline.
//
//go:embed page.html
var pageHTML []byte

// listenAddr is the ledger-6 bind rule: PORT set (Render's own contract —
// the platform's signal, never ours) → all interfaces on that port; unset
// → loopback, so a local `go run` keeps the repo's loopback-default
// posture (NFR-4).
func listenAddr(port string) string {
	if port == "" {
		return "127.0.0.1:8080"
	}
	return "0.0.0.0:" + port
}

// newMux builds the two-route surface (D-SL7-2, PLAN §H): Go 1.22+
// method+exact patterns — the method makes non-GET → 405 and the "{$}"
// makes unknown paths → 404, both by the stdlib mux. NEVER a bare "/"
// (the catch-all trap). Neither handler reads a body or a query
// parameter: a request can influence nothing but its own response.
func newMux(h *snapshotHolder) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(pageHTML)
	})
	mux.HandleFunc("GET /feed", func(w http.ResponseWriter, r *http.Request) {
		// §S discipline: load + marshal — the handler computes NOTHING
		// and never touches feeder state.
		body, err := json.Marshal(h.load())
		if err != nil {
			http.Error(w, "encoding snapshot", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write(body)
	})
	return mux
}

// newServer is the production server constructor: the §H literal with all
// four timeouts non-zero — a zero-valued http.Server has NO timeouts and
// a slow-header flood would pin the free instance's connections.
func newServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
