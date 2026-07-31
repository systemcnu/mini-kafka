package main

import (
	_ "embed"
	"net/http"
)

// pageHTML is the D-SL7-5 embedded page: one file, all CSS/JS inline.
//
//go:embed page.html
var pageHTML []byte

// listenAddr is the ledger-6 bind rule. SKELETON (PLAN row 1): inert —
// row 2 lands the two literals.
func listenAddr(port string) string { return "" }

// newMux builds the two-route surface (D-SL7-2, PLAN §H). SKELETON (PLAN
// row 1): routeless — row 2 registers "GET /{$}" and "GET /feed".
func newMux(h *snapshotHolder) *http.ServeMux {
	_ = h
	return http.NewServeMux()
}

// newServer is the production server constructor (PLAN §H). SKELETON (PLAN
// row 1): zero-valued timeouts — row 2 lands the four-timeout literal.
func newServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler}
}
