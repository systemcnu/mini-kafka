// Command showcase is DD-23's watch-only live page (D-SL7-1..5): one
// process hosting the broker in-process on an ephemeral loopback port, a
// self-feeder driving it through the public client surface (producer +
// group consumer, topic showcase×4, ~2 msg/s), and a two-route HTTP
// server — "GET /{$}" (the embedded page) and "GET /feed" (read-only
// JSON). Env-only wiring: PORT (set → 0.0.0.0:$PORT, unset →
// 127.0.0.1:8080) and SHOWCASE_DISK_CAP_MB (default 200). No flags by
// construction.
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// capBytesFromEnv parses SHOWCASE_DISK_CAP_MB (whole MiB); unset,
// garbage, non-positive, or absurd (> 1 TiB) take the 200 MiB default
// (D-SL7-4).
func capBytesFromEnv(v string) int64 {
	mb, err := strconv.ParseInt(v, 10, 64)
	if err != nil || mb <= 0 || mb > 1<<20 {
		mb = defaultCapMiB
	}
	return mb << 20
}

func main() {
	capBytes := capBytesFromEnv(os.Getenv("SHOWCASE_DISK_CAP_MB"))
	holder := newSnapshotHolder(capBytes, time.Now())
	f := newFeeder(feederConfig{capBytes: capBytes}, holder)
	if err := f.start(); err != nil {
		fmt.Fprintf(os.Stderr, "showcase: %v\n", err)
		os.Exit(1)
	}
	// The data dir is deliberately NOT removed on exit (no signal-handler
	// ceremony): restart-fresh is the design (D-SL7-4) — every boot gets a
	// NEW MkdirTemp dir, and Render's ephemeral disk resets it anyway.
	srv := newServer(listenAddr(os.Getenv("PORT")), newMux(holder))
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "showcase: %v\n", err)
		os.Exit(1)
	}
}
