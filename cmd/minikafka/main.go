// Command minikafka is the broker: it opens the data directory (running
// boot recovery), listens on a loopback address by default, and runs the
// graceful-stop sequence on SIGINT/SIGTERM (D-SL0-6/7).
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/systemcnu/mini-kafka/internal/broker"
)

func main() {
	addr := flag.String("addr", broker.DefaultAddr, "listen address; binding beyond loopback requires setting this explicitly (NFR-4)")
	data := flag.String("data", "./data", "data directory")
	flag.Parse()

	srv, err := broker.New(broker.Config{Addr: *addr, DataDir: *data})
	if err != nil {
		// A refused partition lands here: loud, and the broker does not start.
		log.Fatalf("minikafka: %v", err)
	}
	if err := srv.Start(); err != nil {
		log.Fatalf("minikafka: %v", err)
	}
	log.Printf("minikafka listening on %s (data: %s)", srv.Addr(), *data)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("minikafka: signal received, stopping gracefully")
	srv.Stop()
	log.Printf("minikafka: stopped")
}
