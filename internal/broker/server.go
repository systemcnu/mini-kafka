// TCP server: listener with the DD-24 connection cap, goroutine-per-conn
// frame loop, dispatch, and the D-SL0-6 graceful-stop sequence.
package broker

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/wire"
)

// Defaults (DD-24, D-SL0-7/8).
const (
	DefaultAddr     = "127.0.0.1:7621"
	DefaultMaxConns = 256
	drainTimeout    = 5 * time.Second
)

// Config configures a broker. Zero values take the defaults above.
type Config struct {
	Addr     string
	DataDir  string
	MaxConns int
}

// Server is the broker: it owns the storage.Store and the listener. Create
// with New, run with Start, stop (once) with Stop.
type Server struct {
	store    *storage.Store
	addr     string
	maxConns int

	ln         net.Listener
	acceptDone chan struct{}
	stopping   chan struct{} // closed at stop step 3: unparks every fetch
	draining   atomic.Bool
	stopOnce   sync.Once

	connMu sync.Mutex
	conns  map[net.Conn]chan struct{} // conn → its cancel channel

	// inflight counts requests between frame-read and response-write; Stop
	// waits (bounded) for it to reach zero before force-closing conns so
	// released parked fetches and final acks actually reach their clients.
	inflight atomic.Int64

	wg sync.WaitGroup
}

// New opens the data directory (running boot recovery; a refused partition
// aborts loudly here) and prepares a broker. It does not listen yet.
func New(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = DefaultMaxConns
	}
	store, err := storage.Open(cfg.DataDir, storage.OSFS(), storage.FileSyncer{})
	if err != nil {
		return nil, err
	}
	return &Server{
		store:      store,
		addr:       cfg.Addr,
		maxConns:   cfg.MaxConns,
		acceptDone: make(chan struct{}),
		stopping:   make(chan struct{}),
		conns:      make(map[net.Conn]chan struct{}),
	}, nil
}

// Start binds the listener and begins accepting.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	go s.acceptLoop()
	return nil
}

// Addr returns the listener's resolved address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Stop runs the D-SL0-6 graceful-stop sequence, in order: (1) stop
// accepting; (2) draining on — new requests get SHUTTING_DOWN; (3) close the
// stopping channel so parked fetches return their empty-at-timeout shape;
// (4) wait ≤5 s for queued produce acks; (5)(6) join flushers and close
// files; (7) close connections. Idempotent.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.ln.Close()
		<-s.acceptDone
		s.draining.Store(true)
		close(s.stopping)
		s.store.Drain(drainTimeout)
		if err := s.store.Close(); err != nil {
			log.Printf("closing storage: %v", err)
		}
		deadline := time.Now().Add(drainTimeout)
		for time.Now().Before(deadline) && s.inflight.Load() > 0 {
			time.Sleep(time.Millisecond)
		}
		s.connMu.Lock()
		for conn, cancel := range s.conns {
			close(cancel)
			conn.Close()
		}
		s.connMu.Unlock()
		s.wg.Wait()
	})
}

func (s *Server) acceptLoop() {
	defer close(s.acceptDone)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		s.connMu.Lock()
		if len(s.conns) >= s.maxConns {
			s.connMu.Unlock()
			// DD-24 accept-guard: over-cap conns are closed immediately;
			// the served-error-frame polish is SL4's.
			conn.Close()
			continue
		}
		cancel := make(chan struct{})
		s.conns[conn] = cancel
		s.connMu.Unlock()
		s.wg.Add(1)
		go s.serveConn(conn, cancel)
	}
}

// dropConn removes conn from the registry on EVERY exit path — a missed
// decrement would wedge the cap after 256 total connections ever.
func (s *Server) dropConn(conn net.Conn) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
	conn.Close()
}

func (s *Server) serveConn(conn net.Conn, cancel <-chan struct{}) {
	defer s.wg.Done()
	defer s.dropConn(conn)
	// No read deadline while waiting for a frame: idle reclaim is SL4's,
	// and a deadline left armed would kill parked fetches (PLAN pitfall).
	for {
		typ, payload, err := wire.ReadFrame(conn, wire.MaxRequestFrame)
		if err != nil {
			var werr *wire.Error
			if errors.As(err, &werr) {
				// Oversized frame or bad version: serve the error, then
				// close — the stream is not trustworthy past this point.
				s.writeError(conn, werr)
			}
			// Partial frame / clean close: just close (debug log only).
			return
		}
		if !s.serveRequest(conn, typ, payload, cancel) {
			return
		}
	}
}

// serveRequest handles one framed request end-to-end; false means drop the
// connection. The inflight window spans dispatch AND response write so a
// graceful stop never closes a conn under a response in transit.
func (s *Server) serveRequest(conn net.Conn, typ byte, payload []byte, cancel <-chan struct{}) bool {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)

	if s.draining.Load() {
		return s.writeError(conn, wire.Errf(wire.CodeShuttingDown, "broker is shutting down"))
	}
	respType, respBody, werr, closeAfter := s.dispatch(typ, payload, cancel)
	if werr != nil {
		if !s.writeError(conn, werr) {
			return false
		}
		// closeAfter with an error: e.g. unknown type (D-SL0-2).
		return !closeAfter
	}
	if closeAfter {
		return false // canceled park or unservable read: drop the conn
	}
	return wire.WriteFrame(conn, respType, respBody) == nil
}

func (s *Server) writeError(conn net.Conn, werr *wire.Error) bool {
	body := wire.ErrorMsg{Code: uint16(werr.Code), Msg: werr.Msg}.Encode()
	return wire.WriteFrame(conn, wire.TypeError, body) == nil
}
