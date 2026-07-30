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

	"github.com/systemcnu/mini-kafka/internal/group"
	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/wire"
)

// Defaults (DD-24, D-SL0-7/8; DefaultIdleTimeout is D-SL4-3's).
const (
	DefaultAddr        = "127.0.0.1:7621"
	DefaultMaxConns    = 256
	DefaultIdleTimeout = 5 * time.Minute
	drainTimeout       = 5 * time.Second
)

// Config configures a broker. Zero values take the defaults above.
type Config struct {
	Addr     string
	DataDir  string
	MaxConns int
	// IdleTimeout is DD-24's idle-reclaim window (D-SL4-3): a connection
	// with no complete request for this long is closed, silently. 0 → 5 min.
	// A duration, not a Clock seam: net.Conn deadlines run on wall clock.
	IdleTimeout time.Duration
}

// Server is the broker: it owns the storage.Store, the group coordinator,
// and the listener. Create with New, run with Start, stop (once) with Stop.
type Server struct {
	store       *storage.Store
	coord       *group.Coordinator
	addr        string
	maxConns    int
	idleTimeout time.Duration

	ln         net.Listener
	acceptDone chan struct{}
	stopping   chan struct{} // closed at stop step 3: unparks every fetch
	draining   atomic.Bool
	stopOnce   sync.Once

	connMu sync.Mutex
	conns  map[net.Conn]chan struct{} // conn → its cancel channel
	// connSeq numbers connections for the coordinator's conn↔member binding
	// (D-SL2-11): teardown reports the id, never the net.Conn.
	connSeq atomic.Uint64

	// inflight counts requests between frame-read and response-write; Stop
	// waits (bounded) for it to reach zero before force-closing conns so
	// released parked fetches and final acks actually reach their clients.
	inflight atomic.Int64

	wg sync.WaitGroup
}

// New opens the data directory (running boot recovery; a refused partition
// aborts loudly here) and prepares a broker. It does not listen yet.
func New(cfg Config) (*Server, error) {
	return newWithFS(cfg, storage.OSFS(), storage.FileSyncer{})
}

// newWithFS is the D-SL1-4 seam: New over an injectable FS/Syncer so tests
// can script storage faults through a real listening broker. Test-constructor
// only — no flag, no runtime configurability.
func newWithFS(cfg Config, fsys storage.FS, syncer storage.Syncer) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = DefaultMaxConns
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	store, err := storage.Open(cfg.DataDir, fsys, syncer)
	if err != nil {
		return nil, err
	}
	coord, err := group.New(group.Config{}, cfg.DataDir, fsys)
	if err != nil {
		store.Close()
		return nil, err
	}
	coord.Run()
	return &Server{
		store:       store,
		coord:       coord,
		addr:        cfg.Addr,
		maxConns:    cfg.MaxConns,
		idleTimeout: cfg.IdleTimeout,
		acceptDone:  make(chan struct{}),
		stopping:    make(chan struct{}),
		conns:       make(map[net.Conn]chan struct{}),
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
		s.coord.Stop() // sweeper off; group requests already get SHUTTING_DOWN via draining
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
			// DD-24's accept → write error → close (D-SL4-2). The writer
			// goroutine + 1 s write deadline keep a client that never reads
			// from wedging the accept loop; wg.Add BEFORE go, so Stop cannot
			// return with a live writer holding the fd (ordering is safe:
			// Stop waits on acceptDone before wg.Wait). The conn never
			// enters s.conns — closing it on every path is the writer's job.
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer conn.Close()
				conn.SetWriteDeadline(time.Now().Add(time.Second))
				s.writeError(conn, wire.Errf(wire.CodeCapExceeded, "connection cap %d reached", s.maxConns))
			}()
			continue
		}
		cancel := make(chan struct{})
		s.conns[conn] = cancel
		s.connMu.Unlock()
		s.wg.Add(1)
		go s.serveConn(conn, cancel, s.connSeq.Add(1))
	}
}

// dropConn removes conn from the registry on EVERY exit path — a missed
// decrement would wedge the cap after 256 total connections ever — and
// reports the drop to the coordinator: control-conn drop is immediate
// member death (DD-10, D-SL2-11). ConnClosed takes the coordinator mutex,
// so it runs strictly after connMu is released.
func (s *Server) dropConn(conn net.Conn, connID uint64) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
	conn.Close()
	s.coord.ConnClosed(connID)
}

func (s *Server) serveConn(conn net.Conn, cancel <-chan struct{}, connID uint64) {
	defer s.wg.Done()
	defer s.dropConn(conn, connID)
	for {
		// Idle reclaim (D-SL4-3): armed at the TOP of every iteration, never
		// during dispatch — a park happens AFTER the frame is read and does
		// no conn reads, so the stale absolute deadline is harmless and
		// replaced here. Expiry surfaces as a plain net.Error (never a
		// *wire.Error) and lands in the silent-close branch below: the peer
		// is absent by definition, a farewell frame can stall (G-SL4-1).
		conn.SetReadDeadline(time.Now().Add(s.idleTimeout))
		typ, payload, err := wire.ReadFrame(conn, wire.MaxRequestFrame)
		if err != nil {
			var werr *wire.Error
			if errors.As(err, &werr) {
				// Oversized frame or bad version: serve the error, then
				// close — the stream is not trustworthy past this point.
				conn.SetWriteDeadline(time.Now().Add(s.idleTimeout))
				s.writeError(conn, werr)
			}
			// Partial frame / clean close / idle expiry: just close.
			return
		}
		if !s.serveRequest(conn, typ, payload, cancel, connID) {
			return
		}
	}
}

// serveRequest handles one framed request end-to-end; false means drop the
// connection. The inflight window spans dispatch AND response write so a
// graceful stop never closes a conn under a response in transit.
func (s *Server) serveRequest(conn net.Conn, typ byte, payload []byte, cancel <-chan struct{}, connID uint64) bool {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)

	if s.draining.Load() {
		conn.SetWriteDeadline(time.Now().Add(s.idleTimeout))
		return s.writeError(conn, wire.Errf(wire.CodeShuttingDown, "broker is shutting down"))
	}
	respType, respBody, werr, closeAfter := s.dispatch(typ, payload, cancel, connID)
	// Response writes get their own idle window (D-SL4-3): a client that
	// won't drain its response is the same leaked conn idle reclaim exists
	// to evict. Armed AFTER dispatch — a legal park (≤30 s) may outlive
	// IdleTimeout, so the clock starts at the write, never before it.
	conn.SetWriteDeadline(time.Now().Add(s.idleTimeout))
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
