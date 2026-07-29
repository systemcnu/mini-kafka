// Producer, Consumer, GroupConsumer, and Admin (DD-19), plus the
// re-exported error code registry (DD-1). Raw Producer/Consumer/Admin
// conns carry no deadlines (a fetch may legally park server-side up to its
// maxWait); the GroupConsumer wraps its wire I/O — and only its wire I/O —
// in deadlines per D-SL2-8.
package client

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/systemcnu/mini-kafka/internal/wire"
)

// DefaultAddr mirrors the broker's default loopback listen address (DD-24).
const DefaultAddr = "127.0.0.1:7621"

// Protocol error codes, re-exported so importers never touch internal/wire.
const (
	CodeUnknownTopic    uint16 = 1
	CodeTopicExists     uint16 = 2
	CodeBadPartition    uint16 = 3
	CodeInvalidName     uint16 = 4
	CodeMsgTooLarge     uint16 = 5
	CodeFrameTooLarge   uint16 = 6
	CodeMalformed       uint16 = 7
	CodeCapExceeded     uint16 = 8
	CodeFetchTooWide    uint16 = 9
	CodeShuttingDown    uint16 = 10
	CodeWriteFailed     uint16 = 11
	CodeStaleGeneration uint16 = 12
	CodeUnknownMember   uint16 = 13
)

// Error is a broker-served protocol error.
type Error struct {
	Code uint16
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("broker error %d: %s", e.Code, e.Msg) }

// Record is one fetched record.
type Record struct {
	Offset  uint64
	Payload []byte
}

// TopicInfo is one row of a topic listing.
type TopicInfo struct {
	Name       string
	Partitions uint32
}

// conn is the shared synchronous request/response core: one request in
// flight per connection (DD-15).
type conn struct {
	c net.Conn
}

func dial(addr string) (*conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &conn{c: c}, nil
}

func (cn *conn) roundtrip(reqType byte, body []byte, wantType byte) ([]byte, error) {
	if err := wire.WriteFrame(cn.c, reqType, body); err != nil {
		return nil, err
	}
	respType, respBody, err := wire.ReadFrame(cn.c, wire.MaxResponseFrame)
	if err != nil {
		return nil, err
	}
	if respType == wire.TypeError {
		em, werr := wire.DecodeErrorMsg(respBody)
		if werr != nil {
			return nil, fmt.Errorf("undecodable error frame: %v", werr)
		}
		return nil, &Error{Code: em.Code, Msg: em.Msg}
	}
	if respType != wantType {
		return nil, fmt.Errorf("unexpected response type %d, want %d", respType, wantType)
	}
	return respBody, nil
}

func (cn *conn) close() error { return cn.c.Close() }

// Producer produces synchronously: each Produce blocks until the broker's
// durable ack and returns the assigned offset.
type Producer struct {
	cn *conn
}

// DialProducer connects a Producer to a broker.
func DialProducer(addr string) (*Producer, error) {
	cn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return &Producer{cn: cn}, nil
}

// Produce appends payload to (topic, partition) and returns its offset.
func (p *Producer) Produce(topic string, partition uint32, payload []byte) (uint64, error) {
	body, err := p.cn.roundtrip(wire.TypeProduce,
		wire.Produce{Topic: topic, Partition: partition, Payload: payload}.Encode(),
		wire.TypeProduceResp)
	if err != nil {
		return 0, err
	}
	resp, werr := wire.DecodeProduceResp(body)
	if werr != nil {
		return 0, fmt.Errorf("bad produce response: %v", werr)
	}
	return resp.Offset, nil
}

// Close closes the producer's connection.
func (p *Producer) Close() error { return p.cn.close() }

// Consumer is a raw single-partition fetch loop over one connection.
type Consumer struct {
	cn *conn
}

// DialConsumer connects a Consumer to a broker.
func DialConsumer(addr string) (*Consumer, error) {
	cn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return &Consumer{cn: cn}, nil
}

// Fetch returns records from (topic, partition) starting at offset. It
// blocks up to the broker-side maxWait (0 → broker default 5 s) when no
// durable data is available; an empty result is the at-tail shape.
// maxBytes 0 takes the broker default (1 MiB).
func (c *Consumer) Fetch(topic string, partition uint32, offset uint64, maxWaitMs, maxBytes uint32) ([]Record, error) {
	req := wire.Fetch{
		Topic:     topic,
		Entries:   []wire.FetchEntry{{Partition: partition, Offset: offset}},
		MaxWaitMs: maxWaitMs,
		MaxBytes:  maxBytes,
	}
	body, err := c.cn.roundtrip(wire.TypeFetch, req.Encode(), wire.TypeFetchResp)
	if err != nil {
		return nil, err
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		return nil, fmt.Errorf("bad fetch response: %v", werr)
	}
	if len(resp.Groups) != 1 || resp.Groups[0].Partition != partition {
		return nil, fmt.Errorf("fetch response has %d groups", len(resp.Groups))
	}
	recs := make([]Record, 0, len(resp.Groups[0].Recs))
	for _, r := range resp.Groups[0].Recs {
		recs = append(recs, Record{Offset: r.Offset, Payload: r.Payload})
	}
	return recs, nil
}

// Close closes the consumer's connection.
func (c *Consumer) Close() error { return c.cn.close() }

// GroupConsumer timing (D-SL2-8). The heartbeat cadence matches the
// broker's DD-10 contract; the roundtrip timeout and fetch grace only
// bound wire I/O — never a server-side park.
const (
	heartbeatInterval   = 500 * time.Millisecond
	ctlRoundtripTimeout = 5 * time.Second
	fetchGrace          = 2 * time.Second
	defaultPollWait     = 5 * time.Second // mirrors the broker's default maxWait
)

// PartRecord is one record served through a GroupConsumer poll.
type PartRecord struct {
	Partition uint32
	Offset    uint64
	Payload   []byte
}

// GroupConsumer is a group member over two connections (DD-19): a control
// conn (join, heartbeat every 500 ms, commit, leave) and a fetch conn
// carrying multi-partition GroupFetches for all owned partitions.
//
// Rejoin policy (D-SL2-8, pinned): the heartbeat goroutine only RECORDS a
// rejoin-needed condition — it never re-joins (background auto-heal would
// race the fenced operation a test or user must observe). Poll re-joins
// lazily and re-joins + reissues when fenced mid-poll; a fenced Commit
// SURFACES its 12/13 to the caller, and the next Poll re-joins.
//
// Poll and Commit are individually safe for concurrent use; the control
// mutex serializes whole roundtrips (one request in flight per conn).
type GroupConsumer struct {
	addr  string
	group string
	topic string

	ctl   *conn
	ctlMu sync.Mutex // covers a WHOLE control roundtrip: write+read (DD-15)

	fetchConn *conn
	fetchMu   sync.Mutex // serializes Poll

	mu           sync.Mutex // guards identity, cursors, rejoinNeeded
	memberID     string
	generation   uint64
	cursors      map[uint32]uint64 // partition → next fetch offset
	rejoinNeeded bool

	hbStop    chan struct{}
	hbDone    chan struct{}
	closeOnce sync.Once
}

// JoinGroup joins group (bound to topic) at addr and starts the heartbeat
// loop. The returned consumer resumes every owned partition from the
// group's committed offsets (DD-14).
func JoinGroup(addr, group, topic string) (*GroupConsumer, error) {
	ctl, err := dial(addr)
	if err != nil {
		return nil, err
	}
	fc, err := dial(addr)
	if err != nil {
		ctl.close()
		return nil, err
	}
	g := &GroupConsumer{
		addr: addr, group: group, topic: topic,
		ctl: ctl, fetchConn: fc,
		hbStop: make(chan struct{}), hbDone: make(chan struct{}),
	}
	if err := g.rejoin(); err != nil {
		ctl.close()
		fc.close()
		return nil, err
	}
	go g.heartbeatLoop()
	return g, nil
}

// rejoin runs a JoinGroup roundtrip on the control conn and adopts the
// returned identity, generation, and cursors. The broker treats a join on
// a conn bound to a live member as that member's re-Join (D-SL2-3b), so
// this is safe to call repeatedly.
func (g *GroupConsumer) rejoin() error {
	g.ctlMu.Lock()
	body, err := g.ctlRoundtripLocked(wire.TypeJoinGroup,
		wire.JoinGroup{Group: g.group, Topic: g.topic}.Encode(), wire.TypeJoinGroupResp)
	g.ctlMu.Unlock()
	if err != nil {
		return err
	}
	resp, werr := wire.DecodeJoinGroupResp(body)
	if werr != nil {
		return fmt.Errorf("bad join response: %v", werr)
	}
	g.mu.Lock()
	g.memberID = resp.MemberID
	g.generation = resp.Generation
	g.cursors = make(map[uint32]uint64, len(resp.Assigned))
	for _, a := range resp.Assigned {
		g.cursors[a.Partition] = a.NextOffset
	}
	g.rejoinNeeded = false
	g.mu.Unlock()
	return nil
}

// ctlRoundtripLocked runs one control roundtrip under a deadline that
// wraps only the wire I/O (join/heartbeat/commit/leave never park
// server-side). Caller holds ctlMu.
func (g *GroupConsumer) ctlRoundtripLocked(reqType byte, body []byte, wantType byte) ([]byte, error) {
	g.ctl.c.SetDeadline(time.Now().Add(ctlRoundtripTimeout))
	defer g.ctl.c.SetDeadline(time.Time{})
	return g.ctl.roundtrip(reqType, body, wantType)
}

func (g *GroupConsumer) heartbeatLoop() {
	defer close(g.hbDone)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.heartbeat()
		case <-g.hbStop:
			return
		}
	}
}

// heartbeat sends one Heartbeat and RECORDS any rejoin-needed condition —
// the REJOIN bit, a 13, or an I/O failure. It never re-joins (D-SL2-8/F6).
func (g *GroupConsumer) heartbeat() {
	g.mu.Lock()
	memberID, generation := g.memberID, g.generation
	g.mu.Unlock()
	g.ctlMu.Lock()
	body, err := g.ctlRoundtripLocked(wire.TypeHeartbeat,
		wire.Heartbeat{Group: g.group, MemberID: memberID, Generation: generation}.Encode(),
		wire.TypeHeartbeatResp)
	g.ctlMu.Unlock()
	if err != nil {
		g.setRejoinNeeded()
		return
	}
	resp, werr := wire.DecodeHeartbeatResp(body)
	if werr != nil || resp.Flags&wire.HeartbeatRejoin != 0 {
		g.setRejoinNeeded()
	}
}

func (g *GroupConsumer) setRejoinNeeded() {
	g.mu.Lock()
	g.rejoinNeeded = true
	g.mu.Unlock()
}

func (g *GroupConsumer) needsRejoin() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rejoinNeeded
}

// Poll fetches from every owned partition (one GroupFetch on the fetch
// conn) and advances the in-memory cursors; an empty result is the
// at-tail shape. It re-joins lazily when a rejoin-needed condition was
// recorded, and re-joins + reissues internally when fenced mid-poll —
// fetch data is not a caller-visible promise the way an acked commit is
// (D-SL2-8). maxWait 0 takes the broker default (5 s).
func (g *GroupConsumer) Poll(maxWait time.Duration) ([]PartRecord, error) {
	g.fetchMu.Lock()
	defer g.fetchMu.Unlock()
	effWait := maxWait
	if effWait == 0 {
		effWait = defaultPollWait
	}
	fenced := 0
	for {
		if g.needsRejoin() {
			if err := g.rejoin(); err != nil {
				return nil, err
			}
		}
		g.mu.Lock()
		memberID, generation := g.memberID, g.generation
		parts := make([]uint32, 0, len(g.cursors))
		for p := range g.cursors {
			parts = append(parts, p)
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		entries := make([]wire.FetchEntry, 0, len(parts))
		for _, p := range parts {
			entries = append(entries, wire.FetchEntry{Partition: p, Offset: g.cursors[p]})
		}
		g.mu.Unlock()

		if len(entries) == 0 {
			// A member can legally own zero partitions (more members than
			// partitions): idle out the wait, watching for a rebalance.
			deadline := time.Now().Add(effWait)
			for time.Now().Before(deadline) && !g.needsRejoin() {
				time.Sleep(50 * time.Millisecond)
			}
			if g.needsRejoin() {
				continue
			}
			return nil, nil
		}

		req := wire.GroupFetch{Group: g.group, MemberID: memberID, Generation: generation,
			Entries: entries, MaxWaitMs: uint32(maxWait / time.Millisecond)}
		// The read deadline bounds wire I/O only: server-side the fetch may
		// park up to effWait, so the conn is fatal past effWait + grace.
		g.fetchConn.c.SetReadDeadline(time.Now().Add(effWait + fetchGrace))
		body, err := g.fetchConn.roundtrip(wire.TypeGroupFetch, req.Encode(), wire.TypeFetchResp)
		g.fetchConn.c.SetReadDeadline(time.Time{})
		if err != nil {
			var cerr *Error
			if errors.As(err, &cerr) && (cerr.Code == CodeStaleGeneration || cerr.Code == CodeUnknownMember) {
				// Fenced mid-poll (e.g. a parked fetch woke across a
				// rebalance): re-join and reissue.
				fenced++
				if fenced > 8 {
					return nil, fmt.Errorf("poll fenced %d times in a row: %w", fenced, err)
				}
				g.setRejoinNeeded()
				continue
			}
			var nerr net.Error
			if errors.As(err, &nerr) && nerr.Timeout() {
				// Read-deadline expiry is conn-fatal: redial + rejoin +
				// reissue (D-SL2-8).
				g.fetchConn.close()
				fc, derr := dial(g.addr)
				if derr != nil {
					return nil, derr
				}
				g.fetchConn = fc
				g.setRejoinNeeded()
				continue
			}
			return nil, err
		}
		resp, werr := wire.DecodeFetchResp(body)
		if werr != nil {
			return nil, fmt.Errorf("bad group fetch response: %v", werr)
		}
		var out []PartRecord
		g.mu.Lock()
		for _, grp := range resp.Groups {
			for _, r := range grp.Recs {
				out = append(out, PartRecord{Partition: grp.Partition, Offset: r.Offset, Payload: r.Payload})
				if next := r.Offset + 1; next > g.cursors[grp.Partition] {
					g.cursors[grp.Partition] = next
				}
			}
		}
		g.mu.Unlock()
		return out, nil
	}
}

// Commit commits the current cursors (next-to-read, SPEC §1b) for every
// owned partition over the control conn. It never re-joins first: a fenced
// Commit SURFACES its 12/13 to the caller — the pinned public contract
// (D-SL2-8) — and the next Poll re-joins. A nil return means the broker
// acked, which means the positions are durable (CONS-3).
func (g *GroupConsumer) Commit() error {
	g.mu.Lock()
	m := wire.CommitOffsets{Group: g.group, MemberID: g.memberID, Generation: g.generation}
	parts := make([]uint32, 0, len(g.cursors))
	for p := range g.cursors {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	for _, p := range parts {
		m.Entries = append(m.Entries, wire.CommitEntry{Partition: p, Next: g.cursors[p]})
	}
	g.mu.Unlock()
	if len(m.Entries) == 0 {
		return nil
	}
	g.ctlMu.Lock()
	_, err := g.ctlRoundtripLocked(wire.TypeCommitOffsets, m.Encode(), wire.TypeCommitOffsetsResp)
	g.ctlMu.Unlock()
	if err != nil {
		var cerr *Error
		if errors.As(err, &cerr) && (cerr.Code == CodeStaleGeneration || cerr.Code == CodeUnknownMember) {
			g.setRejoinNeeded()
		}
		return err
	}
	return nil
}

// Close stops the heartbeat loop, leaves the group (best effort — a swept
// member legally gets 13 here), and closes both connections.
func (g *GroupConsumer) Close() error {
	g.closeOnce.Do(func() {
		close(g.hbStop)
		<-g.hbDone
		g.mu.Lock()
		memberID := g.memberID
		g.mu.Unlock()
		g.ctlMu.Lock()
		_, _ = g.ctlRoundtripLocked(wire.TypeLeaveGroup,
			wire.LeaveGroup{Group: g.group, MemberID: memberID}.Encode(), wire.TypeLeaveGroupResp)
		g.ctlMu.Unlock()
		g.ctl.close()
		g.fetchConn.close()
	})
	return nil
}

// Admin covers topic administration: create and list.
type Admin struct {
	cn *conn
}

// DialAdmin connects an Admin to a broker.
func DialAdmin(addr string) (*Admin, error) {
	cn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return &Admin{cn: cn}, nil
}

// CreateTopic creates topic with the given partition count (1..16).
func (a *Admin) CreateTopic(topic string, partitions uint32) error {
	_, err := a.cn.roundtrip(wire.TypeCreateTopic,
		wire.CreateTopic{Topic: topic, Partitions: partitions}.Encode(),
		wire.TypeCreateTopicResp)
	return err
}

// Topics lists live topics, sorted by name.
func (a *Admin) Topics() ([]TopicInfo, error) {
	body, err := a.cn.roundtrip(wire.TypeListTopics, nil, wire.TypeListTopicsResp)
	if err != nil {
		return nil, err
	}
	resp, werr := wire.DecodeListTopicsResp(body)
	if werr != nil {
		return nil, fmt.Errorf("bad list response: %v", werr)
	}
	out := make([]TopicInfo, 0, len(resp.Topics))
	for _, t := range resp.Topics {
		out = append(out, TopicInfo{Name: t.Name, Partitions: t.Partitions})
	}
	return out, nil
}

// Close closes the admin's connection.
func (a *Admin) Close() error { return a.cn.close() }
