// Coordinator state machine (D-SL2-3/3b/6): groups, members, generations,
// range assignment, the liveness sweeper, and the fence funcs deciding 12
// vs 13. All transitions happen under one mutex, never held during I/O or
// parking (DD-25); the clock is injectable so GRP-2's bound is measurable
// without real sleeps (DESIGN §9).
package group

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/systemcnu/mini-kafka/internal/storage"
)

// Defaults (D-SL2-3). Compile-time constants by design — nothing new
// user-tunable without a cap (G-SL2-3, NFR-2).
const (
	DefaultHeartbeatInterval = 500 * time.Millisecond
	DefaultSessionTimeout    = 2 * time.Second
	DefaultSweepInterval     = 100 * time.Millisecond
)

// Caps (DD-16 rows owned here).
const (
	MaxGroups          = 64
	MaxMembersPerGroup = 32
)

// Sentinel errors the broker maps onto wire codes. Precedence is pinned by
// D-SL2-6: not-live wins over stale-generation (13 before 12).
var (
	// ErrUnknownMember → UNKNOWN_MEMBER (13): group unknown or memberID not
	// in the live set — a swept member's later requests always land here.
	ErrUnknownMember = errors.New("unknown or dead member")
	// ErrStaleGeneration → STALE_GENERATION (12): reachable only by a LIVE
	// member whose joined generation trails the group's.
	ErrStaleGeneration = errors.New("stale generation")
	// ErrTopicMismatch → MALFORMED: one topic per group (D15).
	ErrTopicMismatch = errors.New("topic mismatch")
	// ErrTooManyGroups / ErrTooManyMembers → CAP_EXCEEDED.
	ErrTooManyGroups  = errors.New("group cap reached")
	ErrTooManyMembers = errors.New("member cap reached")
)

// Clock is DESIGN §9's injectable coordinator clock.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config configures a Coordinator. Zero values take the defaults above; a
// nil Clock takes the real one.
type Config struct {
	HeartbeatInterval time.Duration
	SessionTimeout    time.Duration
	SweepInterval     time.Duration
	Clock             Clock
}

func (cfg Config) withDefaults() Config {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = DefaultSessionTimeout
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = DefaultSweepInterval
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	return cfg
}

// member is one live group member. generation records the generation the
// member last JOINED at — the REJOIN level derives from it (D-SL2-3), it is
// never a consumable flag.
type member struct {
	id         string
	generation uint64
	lastBeat   time.Time
	assignment []uint32
	connID     uint64
}

// grp is one consumer group. committed maps partition → next-to-read.
type grp struct {
	name       string
	topic      string
	partitions uint32
	generation uint64
	members    map[string]*member

	committed map[uint32]uint64

	// commitMu is D-SL2-5's per-group commitLock. Lock order is pinned
	// globally: commitMu → Coordinator.mu, NEVER the reverse.
	commitMu sync.Mutex

	// Two-phase lazy-load latch (D-SL2-5/F8): loading is closed once the
	// creating joiner finished reading disk; loaded/loadErr are then final
	// for the boot. Guarded by Coordinator.mu.
	loaded  bool
	loading chan struct{}
	loadErr error
}

// Assigned is one partition of a join result: ownership plus the committed
// next-to-read offset the member resumes from (DD-14).
type Assigned struct {
	Partition uint32
	Next      uint64
}

// JoinResult is what a JoinGroup serves: identity, generation, and the full
// resume state (join carries state — no separate offset-fetch round).
type JoinResult struct {
	MemberID   string
	Generation uint64
	Assigned   []Assigned
}

// Coordinator owns every group's state behind one mutex. Create with New;
// call Run to start the liveness sweeper (tests drive sweepOnce directly on
// a fake clock instead).
type Coordinator struct {
	cfg  Config
	dir  string // <data>/_groups — reserved storage (D-SL2-4)
	fsys storage.FS

	mu        sync.Mutex
	groups    map[string]*grp
	conns     map[uint64]map[string]string // connID → group → memberID
	memberSeq uint64                       // memberIDs unique per broker lifetime (DD-12 auto-fencing)

	stop        chan struct{}
	sweeperDone chan struct{}
}

// New prepares a coordinator over dataDir. It creates data/_groups up front
// because atomicWrite's temp file lives in the destination dir (D-SL2-5).
func New(cfg Config, dataDir string, fsys storage.FS) (*Coordinator, error) {
	dir := filepath.Join(dataDir, "_groups")
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	return &Coordinator{
		cfg:         cfg.withDefaults(),
		dir:         dir,
		fsys:        fsys,
		groups:      make(map[string]*grp),
		conns:       make(map[uint64]map[string]string),
		stop:        make(chan struct{}),
		sweeperDone: make(chan struct{}),
	}, nil
}

// Run starts the sweeper goroutine (real broker only — fake-clock tests
// call sweepOnce themselves).
func (c *Coordinator) Run() {
	go func() {
		defer close(c.sweeperDone)
		ticker := time.NewTicker(c.cfg.SweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.sweepOnce()
			case <-c.stop:
				return
			}
		}
	}()
}

// Stop halts the sweeper started by Run. Call at most once.
func (c *Coordinator) Stop() {
	close(c.stop)
	<-c.sweeperDone
}

// Join handles a JoinGroup from connID. A join on a conn already bound to a
// LIVE member of the group is that member's re-Join: same memberID, current
// state served, NO generation bump (D-SL2-3b — the livelock guard). A new
// member is a membership event: bump + immediate range reassign (DD-11).
func (c *Coordinator) Join(connID uint64, groupName, topic string, partitions uint32) (JoinResult, error) {
	g, err := c.groupForJoin(groupName, topic, partitions)
	if err != nil {
		return JoinResult{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if g.topic != topic {
		return JoinResult{}, fmt.Errorf("%w: group %s is bound to topic %s", ErrTopicMismatch, groupName, g.topic)
	}
	// The creating joiner may have claimed the wrong topic against a
	// disk-bound group; the first CORRECT joiner fixes the partition count.
	g.partitions = partitions
	now := c.cfg.Clock.Now()
	if mid, ok := c.conns[connID][groupName]; ok {
		if m, live := g.members[mid]; live {
			// Re-Join: adopt the current generation, refresh liveness, bump
			// nothing. A swept binding falls through to a fresh join below
			// (the old memberID stays fenced).
			m.lastBeat = now
			m.generation = g.generation
			return joinResultLocked(g, m), nil
		}
	}
	if len(g.members) >= MaxMembersPerGroup {
		return JoinResult{}, fmt.Errorf("%w: %d members in group %s", ErrTooManyMembers, MaxMembersPerGroup, groupName)
	}
	c.memberSeq++
	m := &member{id: fmt.Sprintf("m%d", c.memberSeq), lastBeat: now, connID: connID}
	g.members[m.id] = m
	if c.conns[connID] == nil {
		c.conns[connID] = make(map[string]string)
	}
	c.conns[connID][groupName] = m.id
	bumpLocked(g)
	m.generation = g.generation
	return joinResultLocked(g, m), nil
}

// groupForJoin resolves (creating if needed) the group for a join,
// enforcing the group cap and running the once-per-boot two-phase load: the
// creating joiner installs a loading placeholder, releases the mutex, reads
// disk (commits.go), reinstalls; contemporaries block on the latch — never
// on the mutex — so no I/O ever happens under it (D-SL2-5/F8, DD-25).
func (c *Coordinator) groupForJoin(groupName, topic string, partitions uint32) (*grp, error) {
	for {
		c.mu.Lock()
		g, ok := c.groups[groupName]
		if !ok {
			if len(c.groups) >= MaxGroups {
				c.mu.Unlock()
				return nil, fmt.Errorf("%w: %d groups live", ErrTooManyGroups, MaxGroups)
			}
			g = &grp{
				name:       groupName,
				topic:      topic,
				partitions: partitions,
				members:    make(map[string]*member),
				committed:  make(map[uint32]uint64),
				loading:    make(chan struct{}),
			}
			c.groups[groupName] = g
			c.mu.Unlock()
			c.loadCommits(g)
			continue
		}
		if !g.loaded {
			ch := g.loading
			c.mu.Unlock()
			<-ch
			continue
		}
		err := g.loadErr
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return g, nil
	}
}

// joinResultLocked builds the resume state for m. Caller holds c.mu.
func joinResultLocked(g *grp, m *member) JoinResult {
	res := JoinResult{
		MemberID:   m.id,
		Generation: g.generation,
		Assigned:   make([]Assigned, 0, len(m.assignment)),
	}
	for _, p := range m.assignment {
		res.Assigned = append(res.Assigned, Assigned{Partition: p, Next: g.committed[p]})
	}
	return res
}

// bumpLocked is the membership event: one generation bump + immediate range
// assignment (sorted memberIDs × contiguous ranges, DD-11). Caller holds
// c.mu.
func bumpLocked(g *grp) {
	g.generation++
	if len(g.members) == 0 {
		return
	}
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	total := int(g.partitions)
	base, extra := total/len(ids), total%len(ids)
	next := 0
	for i, id := range ids {
		count := base
		if i < extra {
			count++
		}
		m := g.members[id]
		m.assignment = make([]uint32, 0, count)
		for j := 0; j < count; j++ {
			m.assignment = append(m.assignment, uint32(next))
			next++
		}
	}
}

// Heartbeat refreshes liveness and reports the level-triggered REJOIN bit.
// Heartbeats are EXEMPT from the generation fence (D-SL2-6, F1): fencing
// them would make REJOIN undeliverable and falsely sweep live members
// mid-rebalance. Only an unknown member errors.
func (c *Coordinator) Heartbeat(groupName, memberID string) (rejoin bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, g, err := c.liveMemberLocked(groupName, memberID)
	if err != nil {
		return false, err
	}
	m.lastBeat = c.cfg.Clock.Now()
	return m.generation != g.generation, nil
}

// Leave removes a live member (a membership event). An unknown member gets
// ErrUnknownMember, mirroring heartbeat.
func (c *Coordinator) Leave(groupName, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, g, err := c.liveMemberLocked(groupName, memberID)
	if err != nil {
		return err
	}
	m.lastBeat = c.cfg.Clock.Now() // liveness evidence at validation (moot after removal)
	c.removeMemberLocked(g, m)
	return nil
}

// ConnClosed is the server's conn-teardown hook (D-SL2-11): control-conn
// drop is immediate death for every member bound to the conn (DD-10). It
// takes the coordinator mutex — callers must hold nothing else.
func (c *Coordinator) ConnClosed(connID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for groupName, mid := range c.conns[connID] {
		g := c.groups[groupName]
		if g == nil {
			continue
		}
		if m := g.members[mid]; m != nil {
			delete(g.members, mid)
			bumpLocked(g)
		}
	}
	delete(c.conns, connID)
}

// ValidateFetch fences a GroupFetch at serve time (DD-12): 13 before 12,
// zero state change. It does NOT refresh lastBeat — fetch connections carry
// no liveness meaning (DD-10). Returns the group's bound topic.
func (c *Coordinator) ValidateFetch(groupName, memberID string, generation uint64) (topic string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, g, err := c.liveMemberLocked(groupName, memberID)
	if err != nil {
		return "", err
	}
	if generation != g.generation {
		return "", fmt.Errorf("%w: generation %d, current is %d", ErrStaleGeneration, generation, g.generation)
	}
	return g.topic, nil
}

// liveMemberLocked resolves (group, member) or returns ErrUnknownMember —
// the 13-first half of the D-SL2-6 precedence. Caller holds c.mu.
func (c *Coordinator) liveMemberLocked(groupName, memberID string) (*member, *grp, error) {
	g := c.groups[groupName]
	if g == nil {
		return nil, nil, fmt.Errorf("%w: group %s unknown", ErrUnknownMember, groupName)
	}
	m := g.members[memberID]
	if m == nil {
		return nil, nil, fmt.Errorf("%w: %s in group %s", ErrUnknownMember, memberID, groupName)
	}
	return m, g, nil
}

// removeMemberLocked deletes m and its conn binding, then bumps. Caller
// holds c.mu.
func (c *Coordinator) removeMemberLocked(g *grp, m *member) {
	delete(g.members, m.id)
	if b := c.conns[m.connID]; b != nil && b[g.name] == m.id {
		delete(b, g.name)
		if len(b) == 0 {
			delete(c.conns, m.connID)
		}
	}
	bumpLocked(g)
}

// sweepOnce marks members dead on missed session deadlines (DD-10). A
// heartbeat and the sweep serialize on the one mutex: the member either
// lives (beat landed first) or is fenced on its next call — no torn state.
func (c *Coordinator) sweepOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Clock.Now()
	for _, g := range c.groups {
		for _, m := range g.members {
			if now.Sub(m.lastBeat) > c.cfg.SessionTimeout {
				c.removeMemberLocked(g, m)
			}
		}
	}
}
