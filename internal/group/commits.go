// Commit durability (D-SL2-5, CONS-3): merge INSIDE the per-group
// commitLock, atomicWrite BEFORE the ack, re-fence at install, and the
// two-phase once-per-boot lazy load of data/_groups/<group>.json. Files are
// per-group JSON rewritten whole per commit batch — fine at D4's per-batch
// rate and ≤16 partitions, not a mechanism for high-rate commits (G-SL2-2).
package group

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
)

// ErrCorruptCommits → MALFORMED: a group's commit file exists but cannot be
// read or parsed. The join is refused loudly, naming the file — positions
// are never guessed (D-SL2-5).
var ErrCorruptCommits = errors.New("unreadable group commit state")

// commitFileJSON is DD-13's shape: {"topic":T,"offsets":{"0":n,...}} with
// offsets = next-to-read (SPEC §1b).
type commitFileJSON struct {
	Topic   string            `json:"topic"`
	Offsets map[string]uint64 `json:"offsets"`
}

// Commit runs D-SL2-5's pinned order: commitLock → mutex{fence · snapshot ·
// merge} → atomicWrite → mutex{re-fence · install} → release → ack. Because
// every committer merges onto the latest INSTALLED map under the
// commitLock, concurrent same-group committers cannot erase each other's
// acked partitions. offsets maps partition → next-to-read.
func (c *Coordinator) Commit(groupName, memberID string, generation uint64, offsets map[uint32]uint64) error {
	c.mu.Lock()
	g := c.groups[groupName]
	c.mu.Unlock()
	if g == nil {
		return fmt.Errorf("%w: group %s unknown", ErrUnknownMember, groupName)
	}

	// Lock order pinned globally: commitMu → c.mu, NEVER the reverse
	// (deadlock otherwise). The mutex is not held across the atomicWrite.
	g.commitMu.Lock()
	defer g.commitMu.Unlock()

	c.mu.Lock()
	merged, err := c.validateAndMergeLocked(g, memberID, generation, offsets)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	topic := g.topic
	c.mu.Unlock()

	if err := c.writeCommitFile(g.name, topic, merged); err != nil {
		return fmt.Errorf("persisting commits for group %s: %w", g.name, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-fence at install: a member fenced mid-write gets 12/13 and NO ack,
	// and nothing is installed (D-SL2-5).
	if err := c.fenceLocked(g, memberID, generation); err != nil {
		return err
	}
	g.committed = merged
	return nil
}

// validateAndMergeLocked fences the commit (13 before 12; partitions
// outside the member's current assignment → stale), counts it as liveness
// evidence (D-SL2-3), and merges it onto a snapshot of the CURRENT
// committed map. Caller holds g.commitMu and c.mu.
func (c *Coordinator) validateAndMergeLocked(g *grp, memberID string, generation uint64, offsets map[uint32]uint64) (map[uint32]uint64, error) {
	m := g.members[memberID]
	if m == nil {
		return nil, fmt.Errorf("%w: %s in group %s", ErrUnknownMember, memberID, g.name)
	}
	if generation != g.generation {
		return nil, fmt.Errorf("%w: generation %d, current is %d", ErrStaleGeneration, generation, g.generation)
	}
	owned := make(map[uint32]bool, len(m.assignment))
	for _, p := range m.assignment {
		owned[p] = true
	}
	for p := range offsets {
		if !owned[p] {
			return nil, fmt.Errorf("%w: partition %d is outside the member's assignment", ErrStaleGeneration, p)
		}
	}
	// An in-flight commit is itself liveness evidence, refreshed at
	// validation time — a slow commit can never starve liveness.
	m.lastBeat = c.cfg.Clock.Now()

	merged := make(map[uint32]uint64, len(g.committed)+len(offsets))
	for p, n := range g.committed {
		merged[p] = n
	}
	for p, n := range offsets {
		merged[p] = n
	}
	return merged, nil
}

// fenceLocked re-validates member liveness and generation (13 before 12).
// Caller holds c.mu.
func (c *Coordinator) fenceLocked(g *grp, memberID string, generation uint64) error {
	if g.members[memberID] == nil {
		return fmt.Errorf("%w: %s in group %s", ErrUnknownMember, memberID, g.name)
	}
	if generation != g.generation {
		return fmt.Errorf("%w: generation %d, current is %d", ErrStaleGeneration, generation, g.generation)
	}
	return nil
}

func (c *Coordinator) commitPath(groupName string) string {
	return filepath.Join(c.dir, groupName+".json")
}

func (c *Coordinator) writeCommitFile(groupName, topic string, offsets map[uint32]uint64) error {
	enc := commitFileJSON{Topic: topic, Offsets: make(map[string]uint64, len(offsets))}
	for p, n := range offsets {
		enc.Offsets[strconv.FormatUint(uint64(p), 10)] = n
	}
	data, err := json.Marshal(enc)
	if err != nil {
		return err
	}
	return c.fsys.WriteFileAtomic(c.commitPath(groupName), data)
}

// loadCommits is the two-phase lazy load (D-SL2-5, F8), run exactly once
// per group per boot: the caller installed g as the loading placeholder and
// holds NOTHING; disk is read without the mutex, then the mutex is retaken
// to install the result and wake blocked joiners. A live group never
// re-reads disk; a failed load is sticky until restart.
func (c *Coordinator) loadCommits(g *grp) {
	path := c.commitPath(g.name)
	var (
		topic     string
		committed map[uint32]uint64
		loadErr   error
	)
	data, err := c.fsys.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Fresh group: no committed offsets, earliest = 0 always (D14).
		committed = make(map[uint32]uint64)
	case err != nil:
		loadErr = fmt.Errorf("%w: reading %s: %v", ErrCorruptCommits, path, err)
	default:
		topic, committed, loadErr = parseCommitFile(path, data)
	}

	c.mu.Lock()
	if loadErr != nil {
		g.loadErr = loadErr
	} else {
		g.committed = committed
		if topic != "" {
			// The disk binding is the durable truth; a joiner claiming a
			// different topic is refused at join validation.
			g.topic = topic
		}
	}
	g.loaded = true
	close(g.loading)
	c.mu.Unlock()
}

func parseCommitFile(path string, data []byte) (topic string, offsets map[uint32]uint64, err error) {
	var f commitFileJSON
	if jerr := json.Unmarshal(data, &f); jerr != nil {
		return "", nil, fmt.Errorf("%w: %s: %v", ErrCorruptCommits, path, jerr)
	}
	offsets = make(map[uint32]uint64, len(f.Offsets))
	for k, v := range f.Offsets {
		p, perr := strconv.ParseUint(k, 10, 32)
		if perr != nil {
			return "", nil, fmt.Errorf("%w: %s: bad partition key %q", ErrCorruptCommits, path, k)
		}
		offsets[uint32(p)] = v
	}
	return f.Topic, offsets, nil
}
