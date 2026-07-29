// Topics registry (DD-9): atomic topic creation with meta.json written
// last, boot cleanup of aborted creates, partition lifecycle, and the
// graceful-stop drain. Errors are storage sentinels; broker maps them onto
// protocol codes (storage never imports wire).
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Storage-level caps (DD-16 subset owned here; the rest live at the edge).
const (
	MaxTopics             = 64
	MaxPartitionsPerTopic = 16
)

// Sentinel errors the broker maps onto wire codes.
var (
	ErrTopicExists       = errors.New("topic already exists")
	ErrUnknownTopic      = errors.New("unknown topic")
	ErrBadPartition      = errors.New("partition out of range")
	ErrTooManyTopics     = errors.New("topic cap reached")
	ErrBadPartitionCount = errors.New("partition count out of range")
)

// TopicInfo is one row of the topics listing.
type TopicInfo struct {
	Name       string
	Partitions uint32
}

// topicMeta is meta.json's shape; its presence IS the topic's existence.
type topicMeta struct {
	Name       string `json:"name"`
	Partitions uint32 `json:"partitions"`
	CreatedAt  string `json:"createdAt"`
}

type topic struct {
	meta  topicMeta
	parts []*Partition
}

// Store owns the data directory: topic registry plus every partition's
// lifecycle. Safe for concurrent use.
type Store struct {
	fsys   FS
	dir    string
	syncer Syncer

	mu     sync.RWMutex
	topics map[string]*topic
}

// Open loads dir, removing aborted creates (topic dirs without meta.json)
// and running the DD-4 boot scan on every partition; any refusal aborts the
// boot loudly.
func Open(dir string, fsys FS, syncer Syncer) (*Store, error) {
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	s := &Store{fsys: fsys, dir: dir, syncer: syncer, topics: make(map[string]*topic)}

	entries, err := fsys.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name == "_groups" {
			// D-SL2-4: reserved coordinator storage — commit files live
			// there with no meta.json, and deleting it would eat every
			// group's positions. Collision with a topic is impossible:
			// DD-18 names must start [a-z0-9].
			continue
		}
		topicDir := filepath.Join(dir, name)
		metaBytes, err := fsys.ReadFile(filepath.Join(topicDir, "meta.json"))
		if errors.Is(err, fs.ErrNotExist) {
			// Aborted create: meta.json is written last, so a dir without
			// one never existed as a topic.
			if err := fsys.RemoveAll(topicDir); err != nil {
				return nil, fmt.Errorf("removing aborted topic dir %s: %w", topicDir, err)
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		var meta topicMeta
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return nil, fmt.Errorf("topic %s: unreadable meta.json: %w", name, err)
		}
		if meta.Partitions < 1 || meta.Partitions > MaxPartitionsPerTopic {
			return nil, fmt.Errorf("topic %s: meta.json partition count %d out of range", name, meta.Partitions)
		}
		t := &topic{meta: meta}
		for i := uint32(0); i < meta.Partitions; i++ {
			p, err := openPartition(fsys, filepath.Join(topicDir, strconv.Itoa(int(i))), syncer)
			if err != nil {
				s.Close()
				return nil, err
			}
			t.parts = append(t.parts, p)
		}
		s.topics[name] = t
	}
	return s, nil
}

// CreateTopic creates a topic per DD-9's ordering: partition dirs and empty
// log+frontier files first (all fsynced), meta.json last via atomicWrite.
// The caller has already validated the name (DD-18, protocol layer).
func (s *Store) CreateTopic(name string, partitions uint32) error {
	if partitions < 1 || partitions > MaxPartitionsPerTopic {
		return fmt.Errorf("%w: %d not in 1..%d", ErrBadPartitionCount, partitions, MaxPartitionsPerTopic)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[name]; ok {
		return ErrTopicExists
	}
	if len(s.topics) >= MaxTopics {
		return fmt.Errorf("%w: %d topics live", ErrTooManyTopics, len(s.topics))
	}

	topicDir := filepath.Join(s.dir, name)
	abort := func(err error) error {
		// Best-effort cleanup; boot removes any meta-less leftovers anyway.
		s.fsys.RemoveAll(topicDir)
		return err
	}
	for i := uint32(0); i < partitions; i++ {
		pdir := filepath.Join(topicDir, strconv.Itoa(int(i)))
		if err := s.fsys.MkdirAll(pdir); err != nil {
			return abort(err)
		}
		for _, fname := range []string{"log", "frontier"} {
			f, err := s.fsys.OpenAppend(filepath.Join(pdir, fname))
			if err != nil {
				return abort(err)
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return abort(err)
			}
			if err := f.Close(); err != nil {
				return abort(err)
			}
		}
		if err := s.fsys.SyncDir(pdir); err != nil {
			return abort(err)
		}
	}
	if err := s.fsys.SyncDir(topicDir); err != nil {
		return abort(err)
	}

	meta := topicMeta{Name: name, Partitions: partitions, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return abort(err)
	}
	if err := s.fsys.WriteFileAtomic(filepath.Join(topicDir, "meta.json"), metaBytes); err != nil {
		return abort(err)
	}

	t := &topic{meta: meta}
	for i := uint32(0); i < partitions; i++ {
		p, err := openPartition(s.fsys, filepath.Join(topicDir, strconv.Itoa(int(i))), s.syncer)
		if err != nil {
			for _, prev := range t.parts {
				prev.Close()
			}
			return abort(err)
		}
		t.parts = append(t.parts, p)
	}
	s.topics[name] = t
	return nil
}

// Topics lists live topics sorted by name (stable output for TOP-2's test
// and ListTopicsResp).
func (s *Store) Topics() []TopicInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TopicInfo, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, TopicInfo{Name: t.meta.Name, Partitions: t.meta.Partitions})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// TopicPartitions returns a live topic's partition count (the broker's
// group-join validation needs it before any group state changes).
func (s *Store) TopicPartitions(name string) (uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrUnknownTopic, name)
	}
	return t.meta.Partitions, nil
}

// Partition resolves (topic, index) to its live partition.
func (s *Store) Partition(name string, index uint32) (*Partition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownTopic, name)
	}
	if index >= t.meta.Partitions {
		return nil, fmt.Errorf("%w: %d not in 0..%d", ErrBadPartition, index, t.meta.Partitions-1)
	}
	return t.parts[index], nil
}

// Drain waits up to timeout for every queued produce waiter to be acked
// (graceful-stop step 4: a snapshot of already-queued work, bounded, never
// quiescence-chasing — the broker's draining flag already rejects new work).
func (s *Store) Drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.queuedWaiters() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (s *Store) queuedWaiters() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, t := range s.topics {
		for _, p := range t.parts {
			n += p.QueuedWaiters()
		}
	}
	return n
}

// Close stops every partition's flusher (joining them) and closes the log
// files (graceful-stop steps 5–6). Safe to call on a partially-opened store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, t := range s.topics {
		for _, p := range t.parts {
			if err := p.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
