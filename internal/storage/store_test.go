// Store tests: TOP-1 (atomic create, duplicate → exists, list), TOP-2
// (partition count stable across restart), aborted-create cleanup, and the
// storage-level cap sentinels.
package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, OSFS(), FileSyncer{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCreateTopicAndList(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir)

	if err := s.CreateTopic("orders", 3); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := s.CreateTopic("audit", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	got := s.Topics()
	if len(got) != 2 || got[0].Name != "audit" || got[0].Partitions != 1 ||
		got[1].Name != "orders" || got[1].Partitions != 3 {
		t.Fatalf("Topics() = %+v, want sorted [audit/1 orders/3]", got)
	}

	// On-disk shape per DD-9: per-partition dirs with log+frontier, and
	// meta.json written last as the topic's existence marker.
	for _, sub := range []string{"0", "1", "2"} {
		for _, f := range []string{"log", "frontier"} {
			if _, err := os.Stat(filepath.Join(dir, "orders", sub, f)); err != nil {
				t.Errorf("missing %s/%s: %v", sub, f, err)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "orders", "meta.json")); err != nil {
		t.Errorf("missing meta.json: %v", err)
	}

	if _, err := s.Partition("orders", 2); err != nil {
		t.Errorf("Partition(orders, 2): %v", err)
	}
	if _, err := s.Partition("orders", 3); !errors.Is(err, ErrBadPartition) {
		t.Errorf("Partition(orders, 3) = %v, want ErrBadPartition", err)
	}
	if _, err := s.Partition("nope", 0); !errors.Is(err, ErrUnknownTopic) {
		t.Errorf("Partition(nope, 0) = %v, want ErrUnknownTopic", err)
	}
}

func TestCreateTopicDuplicate(t *testing.T) {
	s := openTestStore(t, t.TempDir())
	if err := s.CreateTopic("dup", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTopic("dup", 2); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("duplicate create = %v, want ErrTopicExists", err)
	}
	// The duplicate attempt must not have touched the original.
	if got := s.Topics(); len(got) != 1 || got[0].Partitions != 1 {
		t.Fatalf("Topics() after duplicate = %+v", got)
	}
}

func TestPartitionCountStableAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir)
	if err := s.CreateTopic("stable", 5); err != nil {
		t.Fatal(err)
	}
	p, err := s.Partition("stable", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openTestStore(t, dir)
	got := s2.Topics()
	if len(got) != 1 || got[0].Name != "stable" || got[0].Partitions != 5 {
		t.Fatalf("Topics() after restart = %+v, want stable/5 (TOP-2)", got)
	}
	if _, err := s2.Partition("stable", 4); err != nil {
		t.Fatalf("Partition(stable, 4) after restart: %v", err)
	}
}

func TestBootRemovesTopicDirWithoutMeta(t *testing.T) {
	dir := t.TempDir()
	// Simulate a create that died before its meta.json (DD-9: meta.json's
	// presence IS the topic's existence).
	orphan := filepath.Join(dir, "halfmade")
	if err := os.MkdirAll(filepath.Join(orphan, "0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "0", "log"), []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	s := openTestStore(t, dir)
	if got := s.Topics(); len(got) != 0 {
		t.Fatalf("Topics() = %+v, want none", got)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("meta-less topic dir still present after boot (err=%v)", err)
	}
}

// TestBootPreservesGroupsDirRemovesJunk is D-SL2-4: `_groups` is reserved
// coordinator storage with no meta.json — the aborted-create cleanup must
// skip exactly it, or every boot would delete all group commits.
func TestBootPreservesGroupsDirRemovesJunk(t *testing.T) {
	dir := t.TempDir()
	groupsDir := filepath.Join(dir, "_groups")
	if err := os.MkdirAll(groupsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	commitFile := filepath.Join(groupsDir, "workers.json")
	if err := os.WriteFile(commitFile, []byte(`{"topic":"t","offsets":{"0":5}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	junk := filepath.Join(dir, "junkdir")
	if err := os.MkdirAll(junk, 0o755); err != nil {
		t.Fatal(err)
	}

	openTestStore(t, dir)
	if _, err := os.Stat(commitFile); err != nil {
		t.Fatalf("_groups commit file eaten by boot cleanup: %v", err)
	}
	if _, err := os.Stat(junk); !os.IsNotExist(err) {
		t.Fatalf("meta-less junk dir still present after boot (err=%v)", err)
	}
}

func TestCreateTopicPartitionCountBounds(t *testing.T) {
	s := openTestStore(t, t.TempDir())
	for _, n := range []uint32{0, MaxPartitionsPerTopic + 1} {
		if err := s.CreateTopic("t", n); !errors.Is(err, ErrBadPartitionCount) {
			t.Errorf("CreateTopic(%d partitions) = %v, want ErrBadPartitionCount", n, err)
		}
	}
	if err := s.CreateTopic("t", MaxPartitionsPerTopic); err != nil {
		t.Errorf("CreateTopic(max partitions) = %v, want nil", err)
	}
}
