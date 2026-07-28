// TOP-1's crash half, API-fault side (D-SL1-1 class a): a scripted fault
// mid-CreateTopic aborts cleanly — error out, no half-topic listed, dir
// gone. External test package: storagetest imports storage.
package storage_test

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/storage/storagetest"
)

func TestCreateTopicAbortsCleanlyOnMidCreateFault(t *testing.T) {
	// One case per distinct create-path seam call (DD-9's ordering: files
	// fsynced first, meta.json last).
	cases := []struct {
		name string
		arm  func(*storagetest.FaultFS)
	}{
		{"frontier file fsync fails", func(f *storagetest.FaultFS) {
			f.FailSync("frontier", 1, syscall.ENOSPC)
		}},
		{"partition dir fsync fails", func(f *storagetest.FaultFS) {
			f.FailSyncDir("0", 1, syscall.EIO)
		}},
		{"meta.json atomicWrite fails", func(f *storagetest.FaultFS) {
			f.FailWriteFileAtomic("meta.json", 1, syscall.ENOSPC)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ffs := storagetest.WrapFS(storage.OSFS())
			s, err := storage.Open(dir, ffs, storage.FileSyncer{})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { s.Close() })

			tc.arm(ffs)
			err = s.CreateTopic("half", 2)
			if !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.EIO) {
				t.Fatalf("CreateTopic under fault = %v, want the scripted error", err)
			}
			if got := s.Topics(); len(got) != 0 {
				t.Fatalf("Topics() after aborted create = %+v, want none", got)
			}
			topicDir := filepath.Join(dir, "half")
			if _, err := os.Stat(topicDir); !os.IsNotExist(err) {
				t.Fatalf("topic dir survives the abort (err=%v), want removed", err)
			}

			// Next boot on the real FS agrees: nothing to clean, no topic.
			s2, err := storage.Open(dir, storage.OSFS(), storage.FileSyncer{})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer s2.Close()
			if got := s2.Topics(); len(got) != 0 {
				t.Fatalf("Topics() after reboot = %+v, want none", got)
			}
			// A retry with the fault gone must succeed on the same store.
			if err := s2.CreateTopic("half", 2); err != nil {
				t.Fatalf("retry CreateTopic = %v, want success", err)
			}
		})
	}
}
