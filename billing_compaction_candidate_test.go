package cloudlogger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestCompactionCandidateCreationRejectsExistingOrSymlink(t *testing.T) {
	for _, variant := range []string{"existing", "symlink"} {
		t.Run(variant, func(t *testing.T) {
			s, _, _, _ := lifecycleInbox(t)
			migrateAll(t, s)
			oldPath := s.path
			outside := filepath.Join(privateBillingDir(t), "unrelated.db")
			if err := exclusiveFile(outside, []byte("unrelated candidate must survive")); err != nil {
				t.Fatal(err)
			}
			var candidatePath string
			ctx := compactionInspectContext{Context: context.Background(), inspect: func() {
				if candidatePath != "" {
					return
				}
				copies, err := filepath.Glob(filepath.Join(s.lifecycle.ScratchDir, "compact-*", "snapshot.db"))
				if err != nil {
					t.Fatal(err)
				}
				if len(copies) == 0 {
					return
				}
				generation := strings.TrimPrefix(filepath.Base(filepath.Dir(copies[0])), "compact-")
				candidatePath = s.anchor + ".gen-" + generation + ".db"
				if variant == "existing" {
					if err := exclusiveFile(candidatePath, []byte("unrelated candidate must survive")); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Symlink(outside, candidatePath); err != nil {
					t.Fatal(err)
				}
			}}
			if _, err := s.CompactOnline(ctx); !errors.Is(err, ErrBillingUnavailable) || candidatePath == "" || s.path != oldPath {
				t.Fatal("pre-existing candidate was opened or switched", variant, err)
			}
			if raw, err := os.ReadFile(candidatePath); err != nil || string(raw) != "unrelated candidate must survive" {
				t.Fatal("O_EXCL refusal deleted or modified a nonowned candidate", err)
			}
			if raw, err := os.ReadFile(outside); err != nil || string(raw) != "unrelated candidate must survive" {
				t.Fatal("candidate creation followed an existing symlink", err)
			}
			assertNoCompactionScratch(t, s.lifecycle.ScratchDir)
		})
	}
}

func TestCompactionSubstitutedCandidateNeverPublishes(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	migrateAll(t, s)
	if err := s.InsertEvent(context.Background(), canonicalBillingEvent("candidate-substitution")); err != nil {
		t.Fatal(err)
	}
	oldPath := s.path
	var candidatePath, preserved string
	ctx := compactionInspectContext{Context: context.Background(), inspect: func() {
		if preserved != "" {
			return
		}
		candidates, err := filepath.Glob(s.anchor + ".gen-*.db")
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates) == 0 {
			return
		}
		candidatePath = candidates[0]
		preserved = candidatePath + ".preserved"
		if err := os.Rename(candidatePath, preserved); err != nil {
			t.Fatal(err)
		}
		if err := exclusiveFile(candidatePath, []byte("substituted inode must survive")); err != nil {
			t.Fatal(err)
		}
	}}
	result, err := s.CompactOnline(ctx)
	if !errors.Is(err, ErrBillingUnavailable) || preserved == "" || s.path != oldPath {
		t.Fatal("substituted candidate was published", result, err)
	}
	manifest, err := loadActiveManifest(s.anchor)
	if err != nil || filepath.Join(filepath.Dir(s.anchor), manifest.File) != oldPath {
		t.Fatal("substitution changed durable active authority", manifest, err)
	}
	if raw, err := os.ReadFile(candidatePath); err != nil || string(raw) != "substituted inode must survive" {
		t.Fatal("cleanup unlinked a replaced candidate inode", err)
	}
	// The actual opened candidate closes before cleanup; an independently opened
	// read handle succeeds at its preserved path, and unknown siblings are not swept.
	db, err := bolt.Open(preserved, 0600, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal("failed candidate handle was not closed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.lifecycle.ScratchDir, "compact-"+result.Generation)
	if _, err := os.Lstat(filepath.Join(dir, "snapshot.db")); !os.IsNotExist(err) {
		t.Fatal("substitution left plaintext scratch", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, compactionOwnerName)); err != nil {
		t.Fatal("unreclaimed candidate lost ownership inspection metadata", err)
	}
	if err := s.InsertEvent(context.Background(), canonicalBillingEvent("old-authority-still-valid")); err != nil {
		t.Fatal("prepublication substitution interrupted original admission", err)
	}
}
