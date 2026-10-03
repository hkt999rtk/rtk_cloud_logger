package cloudlogger

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type compactionInspectContext struct {
	context.Context
	inspect func()
}

func (c compactionInspectContext) Err() error {
	c.inspect()
	return c.Context.Err()
}

func testCompactionOwner(t *testing.T, s *BillingInbox, generation string) compactionScratchOwner {
	t.Helper()
	var storeID string
	if err := s.db.View(func(tx *bolt.Tx) error {
		storeID = string(tx.Bucket([]byte("meta")).Get([]byte("identity")))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return compactionScratchOwner{Version: 1, Environment: s.lifecycle.Environment, StoreID: storeID, Anchor: s.anchor, Generation: generation}
}

func makeCompactionScratch(t *testing.T, scratch string, owner compactionScratchOwner) string {
	t.Helper()
	dir := filepath.Join(scratch, "compact-"+owner.Generation)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := exclusiveFile(filepath.Join(dir, compactionOwnerName), raw); err != nil {
		t.Fatal(err)
	}
	if err := exclusiveFile(filepath.Join(dir, "snapshot.db"), []byte("interrupted plaintext snapshot")); err != nil {
		t.Fatal(err)
	}
	return dir
}

func assertNoCompactionScratch(t *testing.T, scratch string) {
	t.Helper()
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "compact-") {
			t.Fatalf("compaction retained plaintext scratch: %s", entry.Name())
		}
	}
}

func waitCompactionReclaimed(t *testing.T, s *BillingInbox, oldPath string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		info, err := os.Stat(oldPath)
		if oldPath != s.anchor && os.IsNotExist(err) || oldPath == s.anchor && err == nil && info.Size() < 1024 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("old generation reclamation did not finish", oldPath, err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOnlineCompactionScratchCleanedOnSuccessErrorAndCancel(t *testing.T) {
	for _, outcome := range []string{"success", "snapshot-audit-error", "cancel-during-copy"} {
		t.Run(outcome, func(t *testing.T) {
			s, _, _, _ := lifecycleInbox(t)
			migrateAll(t, s)
			if err := s.InsertEvent(context.Background(), canonicalBillingEvent("cleanup")); err != nil {
				t.Fatal(err)
			}
			// A pending backup is a different namespace and must survive cleanup.
			backup := filepath.Join(s.lifecycle.ScratchDir, "backup-set")
			if err := os.Mkdir(backup, 0700); err != nil {
				t.Fatal(err)
			}
			manifest := filepath.Join(backup, "manifest.json")
			if err := exclusiveFile(manifest, []byte("keep encrypted backup")); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch outcome {
			case "snapshot-audit-error":
				// Test-owned corruption fails only after the snapshot handle opens
				// and compacting finishes, exercising its error-path Close.
				if err := s.db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte("receipt_times")).Put(sequenceKey(2), []byte("unexpected receipt"))
				}); err != nil {
					t.Fatal(err)
				}
			case "cancel-during-copy":
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx = compactionInspectContext{Context: parent, inspect: func() {
					matches, err := filepath.Glob(filepath.Join(s.lifecycle.ScratchDir, "compact-*", "snapshot.db"))
					if err != nil {
						t.Fatal(err)
					}
					if len(matches) != 0 {
						cancel()
					}
				}}
			}
			oldPath := s.path
			result, err := s.CompactOnline(ctx)
			if outcome == "success" && err != nil || outcome == "snapshot-audit-error" && !errors.Is(err, ErrBillingUnavailable) || outcome == "cancel-during-copy" && !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected %s compaction outcome: %+v %v", outcome, result, err)
			}
			if result.Generation == "" {
				t.Fatal("test never reached operation-owned scratch")
			}
			assertNoCompactionScratch(t, s.lifecycle.ScratchDir)
			if outcome == "success" {
				waitCompactionReclaimed(t, s, oldPath)
			} else if _, err := os.Lstat(s.anchor + ".gen-" + result.Generation + ".db"); !os.IsNotExist(err) {
				t.Fatal("prepublication failure retained plaintext shadow", err)
			}
			if raw, err := os.ReadFile(manifest); err != nil || string(raw) != "keep encrypted backup" {
				t.Fatal("cleanup touched unrelated backup", err)
			}
		})
	}
}

func TestCompactionCleanupFailureDoesNotRollBackCutover(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	migrateAll(t, s)
	if err := s.InsertEvent(context.Background(), canonicalBillingEvent("post-cleanup-error")); err != nil {
		t.Fatal(err)
	}
	oldPath := s.path
	var preserve string
	ctx := compactionInspectContext{Context: context.Background(), inspect: func() {
		candidates, err := filepath.Glob(s.anchor + ".gen-*.db")
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates) == 0 || preserve != "" {
			return
		}
		generation := strings.TrimSuffix(strings.TrimPrefix(candidates[0], s.anchor+".gen-"), ".db")
		preserve = filepath.Join(s.lifecycle.ScratchDir, "compact-"+generation, "unrelated.txt")
		if err := exclusiveFile(preserve, []byte("do not sweep unknown files")); err != nil {
			t.Fatal(err)
		}
	}}
	result, err := s.CompactOnline(ctx)
	if !errors.Is(err, ErrBillingUnavailable) || preserve == "" || result.Generation == "" || s.path == oldPath || s.path != s.anchor+".gen-"+result.Generation+".db" {
		t.Fatal("cleanup failure was ignored or rolled back active generation", result, err)
	}
	waitCompactionReclaimed(t, s, oldPath)
	if !strings.Contains(s.Worker(context.Background()).LastError, "compaction scratch cleanup failed") {
		t.Fatal("cleanup failure missing from worker status")
	}
	if raw, err := os.ReadFile(preserve); err != nil || string(raw) != "do not sweep unknown files" {
		t.Fatal("cleanup swept unknown private file", err)
	}
	if err := s.InsertEvent(context.Background(), canonicalBillingEvent("after-error")); err != nil {
		t.Fatal("cleanup error interrupted new generation admission", err)
	}
	if page, err := s.Page(context.Background(), "", 10); err != nil || page.HighWater != 2 {
		t.Fatal("cleanup error lost committed writes", page, err)
	}
	// Once the operator removes the identified unexpected file, a retry reaps
	// only that marked interrupted scratch and never the live generation.
	if err := os.Remove(preserve); err != nil {
		t.Fatal(err)
	}
	previous := s.path
	if _, err := s.CompactOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitCompactionReclaimed(t, s, previous)
	assertNoCompactionScratch(t, s.lifecycle.ScratchDir)
}

func TestRestartCompactionReapsOnlyExactOwnedInterruptedScratch(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	migrateAll(t, s)
	if err := s.InsertEvent(context.Background(), canonicalBillingEvent("restart-cleanup")); err != nil {
		t.Fatal(err)
	}
	cfg, anchor := s.lifecycle, s.anchor
	owner := testCompactionOwner(t, s, strings.Repeat("1", 32))
	if err := publishActive(anchor, activeManifest{Version: 1, File: filepath.Base(anchor), StoreID: owner.StoreID, Generation: "initial"}); err != nil {
		t.Fatal(err)
	}
	interrupted := makeCompactionScratch(t, cfg.ScratchDir, owner)
	foreignOwner := owner
	foreignOwner.Environment, foreignOwner.Generation = "dev", strings.Repeat("2", 32)
	foreign := makeCompactionScratch(t, cfg.ScratchDir, foreignOwner)
	if err := s.update(func(t *mutationTx) error {
		return t.put("meta", []byte("compaction_session"), []byte(owner.Generation))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenBillingInbox(anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.ConfigureLifecycle(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.CompactOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitCompactionReclaimed(t, reopened, anchor)
	if _, err := os.Lstat(interrupted); !os.IsNotExist(err) {
		t.Fatal("restart retained exact owned interrupted scratch", err)
	}
	if raw, err := os.ReadFile(filepath.Join(foreign, "snapshot.db")); err != nil || string(raw) != "interrupted plaintext snapshot" {
		t.Fatal("restart deleted another environment's scratch", err)
	}
	if page, err := reopened.Page(context.Background(), "", 10); err != nil || page.StoreID != owner.StoreID || page.HighWater != 1 {
		t.Fatal("restart scratch cleanup affected active data", page, err)
	}
}

func TestCompactionScratchCleanupRefusesUnknownOrUnsafeFiles(t *testing.T) {
	for _, variant := range []string{"legacy-unmarked", "unknown-file", "symlink-snapshot", "symlink-directory", "public-marker", "mismatched-generation", "active-anchor-overlap"} {
		t.Run(variant, func(t *testing.T) {
			s, _, _, _ := lifecycleInbox(t)
			owner := testCompactionOwner(t, s, strings.Repeat("3", 32))
			if variant == "active-anchor-overlap" {
				owner.Anchor = filepath.Join(s.lifecycle.ScratchDir, "compact-"+owner.Generation, "snapshot.db")
			}
			dir := makeCompactionScratch(t, s.lifecycle.ScratchDir, owner)
			snapshot := filepath.Join(dir, "snapshot.db")
			outside := filepath.Join(privateBillingDir(t), "unrelated.db")
			if err := exclusiveFile(outside, []byte("must survive")); err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "legacy-unmarked":
				if err := os.Remove(filepath.Join(dir, compactionOwnerName)); err != nil {
					t.Fatal(err)
				}
			case "unknown-file":
				if err := exclusiveFile(filepath.Join(dir, "other.db"), []byte("keep")); err != nil {
					t.Fatal(err)
				}
			case "symlink-snapshot":
				if err := os.Remove(snapshot); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, snapshot); err != nil {
					t.Fatal(err)
				}
			case "symlink-directory":
				moved := dir + "-preserved"
				if err := os.Rename(dir, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(moved, dir); err != nil {
					t.Fatal(err)
				}
			case "public-marker":
				if err := os.Chmod(filepath.Join(dir, compactionOwnerName), 0644); err != nil {
					t.Fatal(err)
				}
			case "mismatched-generation":
				changed := owner
				changed.Generation = strings.Repeat("4", 32)
				raw, _ := json.Marshal(changed)
				if err := os.WriteFile(filepath.Join(dir, compactionOwnerName), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := publishActive(s.anchor, activeManifest{Version: 1, File: filepath.Base(s.anchor), StoreID: owner.StoreID, Generation: "initial"}); err != nil {
				t.Fatal(err)
			}
			if err := cleanupInterruptedCompactionScratch(s.lifecycle.ScratchDir, owner, s.path); err == nil {
				t.Fatal("unsafe or legacy scratch was silently considered disposable")
			}
			if _, err := os.Lstat(snapshot); err != nil {
				t.Fatal("refused cleanup still removed snapshot", err)
			}
			if raw, err := os.ReadFile(outside); err != nil || string(raw) != "must survive" {
				t.Fatal("cleanup followed a symlink outside operation ownership", err)
			}
		})
	}
}

func TestCompactionAmbiguousPublicationRetainsCandidateWithoutPlaintextScratch(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	migrateAll(t, s)
	if err := s.InsertEvent(context.Background(), canonicalBillingEvent("ambiguous-publication")); err != nil {
		t.Fatal(err)
	}
	oldPath := s.path
	savedManifest := filepath.Join(privateBillingDir(t), "initial-active.json")
	intercepted := false
	ctx := compactionInspectContext{Context: context.Background(), inspect: func() {
		candidates, err := filepath.Glob(s.anchor + ".gen-*.db")
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates) == 0 || intercepted {
			return
		}
		intercepted = true
		// Test-owned obstruction makes publication's rename fail and prevents
		// authoritative visibility from being read. The candidate must survive.
		if err := os.Rename(s.anchor+".active.json", savedManifest); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(s.anchor+".active.json", 0700); err != nil {
			t.Fatal(err)
		}
	}}
	result, err := s.CompactOnline(ctx)
	if err == nil || !intercepted || !s.failed || s.path != oldPath {
		t.Fatal("ambiguous publication did not fail closed", result, err)
	}
	if _, err := os.Stat(s.anchor + ".gen-" + result.Generation + ".db"); err != nil {
		t.Fatal("ambiguous candidate was deleted", err)
	}
	dir := filepath.Join(s.lifecycle.ScratchDir, "compact-"+result.Generation)
	if _, err := os.Lstat(filepath.Join(dir, "snapshot.db")); !os.IsNotExist(err) {
		t.Fatal("ambiguous publication retained plaintext scratch snapshot", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, compactionOwnerName)); err != nil {
		t.Fatal("ambiguous candidate lost durable ownership evidence", err)
	}
	if !strings.Contains(s.Worker(context.Background()).LastError, "uncertain manifest publication") {
		t.Fatal("ambiguous retained candidate was not reported")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// With publication authority still unavailable, a scavenger must preserve
	// both the owned candidate and marker, not assume the old file won.
	owner := testCompactionOwnerFromMarker(t, dir)
	if err := cleanupInterruptedCompactionScratch(s.lifecycle.ScratchDir, owner, oldPath); err == nil {
		t.Fatal("missing publication authority allowed guessed cleanup")
	}
	if err := os.Remove(s.anchor + ".active.json"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(savedManifest, s.anchor+".active.json"); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenBillingInbox(s.anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.ConfigureLifecycle(s.lifecycle); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.CompactOnline(context.Background()); err != nil {
		t.Fatal("known old authority did not permit owned closed shadow cleanup", err)
	}
	waitCompactionReclaimed(t, reopened, oldPath)
	if _, err := os.Lstat(owner.Anchor + ".gen-" + owner.Generation + ".db"); !os.IsNotExist(err) {
		t.Fatal("reconciled failed shadow retained plaintext", err)
	}
	assertNoCompactionScratch(t, reopened.lifecycle.ScratchDir)
}

func testCompactionOwnerFromMarker(t *testing.T, dir string) compactionScratchOwner {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	owner, err := readCompactionOwner(root)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func TestInterruptedCompactionCleanupPreservesActiveOrUnverifiableShadow(t *testing.T) {
	for _, variant := range []string{"active", "partial", "wrong-store", "wrong-generation"} {
		t.Run(variant, func(t *testing.T) {
			s, _, _, _ := lifecycleInbox(t)
			migrateAll(t, s)
			if err := s.InsertEvent(context.Background(), canonicalBillingEvent("preserved-shadow")); err != nil {
				t.Fatal(err)
			}
			generation := strings.Repeat("5", 32)
			if variant == "active" {
				before := s.path
				result, err := s.CompactOnline(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				waitCompactionReclaimed(t, s, before)
				generation = result.Generation
			}
			owner := testCompactionOwner(t, s, generation)
			if variant != "active" {
				if err := publishActive(s.anchor, activeManifest{Version: 1, File: filepath.Base(s.anchor), StoreID: owner.StoreID, Generation: "initial"}); err != nil {
					t.Fatal(err)
				}
			}
			dir := makeCompactionScratch(t, s.lifecycle.ScratchDir, owner)
			path := owner.Anchor + ".gen-" + generation + ".db"
			if variant == "partial" {
				if err := exclusiveFile(path, []byte("unverifiable partial shadow")); err != nil {
					t.Fatal(err)
				}
			} else if variant != "active" {
				db, err := bolt.Open(path, 0600, nil)
				if err != nil {
					t.Fatal(err)
				}
				err = db.Update(func(tx *bolt.Tx) error {
					meta, err := tx.CreateBucket([]byte("meta"))
					if err != nil {
						return err
					}
					storeID, session := owner.StoreID, generation
					if variant == "wrong-store" {
						storeID = strings.Repeat("6", 32)
					} else {
						session = strings.Repeat("6", 32)
					}
					if err := meta.Put([]byte("identity"), []byte(storeID)); err != nil {
						return err
					}
					return meta.Put([]byte("compaction_session"), []byte(session))
				})
				if err := errors.Join(err, db.Close()); err != nil {
					t.Fatal(err)
				}
			}
			err := cleanupInterruptedCompactionScratch(s.lifecycle.ScratchDir, owner, s.path)
			if variant == "active" && err != nil || variant != "active" && err == nil {
				t.Fatal("unexpected retained shadow qualification", variant, err)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatal("active or unqualified shadow was swept", variant, err)
			}
			if variant == "active" {
				if _, err := os.Lstat(dir); !os.IsNotExist(err) {
					t.Fatal("published active generation kept obsolete plaintext scratch", err)
				}
				if page, err := s.Page(context.Background(), "", 10); err != nil || page.HighWater != 1 {
					t.Fatal("cleanup damaged active generation", page, err)
				}
			} else if _, err := os.Stat(filepath.Join(dir, "snapshot.db")); err != nil {
				t.Fatal("unqualified shadow lost recovery/inspection evidence", err)
			}
		})
	}
}

func TestCompactionCandidateCleanupRequiresExactClosedInode(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	owner := testCompactionOwner(t, s, strings.Repeat("7", 32))
	path := owner.Anchor + ".gen-" + owner.Generation + ".db"
	if err := exclusiveFile(path, []byte("known closed candidate")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	preserved := path + ".preserved"
	if err := os.Rename(path, preserved); err != nil {
		t.Fatal(err)
	}
	if err := exclusiveFile(path, []byte("replacement must survive")); err != nil {
		t.Fatal(err)
	}
	if err := removeCompactionCandidate(owner, info); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal("candidate cleanup accepted a replaced inode", err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "replacement must survive" {
		t.Fatal("exact filename was treated as sufficient deletion authority", err)
	}
	if raw, err := os.ReadFile(preserved); err != nil || string(raw) != "known closed candidate" {
		t.Fatal("cleanup swept unknown generation siblings", err)
	}
}
