package cloudlogger

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

type activeManifest struct {
	Version           int    `json:"version"`
	File              string `json:"file"`
	StoreID           string `json:"store_id"`
	Generation        string `json:"generation"`
	CutoverGeneration uint64 `json:"cutover_generation,string"`
}
type CompactionResult struct {
	Generation          string `json:"generation"`
	MutationGeneration  uint64 `json:"mutation_generation,string"`
	CutoverMilliseconds int64  `json:"cutover_milliseconds"`
	QueueTargetMet      bool   `json:"queue_target_met"`
	OriginalBytes       int64  `json:"original_bytes,string"`
	CompactedBytes      int64  `json:"compacted_bytes,string"`
}

func loadActiveManifest(anchor string) (activeManifest, error) {
	var m activeManifest
	p := anchor + ".active.json"
	info, err := os.Lstat(p)
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 16<<10 {
		return m, ErrBillingUnavailable
	}
	b, err := os.ReadFile(p)
	if err != nil || billingarchive.StrictDecode(bytes.NewReader(b), 16<<10, &m) != nil || m.Version != 1 || len(m.StoreID) != 32 || !billingarchive.SafeID(m.Generation) || m.File != filepath.Base(m.File) {
		return m, ErrBillingUnavailable
	}
	if m.File != filepath.Base(anchor) && m.File != filepath.Base(anchor)+".gen-"+m.Generation+".db" {
		return m, ErrBillingUnavailable
	}
	return m, nil
}
func loadActivePath(anchor string) (string, error) {
	m, e := loadActiveManifest(anchor)
	if e != nil {
		return "", e
	}
	if m.File == "" {
		return "", nil
	}
	return filepath.Join(filepath.Dir(anchor), m.File), nil
}
func validateActiveManifest(anchor string, tx *bolt.Tx) error {
	m, e := loadActiveManifest(anchor)
	if e != nil {
		return e
	}
	meta := tx.Bucket([]byte("meta"))
	if m.StoreID != string(meta.Get([]byte("identity"))) || readUint(meta.Get([]byte("mutation_generation"))) < m.CutoverGeneration {
		return ErrBillingUnavailable
	}
	if generation := string(meta.Get([]byte("storage_generation"))); generation != "" {
		if generation != m.Generation {
			return ErrBillingUnavailable
		}
	} else if m.Generation != "initial" {
		return ErrBillingUnavailable
	}
	return nil
}
func publishActive(anchor string, m activeManifest) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	id := make([]byte, 8)
	if _, e = rand.Read(id); e != nil {
		return e
	}
	tmp := anchor + ".active-" + hex.EncodeToString(id) + ".tmp"
	if e = exclusiveFile(tmp, b); e != nil {
		return e
	}
	if e = os.Rename(tmp, anchor+".active.json"); e != nil {
		return e
	}
	return syncDir(filepath.Dir(anchor))
}
func applyMutations(tx *bolt.Tx, writes []mutation) error {
	for _, w := range writes {
		if w.Bucket == "mutation_journal" {
			return ErrBillingUnavailable
		}
		switch w.Kind {
		case "create":
			if _, e := tx.CreateBucketIfNotExists([]byte(w.Bucket)); e != nil {
				return e
			}
		case "put":
			b := tx.Bucket([]byte(w.Bucket))
			if b == nil {
				return ErrBillingUnavailable
			}
			if e := b.Put(w.Key, w.Value); e != nil {
				return e
			}
		case "delete":
			b := tx.Bucket([]byte(w.Bucket))
			if b == nil {
				return ErrBillingUnavailable
			}
			if e := b.Delete(w.Key); e != nil {
				return e
			}
		case "sequence":
			b := tx.Bucket([]byte(w.Bucket))
			if b == nil {
				return ErrBillingUnavailable
			}
			if e := b.SetSequence(w.Sequence); e != nil {
				return e
			}
		default:
			return ErrBillingUnavailable
		}
	}
	return nil
}

// CompactOnline never replaces an open inode. It journals concurrent commits,
// prepares a private generation, then queues only the bounded final write gate.
// The one-second queue budget is a measured qualification goal, not a fsync SLA.
func (s *BillingInbox) CompactOnline(ctx context.Context) (CompactionResult, error) {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	var result CompactionResult
	if !s.lifecycle.CompactionEnabled {
		return result, ErrLifecycleConflict
	}
	if err := s.Health(ctx); err != nil {
		return result, err
	}
	if err := privateDir(s.lifecycle.ScratchDir); err != nil {
		return result, err
	}
	var hotFS, scratchFS unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(s.path), &hotFS); err != nil {
		return result, err
	}
	if err := unix.Statfs(s.lifecycle.ScratchDir, &scratchFS); err != nil {
		return result, err
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return result, err
	}
	if int64(hotFS.Bavail)*int64(hotFS.Bsize) < info.Size()+(2<<30) || int64(scratchFS.Bavail)*int64(scratchFS.Bsize) < info.Size()+(2<<30) {
		return result, errors.New("insufficient online compaction scratch or hot generation capacity")
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return result, err
	}
	result.Generation = hex.EncodeToString(id)
	// Bootstrap a durable pointer to the current generation before marking the
	// legacy anchor fail-closed when its manifest is subsequently missing.
	s.mu.Lock()
	var storeID string
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := s.ready(tx); err != nil {
			return err
		}
		storeID = string(tx.Bucket([]byte("meta")).Get([]byte("identity")))
		return nil
	})
	if err == nil {
		existing, e := loadActiveManifest(s.anchor)
		if e != nil {
			err = e
		} else if existing.File == "" {
			err = publishActive(s.anchor, activeManifest{Version: 1, File: filepath.Base(s.path), StoreID: storeID, Generation: "initial"})
		}
	}
	if err == nil {
		// Internal journal housekeeping is excluded from domain write-sets.
		err = s.db.Update(func(tx *bolt.Tx) error {
			if tx.Bucket([]byte("mutation_journal")) != nil {
				if err := tx.DeleteBucket([]byte("mutation_journal")); err != nil {
					return err
				}
			}
			_, err := tx.CreateBucket([]byte("mutation_journal"))
			return err
		})
	}
	if err == nil {
		err = s.updateLocked(func(t *mutationTx) error {
			if t.tx.Bucket([]byte("meta")).Get([]byte("compaction_session")) != nil {
				return ErrLifecycleConflict
			}
			if err := t.put("meta", []byte("manifest_required"), []byte("1")); err != nil {
				return err
			}
			return t.put("meta", []byte("compaction_session"), []byte(result.Generation))
		})
	}
	s.mu.Unlock()
	if err != nil {
		return result, err
	}
	switched := false
	defer func() {
		if !switched {
			_ = s.update(func(t *mutationTx) error { return t.del("meta", []byte("compaction_session")) })
		}
	}()
	scratchDir := filepath.Join(s.lifecycle.ScratchDir, "compact-"+result.Generation)
	if err := os.Mkdir(scratchDir, 0700); err != nil {
		return result, err
	}
	snapshotPath := filepath.Join(scratchDir, "snapshot.db")
	s.mu.RLock()
	tx, err := s.db.Begin(false)
	s.mu.RUnlock()
	if err != nil {
		return result, err
	}
	generation := readUint(tx.Bucket([]byte("meta")).Get([]byte("mutation_generation")))
	result.OriginalBytes = tx.Size()
	out, err := os.OpenFile(snapshotPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		tx.Rollback()
		return result, err
	}
	_, err = tx.WriteTo(&boundedContextWriter{ctx: ctx, w: out, remaining: billingarchive.MaxSetBytes})
	if err == nil {
		err = out.Sync()
	}
	out.Close()
	tx.Rollback()
	if err != nil {
		return result, err
	}
	snapshot, err := bolt.Open(snapshotPath, 0600, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond})
	if err != nil {
		return result, err
	}
	candidatePath := s.anchor + ".gen-" + result.Generation + ".db"
	if _, err := os.Lstat(candidatePath); !os.IsNotExist(err) {
		snapshot.Close()
		return result, ErrBillingUnavailable
	}
	candidate, err := bolt.Open(candidatePath, 0600, &bolt.Options{Timeout: 100 * time.Millisecond, InitialMmapSize: 128 << 30})
	if err != nil {
		snapshot.Close()
		return result, err
	}
	defer func() {
		if !switched {
			candidate.Close()
		}
	}()
	if err := bolt.Compact(candidate, snapshot, 8<<20); err != nil {
		snapshot.Close()
		return result, err
	}
	snapshot.Close()
	// Full validation occurs outside the cutover gate. Every replayed write-set
	// is validated for gap-free generations and supported mutation kinds.
	if err := candidate.View(func(tx *bolt.Tx) error {
		return auditLifecycleTx(ctx, tx, s.lifecycle.Environment, s.lifecycle.SourceEventEnvironments, s.lifecycle.keys)
	}); err != nil {
		return result, err
	}
	start := time.Now()
	replayed := generation
	replay := func(batch int) (uint64, error) {
		var target uint64
		sets := [][]mutation{}
		s.mu.RLock()
		e := s.db.View(func(tx *bolt.Tx) error {
			meta := tx.Bucket([]byte("meta"))
			if string(meta.Get([]byte("compaction_session"))) != result.Generation {
				return ErrLifecycleConflict
			}
			target = readUint(meta.Get([]byte("mutation_generation")))
			end := target
			if end-replayed > uint64(batch) {
				end = replayed + uint64(batch)
			}
			for g := replayed + 1; g <= end; g++ {
				b := tx.Bucket([]byte("mutation_journal")).Get(sequenceKey(g))
				var writes []mutation
				if b == nil || json.Unmarshal(b, &writes) != nil {
					return ErrBillingUnavailable
				}
				sets = append(sets, writes)
			}
			return nil
		})
		s.mu.RUnlock()
		if e != nil {
			return target, e
		}
		if len(sets) > 0 {
			e = candidate.Update(func(tx *bolt.Tx) error {
				for _, writes := range sets {
					if err := applyMutations(tx, writes); err != nil {
						return err
					}
				}
				return nil
			})
			if e == nil {
				replayed += uint64(len(sets))
			}
		}
		return target, e
	}
	// Abort a non-converging compaction; live writers retain priority and the
	// old generation remains authoritative until manifest publication succeeds.
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		target, e := replay(256)
		if e != nil {
			return result, e
		}
		if target-replayed <= 32 {
			break
		}
		if time.Since(start) > 30*time.Second {
			return result, errors.New("online compaction catch-up did not converge")
		}
	}
	if err := candidate.Sync(); err != nil {
		return result, err
	}
	if err := syncDir(filepath.Dir(s.anchor)); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	gateStart := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed || s.recovering {
		return result, ErrBillingUnavailable
	}
	var final uint64
	sets := [][]mutation{}
	err = s.db.View(func(tx *bolt.Tx) error {
		final = readUint(tx.Bucket([]byte("meta")).Get([]byte("mutation_generation")))
		if final < replayed || final-replayed > 32 {
			return errors.New("cutover lag exceeds bounded gate budget")
		}
		for g := replayed + 1; g <= final; g++ {
			var writes []mutation
			b := tx.Bucket([]byte("mutation_journal")).Get(sequenceKey(g))
			if b == nil || json.Unmarshal(b, &writes) != nil {
				return ErrBillingUnavailable
			}
			sets = append(sets, writes)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	err = candidate.Update(func(tx *bolt.Tx) error {
		for _, writes := range sets {
			if err := applyMutations(tx, writes); err != nil {
				return err
			}
		}
		meta := tx.Bucket([]byte("meta"))
		if string(meta.Get([]byte("identity"))) != storeID || readUint(meta.Get([]byte("mutation_generation"))) != final {
			return ErrBillingUnavailable
		}
		if err := meta.Delete([]byte("compaction_session")); err != nil {
			return err
		}
		if err := meta.Put([]byte("storage_generation"), []byte(result.Generation)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if err := candidate.Sync(); err != nil {
		return result, err
	}
	if time.Since(gateStart) > 750*time.Millisecond {
		return result, errors.New("cutover preparation exceeded qualification budget; old generation remains active")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	m := activeManifest{Version: 1, File: filepath.Base(candidatePath), StoreID: storeID, Generation: result.Generation, CutoverGeneration: final}
	publicationErr := publishActive(s.anchor, m)
	if publicationErr != nil { // Rename may have committed even if directory fsync failed. Never resume
		// writes on the old generation after a visible pointer switch.
		visible, e := loadActiveManifest(s.anchor)
		if e != nil || visible.File != m.File {
			s.failed = true
			return result, publicationErr
		}
		s.failed = true
	}
	old := s.db
	oldPath, oldInfo := s.path, s.info
	s.db = candidate
	s.path = candidatePath
	s.info, err = os.Lstat(candidatePath)
	if err == nil {
		err = publicationErr
	}
	if err != nil {
		s.failed = true
	}
	switched = true
	result.MutationGeneration = final
	result.CutoverMilliseconds = time.Since(gateStart).Milliseconds()
	result.QueueTargetMet = result.CutoverMilliseconds <= 1000
	if info, e := os.Stat(candidatePath); e == nil {
		result.CompactedBytes = info.Size()
	}
	go func() {
		if closeErr := old.Close(); closeErr != nil {
			return
		}
		if cleanupErr := s.reclaimGeneration(oldPath, oldInfo, storeID); cleanupErr != nil {
			s.workerMu.Lock()
			s.workerStatus.LastError = "old generation cleanup failed; inspect private retained generation"
			s.workerMu.Unlock()
		}
	}()
	return result, err
}

func (s *BillingInbox) reclaimGeneration(oldPath string, oldInfo os.FileInfo, storeID string) error {
	info, err := os.Lstat(oldPath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !os.SameFile(info, oldInfo) || filepath.Dir(oldPath) != filepath.Dir(s.anchor) {
		return ErrBillingUnavailable
	}
	if oldPath != s.anchor {
		if !strings.HasPrefix(filepath.Base(oldPath), filepath.Base(s.anchor)+".gen-") {
			return ErrBillingUnavailable
		}
		if err := os.Remove(oldPath); err != nil {
			return err
		}
		return syncDir(filepath.Dir(oldPath))
	}
	marker, _ := json.Marshal(struct {
		ManifestRequired bool   `json:"manifest_required"`
		StoreID          string `json:"store_id"`
	}{true, storeID})
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	tmp := s.anchor + ".retired-" + hex.EncodeToString(id) + ".tmp"
	if err := exclusiveFile(tmp, marker); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.anchor); err != nil {
		return err
	}
	return syncDir(filepath.Dir(s.anchor))
}
