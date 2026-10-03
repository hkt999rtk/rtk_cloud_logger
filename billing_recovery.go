package cloudlogger

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
)

type RecoveryState struct {
	Environment             string `json:"environment"`
	StoreID                 string `json:"store_id"`
	HighWater               uint64 `json:"high_water,string"`
	ArchiveFloor            uint64 `json:"archive_floor,string"`
	OperationHistorySHA256  string `json:"operation_history_sha256"`
	ArchiveDependencySHA256 string `json:"archive_dependency_sha256"`
	Fenced                  bool   `json:"fenced"`
}

func (s *BillingInbox) RecoveryFenced() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recovering
}

func recoveryState(tx *bolt.Tx, environment string) (RecoveryState, error) {
	m := tx.Bucket([]byte("meta"))
	st := RecoveryState{Environment: environment, StoreID: string(m.Get([]byte("identity"))), HighWater: tx.Bucket([]byte("events")).Sequence(), ArchiveFloor: readUint(m.Get([]byte("archive_floor"))), Fenced: string(m.Get([]byte("recovery_fence"))) == "1"}
	hashBucket := func(name string) (string, error) {
		records := []json.RawMessage{}
		b := tx.Bucket([]byte(name))
		if b != nil {
			if err := b.ForEach(func(k, v []byte) error {
				var parsed any
				if json.Unmarshal(v, &parsed) != nil {
					return ErrBillingUnavailable
				}
				records = append(records, append(json.RawMessage(nil), v...))
				return nil
			}); err != nil {
				return "", err
			}
		}
		raw, _ := json.Marshal(records)
		return billingarchive.Digest(raw), nil
	}
	var err error
	st.OperationHistorySHA256, err = hashBucket("operations")
	if err != nil {
		return st, err
	}
	st.ArchiveDependencySHA256, err = hashBucket("catalog")
	return st, err
}

// SetRecoveryMode must be explicitly enabled before serving any restored copy.
// It is durable across restarts and cannot be bypassed by clearing an env flag.
func (s *BillingInbox) SetRecoveryMode(ctx context.Context) error {
	return s.update(func(t *mutationTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := t.put("meta", []byte("recovery_fence"), []byte("1")); err != nil {
			return err
		}
		s.recovering = true
		return nil
	})
}
func (s *BillingInbox) RecoveryState(ctx context.Context) (RecoveryState, error) {
	var st RecoveryState
	if err := ctx.Err(); err != nil {
		return st, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.failed || s.closed {
		return st, ErrBillingUnavailable
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		st, err = recoveryState(tx, s.lifecycle.Environment)
		return err
	})
	return st, err
}
func (s *BillingInbox) AdmitRecovery(ctx context.Context, approval billingarchive.SignedRecoveryApproval) (RecoveryState, error) {
	p, err := billingarchive.VerifyRecoveryApproval(approval, s.lifecycle.recoveryKeys, time.Now().UTC())
	if err != nil {
		return RecoveryState{}, ErrLifecycleConflict
	}
	var st RecoveryState
	err = s.update(func(t *mutationTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		st, err = recoveryState(t.tx, s.lifecycle.Environment)
		if err != nil {
			return err
		}
		if !st.Fenced || p.Environment != st.Environment || p.StoreID != st.StoreID || p.HighWater != st.HighWater || p.ArchiveFloor != st.ArchiveFloor || p.OperationHistorySHA256 != st.OperationHistorySHA256 || p.ArchiveDependencySHA256 != st.ArchiveDependencySHA256 {
			return ErrLifecycleConflict
		}
		body, _ := json.Marshal(approval)
		if err := t.put("meta", []byte("recovery_admission"), body); err != nil {
			return err
		}
		if err := t.del("meta", []byte("recovery_fence")); err != nil {
			return err
		}
		st.Fenced = false
		return nil
	})
	if err == nil {
		s.mu.Lock()
		s.recovering = false
		s.mu.Unlock()
	}
	return st, err
}

// PrepareBillingSnapshotRecovery is offline staging only. The controller must
// supply a newly owned private directory, never an installed/live inbox path.
// Original signed snapshot bytes are audited before changing admission metadata.
func PrepareBillingSnapshotRecovery(ctx context.Context, path string, m billingarchive.Manifest, keys map[string]ed25519.PublicKey) (RecoveryState, error) {
	var st RecoveryState
	if !filepath.IsAbs(path) || privateDir(filepath.Dir(path)) != nil {
		return st, ErrBillingUnavailable
	}
	for _, p := range []string{path + ".active.json", path + ".recovery-stage.json"} {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			return st, ErrLifecycleConflict
		}
	}
	if err := ValidateBillingSnapshot(ctx, path, m, keys); err != nil {
		return st, err
	}
	if err := ctx.Err(); err != nil {
		return st, err
	}
	marker, _ := json.Marshal(struct {
		StoreID                string `json:"store_id"`
		SetID                  string `json:"set_id"`
		OriginalSnapshotSHA256 string `json:"original_snapshot_sha256"`
	}{m.StoreID, m.SetID, m.SnapshotSHA256})
	if err := exclusiveFile(path+".recovery-stage.json", marker); err != nil {
		return st, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		return st, err
	}
	defer db.Close()
	generation := "initial"
	var frontier uint64
	err = db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		meta := tx.Bucket([]byte("meta"))
		if v := string(meta.Get([]byte("storage_generation"))); v != "" {
			generation = v
		}
		frontier = readUint(meta.Get([]byte("mutation_generation")))
		if frontier == ^uint64(0) {
			return ErrBillingUnavailable
		}
		frontier++
		if err := meta.Put([]byte("mutation_generation"), sequenceKey(frontier)); err != nil {
			return err
		}
		if err := meta.Put([]byte("recovery_fence"), []byte("1")); err != nil {
			return err
		}
		if err := meta.Put([]byte("manifest_required"), []byte("1")); err != nil {
			return err
		}
		if err := meta.Delete([]byte("compaction_session")); err != nil {
			return err
		}
		var err error
		st, err = recoveryState(tx, m.Environment)
		return err
	})
	if err != nil {
		return st, err
	}
	if err := db.Sync(); err != nil {
		return st, err
	}
	active, _ := json.Marshal(activeManifest{Version: 1, File: filepath.Base(path), StoreID: m.StoreID, Generation: generation, CutoverGeneration: frontier})
	if err := exclusiveFile(path+".active.json", active); err != nil {
		return st, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return st, err
	}
	return st, nil
}
