package cloudlogger

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
)

var ErrLifecycleConflict = errors.New("billing lifecycle precondition failed")
var ErrOperationNotFound = errors.New("billing retirement operation not found")

type LifecycleConfig struct {
	Environment             string             `json:"environment"`
	SourceEventEnvironments []string           `json:"source_event_environments"`
	Stack                   string             `json:"stack"`
	ScratchDir              string             `json:"scratch_dir"`
	EncryptionKeyID         string             `json:"encryption_key_id"`
	Recipients              []string           `json:"recipients"`
	VerifierKeys            map[string]string  `json:"verifier_keys"`
	RecoveryKeys            map[string]string  `json:"recovery_keys"`
	AuthorityURL            string             `json:"authority_url"`
	AuthorityToken          string             `json:"-"`
	RetentionDays           int                `json:"retention_days"`
	PartBytes               int64              `json:"part_bytes"`
	ScratchCapacityBytes    int64              `json:"scratch_capacity_bytes"`
	CacheMaxBytes           int64              `json:"cache_max_bytes"`
	BackupEnabled           bool               `json:"backup_enabled"`
	RetirementEnabled       bool               `json:"retirement_enabled"`
	CompactionEnabled       bool               `json:"compaction_enabled"`
	CaptureInterval         time.Duration      `json:"-"`
	ObjectStore             ArchiveObjectStore `json:"-"`
	authority               func(context.Context, RetirementStatus) error
	keys                    map[string]ed25519.PublicKey
	recoveryKeys            map[string]ed25519.PublicKey
}

type receiptBinding struct {
	Sequence      uint64 `json:"sequence,string"`
	ContentSHA256 string `json:"content_sha256"`
	RecordSHA256  string `json:"record_sha256"`
}
type mutation struct {
	Bucket   string `json:"bucket"`
	Key      []byte `json:"key,omitempty"`
	Value    []byte `json:"value,omitempty"`
	Kind     string `json:"kind"`
	Sequence uint64 `json:"sequence,string,omitempty"`
}
type mutationTx struct {
	tx     *bolt.Tx
	writes []mutation
}

func (t *mutationTx) put(bucket string, key, value []byte) error {
	if err := t.tx.Bucket([]byte(bucket)).Put(key, value); err != nil {
		return err
	}
	t.writes = append(t.writes, mutation{Bucket: bucket, Key: bytes.Clone(key), Value: bytes.Clone(value), Kind: "put"})
	return nil
}
func (t *mutationTx) del(bucket string, key []byte) error {
	if err := t.tx.Bucket([]byte(bucket)).Delete(key); err != nil {
		return err
	}
	t.writes = append(t.writes, mutation{Bucket: bucket, Key: bytes.Clone(key), Kind: "delete"})
	return nil
}
func (t *mutationTx) create(bucket string) error {
	if _, err := t.tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return err
	}
	t.writes = append(t.writes, mutation{Bucket: bucket, Kind: "create"})
	return nil
}
func (t *mutationTx) setSequence(bucket string, v uint64) error {
	if err := t.tx.Bucket([]byte(bucket)).SetSequence(v); err != nil {
		return err
	}
	t.writes = append(t.writes, mutation{Bucket: bucket, Kind: "sequence", Sequence: v})
	return nil
}
func (t *mutationTx) nextSequence(bucket string) (uint64, error) {
	v := t.tx.Bucket([]byte(bucket)).Sequence()
	if v == ^uint64(0) {
		return 0, ErrBillingUnavailable
	}
	return v + 1, t.setSequence(bucket, v+1)
}
func readUint(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// updateLocked is the only domain write gateway. During online compaction the
// deterministic write-set and its generation commit atomically with domain data.
func (s *BillingInbox) updateLocked(fn func(*mutationTx) error) error {
	if s.closed || s.failed {
		return ErrBillingUnavailable
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		t := &mutationTx{tx: tx}
		if err := fn(t); err != nil {
			return err
		}
		if len(t.writes) == 0 {
			return nil
		}
		meta := tx.Bucket([]byte("meta"))
		g := readUint(meta.Get([]byte("mutation_generation")))
		if g == ^uint64(0) {
			return ErrBillingUnavailable
		}
		g++
		if err := t.put("meta", []byte("mutation_generation"), sequenceKey(g)); err != nil {
			return err
		}
		if meta.Get([]byte("compaction_session")) != nil {
			// Journal growth must never exhaust the hot volume or reject ingest.
			// Removing the session makes the compactor abort before any cutover.
			if tx.Bucket([]byte("mutation_journal")).Stats().LeafInuse > 128<<20 {
				return meta.Delete([]byte("compaction_session"))
			}
			body, err := json.Marshal(t.writes)
			if err != nil {
				return err
			}
			if err := tx.Bucket([]byte("mutation_journal")).Put(sequenceKey(g), body); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *BillingInbox) update(fn func(*mutationTx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(fn)
}

type MigrationStatus struct {
	Version           string    `json:"version"`
	StoreID           string    `json:"store_id"`
	LegacyThrough     uint64    `json:"legacy_through,string"`
	BackfilledThrough uint64    `json:"backfilled_through,string"`
	LegacyAgeFloor    time.Time `json:"legacy_age_floor,omitempty"`
}

func (s *BillingInbox) Migration(ctx context.Context) (MigrationStatus, error) {
	var st MigrationStatus
	if err := s.Health(ctx); err != nil {
		return st, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	err := s.db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket([]byte("meta"))
		st.Version = string(m.Get([]byte("version")))
		st.StoreID = string(m.Get([]byte("identity")))
		st.LegacyThrough = readUint(m.Get([]byte("legacy_through")))
		st.BackfilledThrough = readUint(m.Get([]byte("backfilled_through")))
		st.LegacyAgeFloor, _ = time.Parse(time.RFC3339Nano, string(m.Get([]byte("legacy_age_floor"))))
		return nil
	})
	return st, err
}

// Migrate advances at most batch legacy records. Existing event bytes and
// sequences never change. Legacy receipt age starts only at durable completion.
func (s *BillingInbox) Migrate(ctx context.Context, batch int) (MigrationStatus, error) {
	if batch < 1 || batch > 1000 {
		return MigrationStatus{}, ErrLifecycleConflict
	}
	if err := s.Health(ctx); err != nil {
		return MigrationStatus{}, err
	}
	err := s.update(func(t *mutationTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		m := t.tx.Bucket([]byte("meta"))
		if string(m.Get([]byte("recovery_fence"))) == "1" {
			return ErrBillingUnavailable
		}
		v := string(m.Get([]byte("version")))
		if v == "2" {
			return nil
		}
		if v == "1" {
			for _, b := range []string{"receipt_times", "receipt_ids", "catalog", "archive_cache", "operations", "mutation_journal", "captures"} {
				if err := t.create(b); err != nil {
					return err
				}
			}
			if err := t.put("meta", []byte("version"), []byte("2-migrating")); err != nil {
				return err
			}
			if err := t.put("meta", []byte("legacy_through"), sequenceKey(t.tx.Bucket([]byte("events")).Sequence())); err != nil {
				return err
			}
		}
		end := readUint(m.Get([]byte("legacy_through")))
		at := readUint(m.Get([]byte("backfilled_through")))
		events := t.tx.Bucket([]byte("events"))
		for n := 0; n < batch && at < end; n++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			at++
			body := events.Get(sequenceKey(at))
			r, err := decodeBillingRecord(body, at)
			if err != nil {
				return err
			}
			old := t.tx.Bucket([]byte("ids")).Get([]byte(r.Event.EventID))
			if len(old) != 8 || readUint(old) != at {
				return ErrBillingUnavailable
			}
			binding, _ := json.Marshal(receiptBinding{at, r.ContentSHA256, billingarchive.Digest(body)})
			if err := t.put("ids", []byte(r.Event.EventID), binding); err != nil {
				return err
			}
			if err := t.put("receipt_times", sequenceKey(at), []byte("legacy")); err != nil {
				return err
			}
			if err := t.put("receipt_ids", sequenceKey(at), []byte(r.Event.EventID)); err != nil {
				return err
			}
		}
		if err := t.put("meta", []byte("backfilled_through"), sequenceKey(at)); err != nil {
			return err
		}
		if at == end {
			if err := t.put("meta", []byte("legacy_age_floor"), []byte(time.Now().UTC().Format(time.RFC3339Nano))); err != nil {
				return err
			}
			return t.put("meta", []byte("version"), []byte("2"))
		}
		return nil
	})
	if err != nil {
		return MigrationStatus{}, err
	}
	return s.Migration(ctx)
}
func decodeBillingRecord(body []byte, sequence uint64) (BillingRecord, error) {
	var r BillingRecord
	if body == nil || billingarchive.StrictDecode(bytes.NewReader(body), 2<<20, &r) != nil || r.Sequence != sequence {
		return r, ErrBillingUnavailable
	}
	_, sha, err := billingBinding(r.Event)
	if err != nil || sha != r.ContentSHA256 {
		return r, ErrBillingUnavailable
	}
	return r, nil
}
func receiptTime(tx *bolt.Tx, seq uint64) (time.Time, error) {
	b := tx.Bucket([]byte("receipt_times"))
	if b == nil {
		return time.Time{}, ErrLifecycleConflict
	}
	value := string(b.Get(sequenceKey(seq)))
	if value == "legacy" {
		value = string(tx.Bucket([]byte("meta")).Get([]byte("legacy_age_floor")))
	}
	v, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || v.IsZero() {
		return time.Time{}, ErrBillingUnavailable
	}
	return v.UTC(), nil
}

// ValidateBillingExportRecord is shared with the off-cluster verifier. It
// preserves financial integer precision and recomputes Logger's original hash.
func ValidateBillingExportRecord(e billingarchive.ExportRecord) (BillingRecord, error) {
	r, err := decodeBillingRecord(e.Record, e.Sequence)
	if err != nil || e.ContentSHA256 != r.ContentSHA256 || e.ReceivedAt.IsZero() {
		return r, ErrBillingUnavailable
	}
	return r, nil
}

// ValidateBillingSnapshot audits a private decrypted consistent snapshot.
// It does not open it for writes or initialize a missing database.
func ValidateBillingSnapshot(ctx context.Context, path string, m billingarchive.Manifest, approvedKeys ...map[string]ed25519.PublicKey) error {
	if m.Validate() != nil {
		return ErrBillingUnavailable
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() != m.SnapshotBytes {
		return ErrBillingUnavailable
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(&boundedContextWriter{ctx: ctx, w: h, remaining: billingarchive.MaxSetBytes}, io.LimitReader(f, m.SnapshotBytes+1))
	f.Close()
	if err != nil || n != m.SnapshotBytes || hex.EncodeToString(h.Sum(nil)) != m.SnapshotSHA256 {
		return ErrBillingUnavailable
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond})
	if err != nil {
		return err
	}
	defer db.Close()
	var keys map[string]ed25519.PublicKey
	if len(approvedKeys) > 0 {
		keys = approvedKeys[0]
	}
	return db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		events := tx.Bucket([]byte("events"))
		ids := tx.Bucket([]byte("ids"))
		if meta == nil || events == nil || ids == nil || string(meta.Get([]byte("identity"))) != m.StoreID || string(meta.Get([]byte("version"))) != "2" || events.Sequence() != m.HighWater {
			return ErrBillingUnavailable
		}
		if err := auditLifecycleTx(ctx, tx, m.Environment, m.SourceEventEnvironments, keys); err != nil {
			return err
		}
		floor := readUint(meta.Get([]byte("archive_floor")))
		for seq := floor + 1; seq <= m.HighWater; seq++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			r, err := decodeBillingRecord(events.Get(sequenceKey(seq)), seq)
			if err != nil {
				return err
			}
			var binding receiptBinding
			if json.Unmarshal(ids.Get([]byte(r.Event.EventID)), &binding) != nil || binding.Sequence != seq || binding.ContentSHA256 != r.ContentSHA256 || binding.RecordSHA256 != billingarchive.Digest(events.Get(sequenceKey(seq))) {
				return ErrBillingUnavailable
			}
			if _, err := receiptTime(tx, seq); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BillingInbox) ConfigureLifecycle(c LifecycleConfig) error {
	if len(c.SourceEventEnvironments) == 0 {
		c.SourceEventEnvironments = []string{c.Environment}
	}
	seenSource := map[string]bool{}
	if len(c.SourceEventEnvironments) > 8 {
		return ErrLifecycleConflict
	}
	for _, env := range c.SourceEventEnvironments {
		if !billingarchive.SafeID(env) || seenSource[env] {
			return ErrLifecycleConflict
		}
		seenSource[env] = true
	}
	if !billingarchive.SafeID(c.Environment) || !billingarchive.SafeID(c.Stack) || !billingarchive.SafeID(c.EncryptionKeyID) || !filepath.IsAbs(c.ScratchDir) || len(c.Recipients) == 0 {
		return ErrLifecycleConflict
	}
	if c.RetentionDays == 0 {
		c.RetentionDays = 90
	}
	if c.RetentionDays < 90 || c.RetentionDays > 36500 {
		return errors.New("retention must be at least 90 days")
	}
	if c.PartBytes == 0 {
		c.PartBytes = billingarchive.DefaultPartBytes
	}
	if c.PartBytes < 1<<20 || c.PartBytes > billingarchive.DefaultPartBytes {
		return ErrLifecycleConflict
	}
	if c.ScratchCapacityBytes < 4<<30 {
		return errors.New("explicit scratch volume budget of at least 4 GiB required")
	}
	if c.CacheMaxBytes == 0 {
		c.CacheMaxBytes = 1 << 30
	}
	if c.CacheMaxBytes < 2<<20 || c.CacheMaxBytes > 64<<30 {
		return errors.New("archive cache budget outside bounded range")
	}
	if c.CaptureInterval == 0 {
		c.CaptureInterval = 12 * time.Hour
	}
	if c.CaptureInterval < time.Minute || c.CaptureInterval > 24*time.Hour {
		return ErrLifecycleConflict
	}
	if err := privateDir(c.ScratchDir); err != nil {
		return err
	}
	if len(c.Recipients) > 16 {
		return ErrLifecycleConflict
	}
	seenRecipients := map[string]bool{}
	for _, recipient := range c.Recipients {
		if _, err := age.ParseX25519Recipient(recipient); err != nil || seenRecipients[recipient] {
			return errors.New("unique public-only age recipients required")
		}
		seenRecipients[recipient] = true
	}
	c.keys = map[string]ed25519.PublicKey{}
	for k, v := range c.VerifierKeys {
		key, err := decodePublicKey(v)
		if err != nil || !billingarchive.SafeID(k) {
			return ErrLifecycleConflict
		}
		c.keys[k] = key
	}
	if len(c.keys) == 0 {
		return errors.New("approved verifier public key required")
	}
	c.recoveryKeys = map[string]ed25519.PublicKey{}
	for k, v := range c.RecoveryKeys {
		key, err := decodePublicKey(v)
		if err != nil || !billingarchive.SafeID(k) {
			return ErrLifecycleConflict
		}
		if _, exists := c.VerifierKeys[k]; exists {
			return errors.New("recovery key ID must be distinct from verifier role")
		}
		for _, verifyKey := range c.keys {
			if bytes.Equal(key, verifyKey) {
				return errors.New("recovery approval key must differ from verification-only key")
			}
		}
		c.recoveryKeys[k] = key
	}
	if c.AuthorityURL != "" {
		u, err := url.Parse(c.AuthorityURL)
		if err != nil || !qualifiedAuthorityURL(u) || c.AuthorityToken == "" {
			return errors.New("private HTTPS authority and credential required")
		}
		c.AuthorityURL = strings.TrimSuffix(c.AuthorityURL, "/")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lifecycle = c
	return nil
}
func privateDir(p string) error {
	i, err := os.Lstat(p)
	if err != nil || !i.IsDir() || i.Mode().Perm()&0077 != 0 {
		return ErrBillingUnavailable
	}
	return nil
}
func (s *BillingInbox) ready(tx *bolt.Tx) error {
	m := tx.Bucket([]byte("meta"))
	if string(m.Get([]byte("recovery_fence"))) == "1" {
		return ErrBillingUnavailable
	}
	if string(m.Get([]byte("version"))) != "2" || s.lifecycle.Environment == "" {
		return ErrLifecycleConflict
	}
	return nil
}

type RetirementPlan struct {
	OperationID     string `json:"operation_id"`
	Environment     string `json:"environment"`
	StoreID         string `json:"store_id"`
	FromSequence    uint64 `json:"from_sequence,string"`
	ThroughSequence uint64 `json:"through_sequence,string"`
	PlanSHA256      string `json:"plan_sha256"`
	SetID           string `json:"set_id"`
}
type RetirementStatus struct {
	OperationID     string    `json:"operation_id"`
	Environment     string    `json:"environment"`
	StoreID         string    `json:"store_id"`
	FromSequence    uint64    `json:"from_sequence,string"`
	ThroughSequence uint64    `json:"through_sequence,string"`
	PlanSHA256      string    `json:"plan_sha256"`
	Status          string    `json:"status"`
	ReceiptSHA256   string    `json:"receipt_sha256"`
	CompletedAt     time.Time `json:"completed_at"`
	RetiredThrough  uint64    `json:"retired_through,string"`
	SetID           string    `json:"set_id"`
}

func statusFor(p RetirementPlan) RetirementStatus {
	return RetirementStatus{OperationID: p.OperationID, Environment: p.Environment, StoreID: p.StoreID, FromSequence: p.FromSequence, ThroughSequence: p.ThroughSequence, PlanSHA256: p.PlanSHA256, Status: "pending", SetID: p.SetID}
}
func sealStatus(st *RetirementStatus) {
	st.ReceiptSHA256 = ""
	b, _ := json.Marshal(st)
	st.ReceiptSHA256 = billingarchive.Digest(b)
}
func (s *BillingInbox) Retirement(ctx context.Context, id string) (RetirementStatus, error) {
	var st RetirementStatus
	if !billingarchive.SafeID(id) {
		return st, ErrLifecycleConflict
	}
	if err := s.Health(ctx); err != nil {
		return st, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("operations"))
		if b == nil || b.Get([]byte(id)) == nil {
			return ErrOperationNotFound
		}
		return json.Unmarshal(b.Get([]byte(id)), &st)
	})
	return st, err
}
func (s *BillingInbox) PlanRetirement(ctx context.Context, p RetirementPlan) (RetirementStatus, error) {
	var st RetirementStatus
	if !s.lifecycle.RetirementEnabled {
		return st, ErrLifecycleConflict
	}
	if !billingarchive.SafeID(p.OperationID) || !billingarchive.SafeID(p.SetID) || !validDigest(p.PlanSHA256) || p.FromSequence == 0 || p.ThroughSequence < p.FromSequence || p.ThroughSequence-p.FromSequence >= 1000 {
		return st, ErrLifecycleConflict
	}
	err := s.update(func(t *mutationTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ready(t.tx); err != nil {
			return err
		}
		meta := t.tx.Bucket([]byte("meta"))
		if p.Environment != s.lifecycle.Environment || p.StoreID != string(meta.Get([]byte("identity"))) {
			return ErrLifecycleConflict
		}
		b := t.tx.Bucket([]byte("operations"))
		if old := b.Get([]byte(p.OperationID)); old != nil {
			if json.Unmarshal(old, &st) != nil {
				return ErrBillingUnavailable
			}
			want := statusFor(p)
			want.Status = st.Status
			want.CompletedAt = st.CompletedAt
			want.ReceiptSHA256 = st.ReceiptSHA256
			want.RetiredThrough = st.RetiredThrough
			if !equalJSON(st, want) {
				return ErrLifecycleConflict
			}
			return nil
		}
		floor := readUint(meta.Get([]byte("archive_floor")))
		if p.FromSequence != floor+1 || p.ThroughSequence > t.tx.Bucket([]byte("events")).Sequence() {
			return ErrLifecycleConflict
		}
		if err := s.retirementCoverage(t.tx, p.SetID, p.FromSequence, p.ThroughSequence); err != nil {
			return err
		}
		st = statusFor(p)
		st.RetiredThrough = floor
		body, _ := json.Marshal(st)
		return t.put("operations", []byte(p.OperationID), body)
	})
	return st, err
}
func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}
func (s *BillingInbox) retirementCoverage(tx *bolt.Tx, set string, from, through uint64) error {
	var c CatalogEntry
	b := tx.Bucket([]byte("catalog")).Get([]byte(set))
	if b == nil || json.Unmarshal(b, &c) != nil || c.Status != "verified" || c.Manifest.FromSequence > from || c.Manifest.ThroughSequence < through {
		return ErrLifecycleConflict
	}
	if _, err := billingarchive.VerifyCompletion(c.Completion, c.Manifest, c.ManifestBytes, s.lifecycle.keys); err != nil {
		return ErrLifecycleConflict
	}
	cut := time.Now().UTC().Add(-time.Duration(s.lifecycle.RetentionDays) * 24 * time.Hour)
	for seq := from; seq <= through; seq++ {
		at, err := receiptTime(tx, seq)
		if err != nil || at.After(cut) {
			return ErrLifecycleConflict
		}
		r, err := decodeBillingRecord(tx.Bucket([]byte("events")).Get(sequenceKey(seq)), seq)
		if err != nil {
			return err
		}
		var bind receiptBinding
		if json.Unmarshal(tx.Bucket([]byte("ids")).Get([]byte(r.Event.EventID)), &bind) != nil || bind.Sequence != seq || bind.ContentSHA256 != r.ContentSHA256 || bind.RecordSHA256 != billingarchive.Digest(tx.Bucket([]byte("events")).Get(sequenceKey(seq))) {
			return ErrBillingUnavailable
		}
	}
	return nil
}
func (s *BillingInbox) validateAuthority(ctx context.Context, st RetirementStatus, expected string) error {
	if s.lifecycle.authority != nil {
		return s.lifecycle.authority(ctx, st)
	}
	if s.lifecycle.AuthorityURL == "" || s.lifecycle.AuthorityToken == "" {
		return ErrLifecycleConflict
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.lifecycle.AuthorityURL+"/v1/internal/billing/raw-retention/operations/"+url.PathEscape(st.OperationID), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.lifecycle.AuthorityToken)
	client := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return ErrLifecycleConflict
	}
	var authority struct {
		OperationID     string `json:"operation_id"`
		Environment     string `json:"environment"`
		StoreID         string `json:"store_id"`
		FromSequence    uint64 `json:"from_sequence,string"`
		ThroughSequence uint64 `json:"through_sequence,string"`
		PlanSHA256      string `json:"plan_sha256"`
		SetID           string `json:"set_id"`
		Status          string `json:"status"`
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil || json.Unmarshal(body, &authority) != nil {
		return ErrLifecycleConflict
	}
	if authority.Status != expected || authority.OperationID != st.OperationID || authority.Environment != st.Environment || authority.StoreID != st.StoreID || authority.FromSequence != st.FromSequence || authority.ThroughSequence != st.ThroughSequence || authority.PlanSHA256 != st.PlanSHA256 || authority.SetID != st.SetID {
		return ErrLifecycleConflict
	}
	return nil
}
func (s *BillingInbox) ApplyRetirement(ctx context.Context, id string) (RetirementStatus, error) {
	st, err := s.Retirement(ctx, id)
	if err != nil {
		return st, err
	}
	if st.Status == "completed" {
		return st, nil
	}
	if st.Status != "pending" {
		return st, ErrLifecycleConflict
	}
	if !s.lifecycle.RetirementEnabled {
		return st, ErrLifecycleConflict
	}
	if err := s.validateAuthority(ctx, st, "ACTIVE"); err != nil {
		return st, err
	}
	err = s.update(func(t *mutationTx) error {
		if err := s.ready(t.tx); err != nil {
			return err
		}
		var live RetirementStatus
		if json.Unmarshal(t.tx.Bucket([]byte("operations")).Get([]byte(id)), &live) != nil || live.Status != "pending" || !equalJSON(st, live) {
			return ErrLifecycleConflict
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		floor := readUint(t.tx.Bucket([]byte("meta")).Get([]byte("archive_floor")))
		if floor+1 != st.FromSequence {
			return ErrLifecycleConflict
		}
		if err := s.retirementCoverage(t.tx, st.SetID, st.FromSequence, st.ThroughSequence); err != nil {
			return err
		}
		for seq := st.FromSequence; seq <= st.ThroughSequence; seq++ {
			if err := t.del("events", sequenceKey(seq)); err != nil {
				return err
			}
		}
		if err := t.put("meta", []byte("archive_floor"), sequenceKey(st.ThroughSequence)); err != nil {
			return err
		}
		st.Status = "completed"
		st.CompletedAt = time.Now().UTC()
		st.RetiredThrough = st.ThroughSequence
		sealStatus(&st)
		body, _ := json.Marshal(st)
		return t.put("operations", []byte(id), body)
	})
	return st, err
}
func (s *BillingInbox) AbortRetirement(ctx context.Context, id string) (RetirementStatus, error) {
	st, err := s.Retirement(ctx, id)
	if err != nil {
		return st, err
	}
	return s.AbortRetirementPlan(ctx, RetirementPlan{OperationID: st.OperationID, Environment: st.Environment, StoreID: st.StoreID, FromSequence: st.FromSequence, ThroughSequence: st.ThroughSequence, PlanSHA256: st.PlanSHA256, SetID: st.SetID})
}

// AbortRetirementPlan also creates a permanent bound tombstone when apply/plan
// has never reached Logger. Billing's live ABORT_REQUESTED fence is mandatory.
// It deliberately remains available when RetirementEnabled is false: stopping
// payload deletion must not strand an authority's already-durable fence.
func (s *BillingInbox) AbortRetirementPlan(ctx context.Context, p RetirementPlan) (RetirementStatus, error) {
	st, err := s.Retirement(ctx, p.OperationID)
	if err == nil {
		want := statusFor(p)
		want.RetiredThrough = st.RetiredThrough
		want.Status = st.Status
		want.CompletedAt = st.CompletedAt
		want.ReceiptSHA256 = st.ReceiptSHA256
		if !equalJSON(want, st) {
			return st, ErrLifecycleConflict
		}
		if st.Status == "completed" || st.Status == "aborted" {
			return st, nil
		}
	} else if !errors.Is(err, ErrOperationNotFound) {
		return st, err
	} else {
		st = statusFor(p)
	}
	if !billingarchive.SafeID(p.OperationID) || !billingarchive.SafeID(p.SetID) || !validDigest(p.PlanSHA256) || p.FromSequence == 0 || p.ThroughSequence < p.FromSequence || p.ThroughSequence-p.FromSequence >= 1000 {
		return st, ErrLifecycleConflict
	}
	if err := s.validateAuthority(ctx, st, "ABORT_REQUESTED"); err != nil {
		return st, err
	}
	err = s.update(func(t *mutationTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ready(t.tx); err != nil {
			return err
		}
		if p.Environment != s.lifecycle.Environment || p.StoreID != string(t.tx.Bucket([]byte("meta")).Get([]byte("identity"))) {
			return ErrLifecycleConflict
		}
		if old := t.tx.Bucket([]byte("operations")).Get([]byte(p.OperationID)); old != nil {
			if json.Unmarshal(old, &st) != nil {
				return ErrBillingUnavailable
			}
			if st.Status == "completed" || st.Status == "aborted" {
				return nil
			}
		}
		st.Status = "aborted"
		st.CompletedAt = time.Now().UTC()
		st.RetiredThrough = readUint(t.tx.Bucket([]byte("meta")).Get([]byte("archive_floor")))
		sealStatus(&st)
		body, _ := json.Marshal(st)
		return t.put("operations", []byte(p.OperationID), body)
	})
	return st, err
}

func syncDir(p string) error {
	f, e := os.Open(p)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func exclusiveFile(path string, data []byte) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(data)
	if e == nil {
		e = f.Sync()
	}
	c := f.Close()
	if e == nil {
		e = c
	}
	return e
}
func decodePublicKey(v string) (ed25519.PublicKey, error) { return decodeVerifierKey(v) }

func qualifiedAuthorityURL(u *url.URL) bool {
	if u == nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback()) || (strings.HasSuffix(host, ".svc.cluster.local") && len(strings.Split(host, ".")) >= 5)
}
