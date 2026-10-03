package cloudlogger

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

type ArchiveObjectStore interface {
	PutImmutable(context.Context, string, string, int64, string) error
}
type CaptureStatus struct {
	SetID       string                  `json:"set_id"`
	Status      string                  `json:"status"`
	Manifest    billingarchive.Manifest `json:"manifest"`
	ManifestB64 string                  `json:"manifest_b64,omitempty"`
	Error       string                  `json:"error,omitempty"`
}
type CatalogEntry struct {
	Status        string                          `json:"status"`
	Manifest      billingarchive.Manifest         `json:"manifest"`
	ManifestBytes []byte                          `json:"manifest_bytes"`
	Completion    billingarchive.SignedCompletion `json:"completion"`
}
type WorkerStatus struct {
	Environment                   string    `json:"environment"`
	StoreID                       string    `json:"store_id"`
	HighWater                     uint64    `json:"high_water,string"`
	RetiredThrough                uint64    `json:"retired_through,string"`
	State                         string    `json:"state"`
	LastAttempt                   time.Time `json:"last_attempt"`
	LastVerified                  time.Time `json:"last_verified"`
	LastProtectedAt               time.Time `json:"last_protected_at"`
	EarliestUnprotectedReceivedAt time.Time `json:"earliest_unprotected_received_at"`
	LastError                     string    `json:"last_error,omitempty"`
	ProtectedHighWater            uint64    `json:"protected_high_water,string"`
	ProtectionOverdue             bool      `json:"protection_overdue"`
}

func SnapshotPrefix(m billingarchive.Manifest) string {
	return fmt.Sprintf("billing-inbox-snapshots/%s/%s/%s/%s", m.Stack, m.StoreID, m.CreatedAt.UTC().Format("2006/01/02"), m.SetID)
}
func ArchivePartKey(m billingarchive.Manifest, p billingarchive.Part) string {
	prefix := SnapshotPrefix(m)
	if p.Kind == "events" {
		prefix = fmt.Sprintf("billing-raw/%s/%s/%s/%s", m.Stack, m.StoreID, m.CreatedAt.UTC().Format("2006/01/02"), m.SetID)
	}
	return prefix + "/" + p.Name
}
func decodeVerifierKey(v string) ([]byte, error) {
	b, e := base64.StdEncoding.DecodeString(v)
	if e != nil || len(b) != 32 {
		return nil, ErrLifecycleConflict
	}
	return b, nil
}
func (s *BillingInbox) Captures(ctx context.Context) ([]CaptureStatus, error) {
	out := []CaptureStatus{}
	if err := s.Health(ctx); err != nil {
		return out, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("captures"))
		if b == nil {
			return ErrLifecycleConflict
		}
		return b.ForEach(func(k, v []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var st CaptureStatus
			if json.Unmarshal(v, &st) != nil {
				return ErrBillingUnavailable
			}
			out = append(out, st)
			return nil
		})
	})
	return out, err
}
func (s *BillingInbox) saveCapture(st CaptureStatus) error {
	return s.update(func(t *mutationTx) error {
		b, e := json.Marshal(st)
		if e != nil {
			return e
		}
		return t.put("captures", []byte(st.SetID), b)
	})
}

// Capture seals immutable independently encrypted parts before any upload. A
// sealed set is resumed byte-for-byte; a pre-seal crash abandons that set ID.
func (s *BillingInbox) Capture(ctx context.Context) (CaptureStatus, error) {
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	s.workerMu.Lock()
	s.workerStatus.State = "capturing"
	s.workerStatus.LastAttempt = time.Now().UTC()
	s.workerMu.Unlock()
	defer func() { s.workerMu.Lock(); s.workerStatus.State = "idle"; s.workerMu.Unlock() }()
	var st CaptureStatus
	if !s.lifecycle.BackupEnabled {
		return st, ErrLifecycleConflict
	}
	captures, err := s.Captures(ctx)
	if err != nil {
		return st, err
	}
	for _, p := range captures {
		if p.Status == "sealed" || p.Status == "uploaded" {
			if p.Status == "uploaded" {
				return p, nil
			}
			return s.uploadCapture(ctx, p)
		}
		if p.Status == "capturing" {
			p.Status = "abandoned"
			p.Error = "interrupted before immutable seal"
			if err := s.saveCapture(p); err != nil {
				return p, err
			}
			if err := s.cleanupScratch(p, true); err != nil {
				return p, err
			}
		}
		if p.Status == "abandoned" {
			if err := s.cleanupScratch(p, true); err != nil {
				return p, err
			}
		}
		if p.Status == "verified" {
			if err := s.cleanupScratch(p, false); err != nil {
				return p, err
			}
		}
	}
	c := s.lifecycle
	if c.Environment == "" {
		return st, ErrLifecycleConflict
	}
	if err := privateDir(c.ScratchDir); err != nil {
		return st, err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return st, err
	}
	st.SetID = hex.EncodeToString(random)
	st.Status = "capturing"
	dir := filepath.Join(c.ScratchDir, st.SetID)
	if err := os.Mkdir(dir, 0700); err != nil {
		return st, err
	}
	if err := syncDir(c.ScratchDir); err != nil {
		return st, err
	}
	if err := s.saveCapture(st); err != nil {
		return st, err
	}
	fail := func(err error) (CaptureStatus, error) {
		st.Error = err.Error()
		s.workerMu.Lock()
		s.workerStatus.LastError = st.Error
		s.workerMu.Unlock()
		return st, err
	}
	s.mu.RLock()
	tx, err := s.db.Begin(false)
	s.mu.RUnlock()
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()
	if err := s.ready(tx); err != nil {
		return fail(err)
	}
	meta := tx.Bucket([]byte("meta"))
	high := tx.Bucket([]byte("events")).Sequence()
	from := readUint(meta.Get([]byte("captured_through"))) + 1
	floor := readUint(meta.Get([]byte("archive_floor")))
	if from <= floor {
		from = floor + 1
	}
	if from > high {
		from = 0
	}
	m := billingarchive.Manifest{Version: billingarchive.Version, Environment: c.Environment, SourceEventEnvironments: c.SourceEventEnvironments, Stack: c.Stack, StoreID: string(meta.Get([]byte("identity"))), SetID: st.SetID, CreatedAt: time.Now().UTC(), HighWater: high, FromSequence: from, EncryptionKeyID: c.EncryptionKeyID, RecipientFingerprints: []string{}}
	for _, recipient := range c.Recipients {
		m.RecipientFingerprints = append(m.RecipientFingerprints, billingarchive.Fingerprint(recipient))
	}
	if from > 0 {
		m.ThroughSequence = high
		m.RecordCount = high - from + 1
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(c.ScratchDir, &fs); err != nil {
		return fail(err)
	}
	capacity := c.ScratchCapacityBytes
	free := int64(fs.Bavail) * int64(fs.Bsize)
	var used int64
	if err := filepath.WalkDir(c.ScratchDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return ErrBillingUnavailable
		}
		if !d.IsDir() {
			i, err := d.Info()
			if err != nil {
				return err
			}
			used += i.Size()
		}
		return nil
	}); err != nil {
		return fail(err)
	}
	if capacity-used < free {
		free = capacity - used
	}
	snapshotSize := tx.Size()
	reserve := capacity / 5
	if reserve < 2<<30 {
		reserve = 2 << 30
	}
	rawBound := int64(0)
	if m.RecordCount > 0 {
		if m.RecordCount > uint64(billingarchive.MaxSetBytes/512) {
			return fail(errors.New("export receipt count exceeds bounded set"))
		}
		rawBound = int64(tx.Bucket([]byte("events")).Stats().LeafInuse) + int64(m.RecordCount)*512
	}
	if snapshotSize > billingarchive.MaxSetBytes || rawBound > billingarchive.MaxSetBytes-snapshotSize {
		return fail(errors.New("snapshot and export exceed bounded set"))
	}
	snapshotParts := (snapshotSize + c.PartBytes - 1) / c.PartBytes
	rawParts := int64(0)
	if rawBound > 0 {
		if c.PartBytes <= 2<<20 {
			rawParts = int64(m.RecordCount)
		} else {
			rawParts = (rawBound + c.PartBytes - (2 << 20) - 1) / (c.PartBytes - (2 << 20))
		}
	}
	if snapshotParts+rawParts > billingarchive.MaxParts {
		return fail(errors.New("bounded part count requires larger part size or smaller capture"))
	}
	estimate := snapshotSize + (snapshotParts+rawParts)*billingarchive.MaxCipherPartBytes + reserve
	if free < estimate {
		return fail(errors.New("insufficient independent scratch capacity"))
	}
	snapshotPath := filepath.Join(dir, "snapshot.db")
	file, err := os.OpenFile(snapshotPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fail(err)
	}
	h := sha256.New()
	n, err := tx.WriteTo(&boundedContextWriter{ctx: ctx, w: io.MultiWriter(file, h), remaining: billingarchive.MaxSetBytes})
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fail(err)
	}
	m.SnapshotBytes = n
	m.SnapshotSHA256 = hex.EncodeToString(h.Sum(nil))
	if err := tx.Rollback(); err != nil {
		return fail(err)
	}
	snapshotDB, err := bolt.Open(snapshotPath, 0600, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond})
	if err != nil {
		return fail(err)
	}
	defer snapshotDB.Close()
	tx, err = snapshotDB.Begin(false)
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return fail(err)
	}
	for offset, ordinal := int64(0), 1; offset < n; ordinal++ {
		length := c.PartBytes
		if n-offset < length {
			length = n - offset
		}
		p, err := sealPart(ctx, dir, "snapshot", ordinal, offset, io.LimitReader(snapshot, length), length, c.Recipients)
		if err != nil {
			snapshot.Close()
			return fail(err)
		}
		m.Parts = append(m.Parts, p)
		offset += length
	}
	snapshot.Close()
	bindHash := sha256.New()
	_, _ = bindHash.Write([]byte("["))
	var raw bytes.Buffer
	rawOffset := int64(0)
	ordinal := 1
	flush := func() error {
		if raw.Len() == 0 {
			return nil
		}
		p, err := sealPart(ctx, dir, "events", ordinal, rawOffset, bytes.NewReader(raw.Bytes()), int64(raw.Len()), c.Recipients)
		if err != nil {
			return err
		}
		m.Parts = append(m.Parts, p)
		rawOffset += int64(raw.Len())
		ordinal++
		raw.Reset()
		return nil
	}
	if from > 0 {
		for seq := from; seq <= high; seq++ {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			body := bytes.Clone(tx.Bucket([]byte("events")).Get(sequenceKey(seq)))
			r, err := decodeBillingRecord(body, seq)
			if err != nil {
				return fail(err)
			}
			if !sourceEnvironmentAllowed(r.Event.Env, m.SourceEventEnvironments) {
				return fail(ErrLifecycleConflict)
			}
			at, err := receiptTime(tx, seq)
			if err != nil {
				return fail(err)
			}
			if at.After(m.CreatedAt) {
				return fail(ErrBillingUnavailable)
			}
			usageRaw, err := json.Marshal(r.Event.Fields["usage_event"])
			if err != nil {
				return fail(err)
			}
			ref, err := billingarchive.CanonicalUsageBinding(seq, r.ContentSHA256, usageRaw)
			if err != nil {
				return fail(err)
			}
			binding, _ := json.Marshal(ref)
			if seq != from {
				_, _ = bindHash.Write([]byte(","))
			}
			_, _ = bindHash.Write(binding)
			if at.After(m.MaxReceivedAt) {
				m.MaxReceivedAt = at
			}
			line, err := json.Marshal(billingarchive.ExportRecord{Sequence: seq, ContentSHA256: r.ContentSHA256, ReceivedAt: at, Record: body})
			if err != nil {
				return fail(err)
			}
			line = append(line, '\n')
			if int64(len(line)) > c.PartBytes {
				return fail(errors.New("record exceeds configured part size"))
			}
			if int64(raw.Len()+len(line)) > c.PartBytes {
				if err := flush(); err != nil {
					return fail(err)
				}
			}
			_, _ = raw.Write(line)
		}
	}
	if err := flush(); err != nil {
		return fail(err)
	}
	_, _ = bindHash.Write([]byte("]"))
	m.RecordsBindingsSHA256 = hex.EncodeToString(bindHash.Sum(nil))
	manifestBytes, err := billingarchive.MarshalManifest(m)
	if err != nil {
		return fail(err)
	}
	if err := exclusiveFile(filepath.Join(dir, "manifest.json"), manifestBytes); err != nil {
		return fail(err)
	}
	if err := syncDir(dir); err != nil {
		return fail(err)
	}
	st.Status = "sealed"
	st.Manifest = m
	st.ManifestB64 = base64.StdEncoding.EncodeToString(manifestBytes)
	// A bbolt write may need to remap. Never retain this read transaction while
	// committing the seal to the same database (that would self-deadlock).
	if err := tx.Rollback(); err != nil {
		return fail(err)
	}
	if err := snapshotDB.Close(); err != nil {
		return fail(err)
	}
	if err := s.update(func(t *mutationTx) error {
		body, _ := json.Marshal(st)
		if err := t.put("captures", []byte(st.SetID), body); err != nil {
			return err
		}
		return t.put("meta", []byte("captured_through"), sequenceKey(high))
	}); err != nil {
		return fail(err)
	}
	if err := os.Remove(snapshotPath); err != nil {
		return fail(err)
	}
	if err := syncDir(dir); err != nil {
		return fail(err)
	}
	return s.uploadCapture(ctx, st)
}
func sealPart(ctx context.Context, dir, kind string, ordinal int, offset int64, r io.Reader, size int64, recipients []string) (billingarchive.Part, error) {
	p := billingarchive.Part{Name: fmt.Sprintf("%s-%06d.zst.age", kind, ordinal), Kind: kind, Ordinal: ordinal, Offset: offset}
	file, err := os.OpenFile(filepath.Join(dir, p.Name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return p, err
	}
	h := sha256.New()
	count := &countWriter{w: io.MultiWriter(file, h)}
	p.PlainBytes, p.PlainSHA256, err = billingarchive.EncodePart(ctx, count, r, recipients, size)
	if err == nil {
		err = file.Sync()
	}
	ce := file.Close()
	if err == nil {
		err = ce
	}
	p.CipherBytes = count.n
	p.CipherSHA256 = hex.EncodeToString(h.Sum(nil))
	return p, err
}

type countWriter struct {
	w io.Writer
	n int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, e := w.w.Write(p)
	w.n += int64(n)
	return n, e
}

type boundedContextWriter struct {
	ctx       context.Context
	w         io.Writer
	remaining int64
}

func (w *boundedContextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > w.remaining {
		return 0, errors.New("snapshot exceeds bound")
	}
	n, e := w.w.Write(p)
	w.remaining -= int64(n)
	return n, e
}
func (s *BillingInbox) uploadCapture(ctx context.Context, st CaptureStatus) (CaptureStatus, error) {
	if s.lifecycle.ObjectStore == nil {
		return st, errors.New("object storage writer not configured")
	}
	dir := filepath.Join(s.lifecycle.ScratchDir, st.SetID)
	for _, p := range st.Manifest.Parts {
		if err := s.lifecycle.ObjectStore.PutImmutable(ctx, ArchivePartKey(st.Manifest, p), filepath.Join(dir, p.Name), p.CipherBytes, p.CipherSHA256); err != nil {
			st.Error = err.Error()
			s.workerMu.Lock()
			s.workerStatus.LastError = st.Error
			s.workerMu.Unlock()
			return st, err
		}
	}
	b, err := base64.StdEncoding.DecodeString(st.ManifestB64)
	if err != nil {
		return st, err
	}
	if err := s.lifecycle.ObjectStore.PutImmutable(ctx, SnapshotPrefix(st.Manifest)+"/manifest.json", filepath.Join(dir, "manifest.json"), int64(len(b)), billingarchive.Digest(b)); err != nil {
		return st, err
	}
	st.Status = "uploaded"
	st.Error = ""
	return st, s.saveCapture(st)
}
func (s *BillingInbox) VerifyCatalog(ctx context.Context, setID string, completion billingarchive.SignedCompletion) (CatalogEntry, error) {
	var entry CatalogEntry
	err := s.update(func(t *mutationTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ready(t.tx); err != nil {
			return err
		}
		var st CaptureStatus
		if json.Unmarshal(t.tx.Bucket([]byte("captures")).Get([]byte(setID)), &st) != nil || (st.Status != "uploaded" && st.Status != "verified") {
			return ErrLifecycleConflict
		}
		b, err := base64.StdEncoding.DecodeString(st.ManifestB64)
		if err != nil {
			return err
		}
		proof, err := billingarchive.VerifyCompletion(completion, st.Manifest, b, s.lifecycle.keys)
		if err != nil || proof.VerifiedAt.After(time.Now().UTC().Add(30*time.Second)) || st.Manifest.CreatedAt.After(time.Now().UTC().Add(30*time.Second)) {
			return ErrLifecycleConflict
		}
		entry = CatalogEntry{Status: "verified", Manifest: st.Manifest, ManifestBytes: b, Completion: completion}
		if existing := t.tx.Bucket([]byte("catalog")).Get([]byte(setID)); existing != nil {
			var old CatalogEntry
			if json.Unmarshal(existing, &old) != nil || !equalJSON(old, entry) {
				return ErrLifecycleConflict
			}
			return nil
		}
		body, _ := json.Marshal(entry)
		if err := t.put("catalog", []byte(setID), body); err != nil {
			return err
		}
		st.Status = "verified"
		body, _ = json.Marshal(st)
		return t.put("captures", []byte(setID), body)
	})
	if err != nil {
		return entry, err
	}
	b, _ := json.Marshal(completion)
	dir := filepath.Join(s.lifecycle.ScratchDir, setID)
	p := filepath.Join(dir, "complete.json")
	if _, err := os.Lstat(p); os.IsNotExist(err) {
		if err := exclusiveFile(p, b); err != nil {
			return entry, err
		}
		if err := syncDir(dir); err != nil {
			return entry, err
		}
	}
	if s.lifecycle.ObjectStore != nil {
		if err := s.lifecycle.ObjectStore.PutImmutable(ctx, SnapshotPrefix(entry.Manifest)+"/complete.json", p, int64(len(b)), billingarchive.Digest(b)); err != nil {
			return entry, err
		}
	}
	s.workerMu.Lock()
	s.workerStatus.LastVerified = time.Now().UTC()
	s.workerStatus.ProtectedHighWater = entry.Manifest.HighWater
	s.workerStatus.LastError = ""
	s.workerMu.Unlock()
	if err := s.cleanupScratch(CaptureStatus{SetID: setID, Status: "verified", Manifest: entry.Manifest}, false); err != nil {
		return entry, err
	}
	return entry, nil
}

// Only task-generated files in the exact recorded set directory are removed.
// A sealed pending set is never cleaned; cloud originals are never deleted.
func (s *BillingInbox) cleanupScratch(st CaptureStatus, abandoned bool) error {
	if !billingarchive.SafeID(st.SetID) || (abandoned && st.Status != "abandoned") || (!abandoned && st.Status != "verified") {
		return ErrLifecycleConflict
	}
	dir := filepath.Join(s.lifecycle.ScratchDir, st.SetID)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := privateDir(dir); err != nil {
		return err
	}
	generated := func(name string) bool {
		return name == "snapshot.db" || name == "manifest.json" || name == "complete.json" || (billingarchive.SafeID(name) && strings.HasSuffix(name, ".zst.age") && (strings.HasPrefix(name, "snapshot-") || strings.HasPrefix(name, "events-")))
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !generated(e.Name()) {
			return ErrBillingUnavailable
		}
	}
	for _, e := range entries {
		if !abandoned && (e.Name() == "manifest.json" || e.Name() == "complete.json") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	if abandoned {
		if err := os.Remove(dir); err != nil {
			return err
		}
		return syncDir(s.lifecycle.ScratchDir)
	}
	return nil
}

// Rehydrate installs only exact original bytes covered by a trusted catalog.
// It never changes StoreID, receipt sequence, dedupe bindings or hot floor.
func (s *BillingInbox) Rehydrate(ctx context.Context, setID string, records []billingarchive.ExportRecord) error {
	if len(records) < 1 || len(records) > 1000 {
		return ErrLifecycleConflict
	}
	return s.update(func(t *mutationTx) error {
		if err := s.ready(t.tx); err != nil {
			return err
		}
		var entry CatalogEntry
		if json.Unmarshal(t.tx.Bucket([]byte("catalog")).Get([]byte(setID)), &entry) != nil {
			return ErrLifecycleConflict
		}
		if _, err := billingarchive.VerifyCompletion(entry.Completion, entry.Manifest, entry.ManifestBytes, s.lifecycle.keys); err != nil {
			return err
		}
		floor := readUint(t.tx.Bucket([]byte("meta")).Get([]byte("archive_floor")))
		cache := t.tx.Bucket([]byte("archive_cache"))
		cacheBytes := readUint(t.tx.Bucket([]byte("meta")).Get([]byte("cache_bytes")))
		for i, e := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.Sequence < entry.Manifest.FromSequence || e.Sequence > entry.Manifest.ThroughSequence || e.Sequence > floor || (i > 0 && e.Sequence != records[i-1].Sequence+1) {
				return ErrLifecycleConflict
			}
			r, err := ValidateBillingExportRecord(e)
			if err != nil {
				return err
			}
			var bind receiptBinding
			if json.Unmarshal(t.tx.Bucket([]byte("ids")).Get([]byte(r.Event.EventID)), &bind) != nil || bind.Sequence != e.Sequence || bind.ContentSHA256 != e.ContentSHA256 || bind.RecordSHA256 != billingarchive.Digest(e.Record) {
				return ErrLifecycleConflict
			}
			at, err := receiptTime(t.tx, e.Sequence)
			if err != nil || !at.Equal(e.ReceivedAt) {
				return ErrLifecycleConflict
			}
			previous := cache.Get(sequenceKey(e.Sequence))
			if uint64(len(previous)) > cacheBytes {
				return ErrBillingUnavailable
			}
			cacheBytes -= uint64(len(previous))
			cacheBytes += uint64(len(e.Record))
			if cacheBytes > uint64(s.lifecycle.CacheMaxBytes) {
				return ErrLifecycleConflict
			}
			if err := t.put("archive_cache", sequenceKey(e.Sequence), e.Record); err != nil {
				return err
			}
		}
		return t.put("meta", []byte("cache_bytes"), sequenceKey(cacheBytes))
	})
}
func (s *BillingInbox) EvictCache(ctx context.Context, through uint64) error {
	return s.update(func(t *mutationTx) error {
		if err := s.ready(t.tx); err != nil {
			return err
		}
		c := t.tx.Bucket([]byte("archive_cache")).Cursor()
		cacheBytes := readUint(t.tx.Bucket([]byte("meta")).Get([]byte("cache_bytes")))
		count := 0
		for k, v := c.First(); k != nil && readUint(k) <= through && count < 1000; k, v = c.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := t.del("archive_cache", k); err != nil {
				return err
			}
			if uint64(len(v)) > cacheBytes {
				return ErrBillingUnavailable
			}
			cacheBytes -= uint64(len(v))
			count++
		}
		return t.put("meta", []byte("cache_bytes"), sequenceKey(cacheBytes))
	})
}
func (s *BillingInbox) Worker(ctx context.Context) WorkerStatus {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	st := s.workerStatus
	st.LastVerified = time.Time{}
	st.LastProtectedAt = time.Time{}
	st.ProtectedHighWater = 0
	s.mu.RLock()
	if !s.closed {
		_ = s.db.View(func(tx *bolt.Tx) error {
			st.Environment = s.lifecycle.Environment
			st.StoreID = string(tx.Bucket([]byte("meta")).Get([]byte("identity")))
			st.HighWater = tx.Bucket([]byte("events")).Sequence()
			st.RetiredThrough = readUint(tx.Bucket([]byte("meta")).Get([]byte("archive_floor")))
			b := tx.Bucket([]byte("catalog"))
			if b == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				var c CatalogEntry
				if json.Unmarshal(v, &c) != nil {
					return ErrBillingUnavailable
				}
				if p, err := billingarchive.VerifyCompletion(c.Completion, c.Manifest, c.ManifestBytes, s.lifecycle.keys); err == nil {
					if p.VerifiedAt.After(st.LastVerified) {
						st.LastVerified = p.VerifiedAt
					}
					if p.HighWater > st.ProtectedHighWater {
						st.ProtectedHighWater = p.HighWater
					}
					if c.Manifest.CreatedAt.After(st.LastProtectedAt) {
						st.LastProtectedAt = c.Manifest.CreatedAt
					}
				}
				return nil
			})
		})
		if st.ProtectedHighWater < st.HighWater {
			_ = s.db.View(func(tx *bolt.Tx) error {
				at, err := receiptTime(tx, st.ProtectedHighWater+1)
				if err == nil {
					st.EarliestUnprotectedReceivedAt = at
				}
				return err
			})
		}
	}
	s.mu.RUnlock()
	st.ProtectionOverdue = st.LastProtectedAt.IsZero() || time.Since(st.LastProtectedAt) > 24*time.Hour
	return st
}
func (s *BillingInbox) RunBackupWorker(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			_, err := s.Capture(ctx)
			if err != nil {
				s.workerMu.Lock()
				s.workerStatus.LastError = err.Error()
				s.workerMu.Unlock()
			}
			timer.Reset(s.lifecycle.CaptureInterval)
		}
	}
}
