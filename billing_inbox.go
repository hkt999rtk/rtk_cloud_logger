package cloudlogger

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrBillingConflict    = errors.New("billing event identity conflicts with stored content")
	ErrBillingCursor      = errors.New("invalid billing inbox cursor or store identity")
	ErrBillingUnavailable = errors.New("billing inbox unavailable")
)

type BillingRecord struct {
	Sequence      uint64   `json:"sequence,string"`
	ContentSHA256 string   `json:"content_sha256"`
	Event         LogEvent `json:"event"`
}

type BillingPage struct {
	StoreID    string          `json:"store_id"`
	HighWater  uint64          `json:"high_water,string"`
	Records    []BillingRecord `json:"records"`
	NextCursor string          `json:"next_cursor"`
	HasMore    bool            `json:"has_more"`
}

type billingCursor struct {
	StoreID string `json:"store_id"`
	After   uint64 `json:"after,string"`
	Through uint64 `json:"through,string"`
}

// BillingInbox is a single-writer retained inbox for periodic snapshots, not a
// billing ledger. The database transaction commits the event and receipt/index
// together. No retention, deletion or automatic reinitialization is permitted.
type BillingInbox struct {
	mu     sync.RWMutex
	failed bool
	db     *bolt.DB
	path   string
	info   os.FileInfo
}

func OpenBillingInbox(path string, initialize bool) (*BillingInbox, error) {
	if !filepath.IsAbs(path) {
		return nil, ErrBillingUnavailable
	}
	dir := filepath.Dir(path)
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, ErrBillingUnavailable
	}
	// macOS /tmp is itself a system symlink; use the resolved parent thereafter.
	path = filepath.Join(resolved, filepath.Base(path))
	d, err := os.Stat(resolved)
	if err != nil || !d.IsDir() || d.Mode().Perm()&0077 != 0 {
		return nil, ErrBillingUnavailable
	}
	prior, err := os.Lstat(path)
	creating := os.IsNotExist(err)
	if creating && !initialize {
		return nil, ErrBillingUnavailable
	}
	if !creating && (err != nil || !prior.Mode().IsRegular() || prior.Size() == 0 || prior.Mode().Perm()&0077 != 0) {
		return nil, ErrBillingUnavailable
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		return nil, ErrBillingUnavailable
	}
	fail := func() (*BillingInbox, error) { _ = db.Close(); return nil, ErrBillingUnavailable }
	err = db.Update(func(tx *bolt.Tx) error {
		if !creating {
			if tx.Bucket([]byte("meta")) == nil || tx.Bucket([]byte("events")) == nil || tx.Bucket([]byte("ids")) == nil {
				return ErrBillingUnavailable
			}
			if len(tx.Bucket([]byte("meta")).Get([]byte("identity"))) != 32 || string(tx.Bucket([]byte("meta")).Get([]byte("version"))) != "1" {
				return ErrBillingUnavailable
			}
			return nil
		}
		for _, name := range []string{"meta", "events", "ids"} {
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		identity := make([]byte, 16)
		if _, err := rand.Read(identity); err != nil {
			return err
		}
		meta := tx.Bucket([]byte("meta"))
		if err := meta.Put([]byte("identity"), []byte(hex.EncodeToString(identity))); err != nil {
			return err
		}
		return meta.Put([]byte("version"), []byte("1"))
	})
	if err != nil {
		return fail()
	}
	info, err := os.Lstat(path)
	if err != nil || (!creating && !os.SameFile(prior, info)) {
		return fail()
	}
	parent, err := os.Open(resolved)
	if err != nil {
		return fail()
	}
	err = parent.Sync()
	_ = parent.Close()
	if err != nil {
		return fail()
	}
	return &BillingInbox{db: db, path: path, info: info}, nil
}

func (s *BillingInbox) Close() error {
	if s == nil {
		return nil
	}
	return s.db.Close()
}

func (s *BillingInbox) Health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrBillingUnavailable
	}
	s.mu.RLock()
	failed := s.failed
	s.mu.RUnlock()
	if failed {
		return ErrBillingUnavailable
	}
	info, err := os.Lstat(s.path)
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(s.info, info) || info.Mode().Perm()&0077 != 0 {
		return ErrBillingUnavailable
	}
	if err := s.db.View(func(tx *bolt.Tx) error { return nil }); err != nil {
		return ErrBillingUnavailable
	}
	return nil
}

func billingBinding(event LogEvent) (LogEvent, string, error) {
	if err := event.Validate(); err != nil {
		return event, "", err
	}
	if event.Stream != "billing_usage" || event.Source != "billing_usage" || event.Fields["usage_event"] == nil {
		return event, "", errors.New("billing usage envelope required")
	}
	encoded, err := json.Marshal(event)
	if err != nil || len(encoded) > 1<<20 || len(event.EventID) > 4096 {
		return event, "", errors.New("invalid or oversized billing event")
	}
	before, err := json.Marshal(event.Fields)
	if err != nil {
		return event, "", err
	}
	event = RedactEvent(event)
	after, err := json.Marshal(event.Fields)
	if err != nil || !bytes.Equal(before, after) {
		return event, "", errors.New("billing usage fields must not require redaction")
	}
	event.Time = event.Time.UTC()
	// Deployment version/host metadata can change when retrying the same frozen
	// snapshot after an upgrade. Bind the immutable usage and isolation fields.
	body, err := json.Marshal(struct {
		ID, Env, Stream, Source string
		Time                    time.Time
		Fields                  map[string]any
	}{event.EventID, event.Env, event.Stream, event.Source, event.Time, event.Fields})
	if err != nil {
		return event, "", err
	}
	sum := sha256.Sum256(body)
	return event, hex.EncodeToString(sum[:]), nil
}

func sequenceKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}

func (s *BillingInbox) InsertEvent(ctx context.Context, event LogEvent) error {
	if err := s.Health(ctx); err != nil {
		return err
	}
	event, digest, err := billingBinding(event)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return ErrBillingUnavailable
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids, events := tx.Bucket([]byte("ids")), tx.Bucket([]byte("events"))
		if key := ids.Get([]byte(event.EventID)); key != nil {
			var previous BillingRecord
			decoder := json.NewDecoder(bytes.NewReader(events.Get(key)))
			decoder.UseNumber()
			if err := decoder.Decode(&previous); err != nil {
				return ErrBillingUnavailable
			}
			_, storedDigest, err := billingBinding(previous.Event)
			if err != nil || storedDigest != previous.ContentSHA256 || previous.Event.EventID != event.EventID || len(key) != 8 || previous.Sequence != binary.BigEndian.Uint64(key) {
				return ErrBillingUnavailable
			}
			if previous.ContentSHA256 != digest {
				return ErrBillingConflict
			}
			return ErrDuplicateEvent
		}
		seq, err := events.NextSequence()
		if err != nil || seq == 0 {
			return ErrBillingUnavailable
		}
		body, err := json.Marshal(BillingRecord{Sequence: seq, ContentSHA256: digest, Event: event})
		if err != nil {
			return err
		}
		key := sequenceKey(seq)
		if err := events.Put(key, body); err != nil {
			return err
		}
		return ids.Put([]byte(event.EventID), key)
	})
	if err != nil && !errors.Is(err, ErrDuplicateEvent) && !errors.Is(err, ErrBillingConflict) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.failed = true // ambiguous storage commits must be inspected/reopened
		return ErrBillingUnavailable
	}
	return err
}

// Page walks commit sequence, never producer timestamps or newly-added counts.
// A non-final cursor freezes the horizon; a final cursor opens a new snapshot on
// the next poll so late producer events are collected without rewinding time.
func (s *BillingInbox) Page(ctx context.Context, encoded string, limit int) (BillingPage, error) {
	page := BillingPage{Records: []BillingRecord{}}
	if err := s.Health(ctx); err != nil {
		return page, err
	}
	if limit < 1 || limit > MaxQueryLimit {
		return page, ErrBillingCursor
	}
	var cursor billingCursor
	if encoded != "" {
		if len(encoded) > 512 {
			return page, ErrBillingCursor
		}
		body, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || json.Unmarshal(body, &cursor) != nil || len(cursor.StoreID) != 32 {
			return page, ErrBillingCursor
		}
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		page.StoreID = string(tx.Bucket([]byte("meta")).Get([]byte("identity")))
		events := tx.Bucket([]byte("events"))
		head := events.Sequence()
		if encoded != "" && (cursor.StoreID != page.StoreID || cursor.After > head || cursor.Through > head || (cursor.Through != 0 && cursor.After > cursor.Through)) {
			return ErrBillingCursor
		}
		cursor.StoreID = page.StoreID
		if cursor.Through == 0 {
			cursor.Through = head
		}
		page.HighWater = cursor.Through
		c := events.Cursor()
		pageBytes := 0
		for cursor.After < cursor.Through && len(page.Records) < limit {
			if err := ctx.Err(); err != nil {
				return err
			}
			key, body := c.Seek(sequenceKey(cursor.After + 1))
			if len(key) != 8 || binary.BigEndian.Uint64(key) != cursor.After+1 {
				return ErrBillingUnavailable
			}
			if pageBytes+len(body) > 8<<20 && len(page.Records) > 0 {
				break
			}
			var record BillingRecord
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.UseNumber()
			if err := decoder.Decode(&record); err != nil || record.Sequence != cursor.After+1 {
				return ErrBillingUnavailable
			}
			_, digest, err := billingBinding(record.Event)
			if err != nil || digest != record.ContentSHA256 {
				return ErrBillingUnavailable
			}
			page.Records = append(page.Records, record)
			pageBytes += len(body)
			cursor.After++
		}
		page.HasMore = cursor.After < cursor.Through
		if !page.HasMore {
			cursor.Through = 0
		}
		body, err := json.Marshal(cursor)
		if err != nil {
			return fmt.Errorf("cursor: %w", err)
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(body)
		return nil
	})
	if errors.Is(err, ErrBillingUnavailable) {
		s.mu.Lock()
		s.failed = true
		s.mu.Unlock()
	}
	return page, err
}
