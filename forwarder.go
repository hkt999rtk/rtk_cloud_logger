package cloudlogger

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type JournalRecord struct {
	Cursor string
	BootID string
	HostID string
	Host   string
	Unit   string
	Source string
	Fields map[string]any
}

type RecordSource interface {
	Next(ctx context.Context, afterCursor string) (JournalRecord, bool, error)
}

type Delivery interface {
	Deliver(ctx context.Context, events []LogEvent) ([]IngestResult, error)
}

type CursorStore interface {
	Load() (string, error)
	Save(cursor string) error
}

type ForwarderSpool interface {
	Append(event LogEvent) error
	Load() ([]LogEvent, error)
	Replace(events []LogEvent) error
	Count() int
	DroppedRecords() int
}

type ForwarderConfig struct {
	Source      RecordSource
	CursorStore CursorStore
	Spool       ForwarderSpool
	Delivery    Delivery
	Now         func() time.Time
	MaxAttempts int
	Backoff     func(attempt int) time.Duration
	Sleep       func(context.Context, time.Duration) error
}

type ForwarderStatus struct {
	Cursor         string    `json:"cursor"`
	LastUploadTime time.Time `json:"last_upload_time,omitempty"`
	Degraded       bool      `json:"degraded"`
	SpoolRecords   int       `json:"spool_records"`
	DroppedRecords int       `json:"dropped_records"`
	LastError      string    `json:"last_error,omitempty"`
}

type Forwarder struct {
	source      RecordSource
	cursorStore CursorStore
	spool       ForwarderSpool
	delivery    Delivery
	now         func() time.Time
	maxAttempts int
	backoff     func(attempt int) time.Duration
	sleep       func(context.Context, time.Duration) error

	mu     sync.RWMutex
	status ForwarderStatus
}

func NewForwarder(cfg ForwarderConfig) *Forwarder {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.Backoff == nil {
		cfg.Backoff = func(attempt int) time.Duration {
			return time.Duration(attempt) * 100 * time.Millisecond
		}
	}
	if cfg.Sleep == nil {
		cfg.Sleep = func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	return &Forwarder{
		source:      cfg.Source,
		cursorStore: cfg.CursorStore,
		spool:       cfg.Spool,
		delivery:    cfg.Delivery,
		now:         cfg.Now,
		maxAttempts: cfg.MaxAttempts,
		backoff:     cfg.Backoff,
		sleep:       cfg.Sleep,
	}
}

func (f *Forwarder) ProcessOnce(ctx context.Context) error {
	if f.source == nil || f.cursorStore == nil || f.spool == nil || f.delivery == nil {
		return errors.New("forwarder is not fully configured")
	}
	cursor, err := f.cursorStore.Load()
	if err != nil {
		f.setDegraded(cursor, err)
		return err
	}

	record, ok, err := f.source.Next(ctx, cursor)
	if err != nil {
		f.setDegraded(cursor, err)
		return err
	}
	if ok {
		event, err := eventFromJournalRecord(record)
		if err != nil {
			f.setDegraded(cursor, err)
			return err
		}
		if err := f.spool.Append(event); err != nil {
			f.setDegraded(cursor, err)
			return err
		}
	}

	pending, err := f.spool.Load()
	if err != nil {
		f.setDegraded(cursor, err)
		return err
	}
	if len(pending) == 0 {
		f.setStatus(ForwarderStatus{Cursor: cursor, SpoolRecords: f.spool.Count(), DroppedRecords: f.spool.DroppedRecords()})
		return nil
	}

	results, err := f.deliverWithRetry(ctx, pending)
	if err != nil {
		f.setDegraded(cursor, err)
		return err
	}
	if len(results) != len(pending) {
		err := errors.New("delivery returned mismatched result count")
		f.setDegraded(cursor, err)
		return err
	}

	remaining := make([]LogEvent, 0)
	lastAckCursor := cursor
	for i, result := range results {
		if result.Accepted {
			if ok && pending[i].EventID == StableEventID(record.HostID, record.BootID, record.Unit, record.Cursor) {
				lastAckCursor = record.Cursor
			}
			continue
		}
		remaining = append(remaining, pending[i])
	}
	if err := f.spool.Replace(remaining); err != nil {
		f.setDegraded(cursor, err)
		return err
	}
	if lastAckCursor != cursor {
		if err := f.cursorStore.Save(lastAckCursor); err != nil {
			f.setDegraded(cursor, err)
			return err
		}
	}
	f.setStatus(ForwarderStatus{
		Cursor:         lastAckCursor,
		LastUploadTime: f.now(),
		SpoolRecords:   f.spool.Count(),
		DroppedRecords: f.spool.DroppedRecords(),
	})
	return nil
}

func (f *Forwarder) deliverWithRetry(ctx context.Context, events []LogEvent) ([]IngestResult, error) {
	var lastErr error
	for attempt := 1; attempt <= f.maxAttempts; attempt++ {
		results, err := f.delivery.Deliver(ctx, events)
		if err == nil {
			return results, nil
		}
		lastErr = err
		if attempt == f.maxAttempts {
			break
		}
		if sleepErr := f.sleep(ctx, f.backoff(attempt)); sleepErr != nil {
			return nil, sleepErr
		}
	}
	return nil, lastErr
}

func (f *Forwarder) Status() ForwarderStatus {
	f.mu.RLock()
	defer f.mu.RUnlock()
	status := f.status
	if f.spool != nil {
		status.SpoolRecords = f.spool.Count()
		status.DroppedRecords = f.spool.DroppedRecords()
	}
	return status
}

func (f *Forwarder) StatusJSON() ([]byte, error) {
	return json.Marshal(f.Status())
}

func (f *Forwarder) setStatus(status ForwarderStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
}

func (f *Forwarder) setDegraded(cursor string, err error) {
	f.setStatus(ForwarderStatus{
		Cursor:         cursor,
		Degraded:       true,
		SpoolRecords:   f.spool.Count(),
		DroppedRecords: f.spool.DroppedRecords(),
		LastError:      err.Error(),
	})
}

type SliceRecordSource struct {
	records []JournalRecord
}

func NewSliceRecordSource(records []JournalRecord) *SliceRecordSource {
	return &SliceRecordSource{records: records}
}

func (s *SliceRecordSource) Next(ctx context.Context, afterCursor string) (JournalRecord, bool, error) {
	for _, record := range s.records {
		if afterCursor == "" || record.Cursor > afterCursor {
			return record, true, nil
		}
	}
	return JournalRecord{}, false, nil
}

type FileRecordSource struct {
	path string
}

func NewFileRecordSource(path string) *FileRecordSource {
	return &FileRecordSource{path: path}
}

func (s *FileRecordSource) Next(ctx context.Context, afterCursor string) (JournalRecord, bool, error) {
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return JournalRecord{}, false, nil
	}
	if err != nil {
		return JournalRecord{}, false, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return JournalRecord{}, false, ctx.Err()
		default:
		}
		var record JournalRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return JournalRecord{}, false, err
		}
		if afterCursor == "" || record.Cursor > afterCursor {
			return record, true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return JournalRecord{}, false, err
	}
	return JournalRecord{}, false, nil
}

type FileCursorStore struct {
	path string
}

func NewFileCursorStore(path string) *FileCursorStore {
	return &FileCursorStore{path: path}
}

func (s *FileCursorStore) Load() (string, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var state struct {
		Cursor string `json:"cursor"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return "", err
	}
	return state.Cursor, nil
}

func (s *FileCursorStore) Save(cursor string) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(struct {
		Cursor string `json:"cursor"`
	}{Cursor: cursor})
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}

type DiskSpool struct {
	path       string
	maxRecords int
	dropped    int
}

func NewDiskSpool(path string, maxRecords int) *DiskSpool {
	if maxRecords < 1 {
		maxRecords = 1
	}
	return &DiskSpool{path: path, maxRecords: maxRecords}
}

func (s *DiskSpool) Append(event LogEvent) error {
	events, err := s.Load()
	if err != nil {
		return err
	}
	events = append(events, event)
	for len(events) > s.maxRecords {
		events = events[1:]
		s.dropped++
	}
	return s.Replace(events)
}

func (s *DiskSpool) Load() ([]LogEvent, error) {
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	events := make([]LogEvent, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event LogEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *DiskSpool) Replace(events []LogEvent) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *DiskSpool) Count() int {
	events, err := s.Load()
	if err != nil {
		return 0
	}
	return len(events)
}

func (s *DiskSpool) DroppedRecords() int {
	return s.dropped
}
