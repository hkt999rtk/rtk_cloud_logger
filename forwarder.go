package cloudlogger

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type JournalRecord struct {
	Cursor string
	Event  LogEvent
}

type EventSource interface {
	Read(context.Context, string, int) ([]JournalRecord, error)
}

type EventSink interface {
	Send(context.Context, []LogEvent) error
}

type ForwarderConfig struct {
	BatchSize int
}

type ForwarderStatus struct {
	Cursor                string    `json:"cursor"`
	LastUploadAt          time.Time `json:"last_upload_at,omitempty"`
	LastError             string    `json:"last_error,omitempty"`
	Degraded              bool      `json:"degraded"`
	LastUploadedCount     int       `json:"last_uploaded_count"`
	SpoolFileCount        int       `json:"spool_file_count"`
	SpoolBytes            int64     `json:"spool_bytes"`
	SpoolOldestAgeSeconds int64     `json:"spool_oldest_age_seconds,omitempty"`
	SpoolError            string    `json:"spool_error,omitempty"`
}

type Forwarder struct {
	source EventSource
	sink   EventSink
	cursor CursorStore
	spool  Spool
	cfg    ForwarderConfig

	mu     sync.Mutex
	status ForwarderStatus
}

type spoolStatsProvider interface {
	Stats() (SpoolStats, error)
}

func NewForwarder(source EventSource, sink EventSink, cursor CursorStore, cfg ForwarderConfig) *Forwarder {
	if cursor == nil {
		cursor = NewMemoryCursorStore()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &Forwarder{source: source, sink: sink, cursor: cursor, cfg: cfg}
}

func (f *Forwarder) WithSpool(spool Spool) *Forwarder {
	f.spool = spool
	return f
}

func (f *Forwarder) RunOnce(ctx context.Context) error {
	if f.spool != nil {
		if err := f.spool.Flush(ctx, f.sink); err != nil {
			f.setError("", err)
			return err
		}
	}
	after, err := f.cursor.Load(ctx)
	if err != nil {
		f.setError(after, err)
		return err
	}
	records, err := f.source.Read(ctx, after, f.cfg.BatchSize)
	if err != nil {
		f.setError(after, err)
		return err
	}
	if len(records) == 0 {
		f.setOK(after, 0)
		return nil
	}
	events := make([]LogEvent, 0, len(records))
	lastCursor := records[len(records)-1].Cursor
	for _, record := range records {
		events = append(events, record.Event)
	}
	if err := f.sink.Send(ctx, events); err != nil {
		if f.spool != nil {
			_ = f.spool.Enqueue(ctx, events)
		}
		f.setError(after, err)
		return err
	}
	if err := f.cursor.Save(ctx, lastCursor); err != nil {
		f.setError(after, err)
		return err
	}
	f.setOK(lastCursor, len(events))
	return nil
}

func (f *Forwarder) Status() ForwarderStatus {
	f.mu.Lock()
	status := f.status
	f.mu.Unlock()
	if provider, ok := f.spool.(spoolStatsProvider); ok {
		stats, err := provider.Stats()
		if err != nil {
			status.SpoolError = err.Error()
			return status
		}
		status.SpoolFileCount = stats.FileCount
		status.SpoolBytes = stats.Bytes
		status.SpoolOldestAgeSeconds = stats.OldestAgeSeconds
	}
	return status
}

func (f *Forwarder) setError(cursor string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.Cursor = cursor
	f.status.LastError = err.Error()
	f.status.Degraded = true
}

func (f *Forwarder) setOK(cursor string, count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.Cursor = cursor
	f.status.LastError = ""
	f.status.Degraded = false
	if count > 0 {
		f.status.LastUploadAt = time.Now().UTC()
	}
	f.status.LastUploadedCount = count
}

func ForwarderStatusHandler(forwarder *Forwarder) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if forwarder.Status().Degraded {
			http.Error(w, "forwarder degraded", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(forwarder.Status())
	})
	return mux
}
