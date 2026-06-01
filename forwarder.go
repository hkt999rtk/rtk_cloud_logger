package cloudlogger

import (
	"context"
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
	Cursor            string    `json:"cursor"`
	LastUploadAt      time.Time `json:"last_upload_at,omitempty"`
	LastError         string    `json:"last_error,omitempty"`
	Degraded          bool      `json:"degraded"`
	LastUploadedCount int       `json:"last_uploaded_count"`
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
	defer f.mu.Unlock()
	return f.status
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
