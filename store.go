package cloudlogger

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrDuplicateEvent = errors.New("duplicate event")

type EventStore interface {
	InsertEvent(context.Context, LogEvent) error
	QueryEvents(context.Context, EventQuery) ([]LogEvent, error)
}

type MemoryEventStore struct {
	mu     sync.Mutex
	events map[string]LogEvent
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{events: make(map[string]LogEvent)}
}

func (s *MemoryEventStore) InsertEvent(_ context.Context, event LogEvent) error {
	event = RedactEvent(event)
	if err := event.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.events[event.EventID]; ok {
		return ErrDuplicateEvent
	}
	s.events[event.EventID] = event
	return nil
}

func (s *MemoryEventStore) QueryEvents(_ context.Context, query EventQuery) ([]LogEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEvent, 0, len(s.events))
	for _, event := range s.events {
		if query.matches(event) {
			out = append(out, event)
		}
	}
	return out, nil
}

func (s *MemoryEventStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

type EventQuery struct {
	Since       time.Time
	Until       time.Time
	Env         string
	Service     string
	Host        string
	Unit        string
	Level       string
	TraceID     string
	RequestID   string
	OperationID string
	DeviceID    string
	OrgID       string
	UserID      string
}

func (q EventQuery) matches(event LogEvent) bool {
	if !q.Since.IsZero() && event.Time.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && event.Time.After(q.Until) {
		return false
	}
	return match(q.Env, event.Env) &&
		match(q.Service, event.Service) &&
		match(q.Host, event.Host) &&
		match(q.Unit, event.Unit) &&
		match(q.Level, event.Level) &&
		match(q.TraceID, event.TraceID) &&
		match(q.RequestID, event.RequestID) &&
		match(q.OperationID, event.OperationID) &&
		match(q.DeviceID, event.DeviceID) &&
		match(q.OrgID, event.OrgID) &&
		match(q.UserID, event.UserID)
}

func match(want string, got string) bool {
	return want == "" || want == got
}
