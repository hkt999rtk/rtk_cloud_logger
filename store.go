package cloudlogger

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var ErrDuplicateEvent = errors.New("duplicate event")

const (
	QueryOrderAsc  = "asc"
	QueryOrderDesc = "desc"
)

type EventStore interface {
	InsertEvent(context.Context, LogEvent) error
	QueryEvents(context.Context, EventQuery) ([]LogEvent, error)
}

type HealthChecker interface {
	Health(context.Context) error
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
	return applyQueryOrderAndLimit(out, query), nil
}

func (s *MemoryEventStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

type EventQuery struct {
	Since       time.Time
	Until       time.Time
	Limit       int
	Order       string
	EventID     string
	Env         string
	Service     string
	Host        string
	Unit        string
	Level       string
	Source      string
	TraceID     string
	RequestID   string
	OperationID string
	DeviceID    string
	OrgID       string
	UserID      string
	Component   string
	ActorID     string
	ActorType   string
	Outcome     string
	StatusCode  string
	StatusClass string
}

func (q EventQuery) matches(event LogEvent) bool {
	if !q.Since.IsZero() && event.Time.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && event.Time.After(q.Until) {
		return false
	}
	return match(q.Env, event.Env) &&
		match(q.EventID, event.EventID) &&
		match(q.Service, event.Service) &&
		match(q.Host, event.Host) &&
		match(q.Unit, event.Unit) &&
		match(q.Level, event.Level) &&
		match(q.Source, event.Source) &&
		match(q.TraceID, event.TraceID) &&
		match(q.RequestID, event.RequestID) &&
		match(q.OperationID, event.OperationID) &&
		match(q.DeviceID, event.DeviceID) &&
		match(q.OrgID, event.OrgID) &&
		match(q.UserID, event.UserID) &&
		match(q.Component, event.Component) &&
		match(q.ActorID, event.ActorID) &&
		match(q.ActorType, event.ActorType) &&
		match(q.Outcome, event.Outcome) &&
		match(q.StatusCode, event.StatusCode) &&
		match(q.StatusClass, event.StatusClass)
}

func match(want string, got string) bool {
	return want == "" || want == got
}

func applyQueryOrderAndLimit(events []LogEvent, query EventQuery) []LogEvent {
	order := query.Order
	if order == "" {
		order = QueryOrderDesc
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Time.Equal(events[j].Time) {
			if order == QueryOrderAsc {
				return events[i].EventID < events[j].EventID
			}
			return events[i].EventID > events[j].EventID
		}
		if order == QueryOrderAsc {
			return events[i].Time.Before(events[j].Time)
		}
		return events[i].Time.After(events[j].Time)
	})
	if query.Limit > 0 && len(events) > query.Limit {
		return events[:query.Limit]
	}
	return events
}
