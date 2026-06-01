package cloudlogger

import "sync"

type IngestResult struct {
	EventID   string `json:"event_id"`
	Accepted  bool   `json:"accepted"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Error     string `json:"error,omitempty"`
}

type MemoryStore struct {
	mu     sync.RWMutex
	events map[string]LogEvent
	order  []string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{events: make(map[string]LogEvent)}
}

func (s *MemoryStore) InsertBatch(events []LogEvent) []IngestResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	results := make([]IngestResult, len(events))
	for i, event := range events {
		results[i].EventID = event.EventID
		if err := event.Validate(); err != nil {
			results[i].Error = err.Error()
			continue
		}
		event = RedactEvent(event)
		if _, exists := s.events[event.EventID]; exists {
			results[i].Accepted = true
			results[i].Duplicate = true
			continue
		}
		s.events[event.EventID] = event
		s.order = append(s.order, event.EventID)
		results[i].Accepted = true
	}
	return results
}

func (s *MemoryStore) Query(query LogQuery) []LogEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]LogEvent, 0)
	for _, id := range s.order {
		event := s.events[id]
		if !matchesQuery(event, query) {
			continue
		}
		out = append(out, event)
	}
	return out
}

func matchesQuery(event LogEvent, query LogQuery) bool {
	if !query.From.IsZero() && event.TS.Before(query.From) {
		return false
	}
	if !query.To.IsZero() && event.TS.After(query.To) {
		return false
	}
	for _, pair := range []struct {
		want string
		got  string
	}{
		{query.Env, event.Env},
		{query.Service, event.Service},
		{query.Host, event.Host},
		{query.Unit, event.Unit},
		{query.Level, event.Level},
		{query.TraceID, event.TraceID},
		{query.RequestID, event.RequestID},
		{query.OperationID, event.OperationID},
		{query.DeviceID, event.DeviceID},
		{query.OrgID, event.OrgID},
		{query.UserID, event.UserID},
	} {
		if pair.want != "" && pair.want != pair.got {
			return false
		}
	}
	return true
}
