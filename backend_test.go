package cloudlogger

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMemoryStoreInsertsEventsIdempotentlyAndQueries(t *testing.T) {
	store := NewMemoryStore()
	event := LogEvent{
		EventID: "host-boot-unit-cursor",
		TS:      time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:   "info",
		Msg:     "http request",
		Service: "video-cloud-api",
		Env:     "staging",
		Version: "v1",
		Host:    "api-1",
		Unit:    "video-cloud-api.service",
		Source:  "journald",
		TraceID: "trace-1",
		OrgID:   "org-1",
		Body: map[string]any{
			"request_id": "req-1",
		},
	}

	first := store.InsertBatch([]LogEvent{event})
	second := store.InsertBatch([]LogEvent{event})

	if !first[0].Accepted || first[0].Duplicate {
		t.Fatalf("first insert result = %#v, want accepted non-duplicate", first[0])
	}
	if !second[0].Accepted || !second[0].Duplicate {
		t.Fatalf("second insert result = %#v, want accepted duplicate", second[0])
	}

	events := store.Query(LogQuery{
		Env:     "staging",
		Service: "video-cloud-api",
		Host:    "api-1",
		Unit:    "video-cloud-api.service",
		Level:   "info",
		TraceID: "trace-1",
		OrgID:   "org-1",
		From:    event.TS.Add(-time.Second),
		To:      event.TS.Add(time.Second),
	})
	if len(events) != 1 {
		t.Fatalf("query returned %d events, want 1: %#v", len(events), events)
	}
}

func TestIngestAPIAuthenticatesRedactsAndDeduplicates(t *testing.T) {
	store := NewMemoryStore()
	server := NewIngestServer(store, IngestServerConfig{Token: "secret-token"})

	event := validLogEvent("event-1")
	event.Body = map[string]any{
		"authorization": "Bearer raw-token",
		"cookie":        "session=raw-cookie",
		"password":      "raw-password",
		"database_dsn":  "postgres://user:pass@db.internal/app",
		"message":       "kept",
	}
	body, err := json.Marshal(IngestRequest{Events: []LogEvent{event, event}})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	unauth := httptest.NewRecorder()
	server.ServeHTTP(unauth, httptest.NewRequest(http.MethodPost, "/v1/logs/ingest", bytes.NewReader(body)))
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("unauth status = %d, want %d", unauth.Code, http.StatusUnauthorized)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/logs/ingest", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("ingest status = %d, want 200: %s", rr.Code, rr.Body.String())
	}

	var response IngestResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Results) != 2 || !response.Results[0].Accepted || !response.Results[1].Duplicate {
		t.Fatalf("unexpected ingest response: %#v", response)
	}

	stored := store.Query(LogQuery{Service: event.Service})
	if len(stored) != 1 {
		t.Fatalf("stored events = %d, want 1", len(stored))
	}
	raw, err := json.Marshal(stored[0])
	if err != nil {
		t.Fatalf("marshal stored event: %v", err)
	}
	for _, forbidden := range []string{"raw-token", "raw-cookie", "raw-password", "user:pass"} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatalf("stored event leaked %q: %s", forbidden, string(raw))
		}
	}
	if stored[0].Body["message"] != "kept" {
		t.Fatalf("non-sensitive body field was not preserved: %#v", stored[0].Body)
	}
}

func TestIngestAPIQueryEndpointFiltersEvents(t *testing.T) {
	store := NewMemoryStore()
	store.InsertBatch([]LogEvent{
		validLogEvent("event-1"),
		func() LogEvent {
			event := validLogEvent("event-2")
			event.Service = "account-manager"
			event.UserID = "user-2"
			return event
		}(),
	})
	server := NewIngestServer(store, IngestServerConfig{Token: "secret-token"})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs/query?service=video-cloud-api&user_id=user-1", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query status = %d, want 200: %s", rr.Code, rr.Body.String())
	}

	var response QueryResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if len(response.Events) != 1 || response.Events[0].EventID != "event-1" {
		t.Fatalf("query response = %#v, want event-1 only", response.Events)
	}
}

func TestHTTPDeliverySendsBatchToIngestAPI(t *testing.T) {
	store := NewMemoryStore()
	api := httptest.NewServer(NewIngestServer(store, IngestServerConfig{Token: "secret-token"}))
	defer api.Close()

	delivery := NewHTTPDelivery(api.URL+"/v1/logs/ingest", "secret-token", api.Client())
	results, err := delivery.Deliver(context.Background(), []LogEvent{validLogEvent("event-1")})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(results) != 1 || !results[0].Accepted {
		t.Fatalf("results = %#v, want accepted event", results)
	}
	if events := store.Query(LogQuery{Service: "video-cloud-api"}); len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
}

func validLogEvent(eventID string) LogEvent {
	return LogEvent{
		EventID:     eventID,
		TS:          time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:       "info",
		Msg:         "event",
		Service:     "video-cloud-api",
		Env:         "staging",
		Version:     "v1",
		Host:        "api-1",
		Unit:        "video-cloud-api.service",
		Source:      "journald",
		TraceID:     "trace-1",
		RequestID:   "req-1",
		OperationID: "op-1",
		DeviceID:    "device-1",
		OrgID:       "org-1",
		UserID:      "user-1",
		Component:   "http",
	}
}
