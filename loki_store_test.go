package cloudlogger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLokiEventStoreIngestsQueriesAndDeduplicates(t *testing.T) {
	var stored []LogEvent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			w.WriteHeader(http.StatusNoContent)
		case "/loki/api/v1/push":
			var payload struct {
				Streams []struct {
					Values [][]string `json:"values"`
				} `json:"streams"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			for _, stream := range payload.Streams {
				for _, value := range stream.Values {
					var event LogEvent
					if err := json.Unmarshal([]byte(value[1]), &event); err != nil {
						t.Fatal(err)
					}
					stored = append(stored, event)
				}
			}
			w.WriteHeader(http.StatusNoContent)
		case "/loki/api/v1/query_range":
			if got := r.URL.Query().Get("query"); !strings.Contains(got, `service="video-cloud-api"`) {
				t.Fatalf("query selector = %q, want service label", got)
			}
			values := make([][]string, 0, len(stored)+1)
			for _, event := range stored {
				body, _ := json.Marshal(event)
				values = append(values, []string{strconvTime(event.Time), string(body)})
				values = append(values, []string{strconvTime(event.Time), string(body)})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data": map[string]any{
					"resultType": "streams",
					"result": []map[string]any{{
						"stream": map[string]string{"service": "video-cloud-api"},
						"values": values,
					}},
				},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
		}
	}))
	defer server.Close()

	store, err := NewLokiEventStore(LokiStoreConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewLokiEventStore: %v", err)
	}
	if err := store.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
	event := lokiTestEvent()
	event.Fields = map[string]any{"access_token": "raw-token", "safe": "ok"}
	if err := store.InsertEvent(context.Background(), event); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.InsertEvent(context.Background(), event); err != ErrDuplicateEvent {
		t.Fatalf("duplicate InsertEvent error = %v, want ErrDuplicateEvent", err)
	}
	if len(stored) != 1 {
		t.Fatalf("stored events = %d, want 1", len(stored))
	}
	events, err := store.QueryEvents(context.Background(), EventQuery{
		Service:   "video-cloud-api",
		TraceID:   "trace-1",
		RequestID: "request-1",
	})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("events = %+v, want one %s event", events, event.EventID)
	}
	if events[0].Fields["access_token"] != RedactedValue {
		t.Fatalf("access_token = %v, want redacted", events[0].Fields["access_token"])
	}
	if events[0].Fields["safe"] != "ok" {
		t.Fatalf("safe = %v, want ok", events[0].Fields["safe"])
	}
}

func TestIngestHandlerHealthReportsLokiUnavailable(t *testing.T) {
	store, err := NewLokiEventStore(LokiStoreConfig{BaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewLokiEventStore: %v", err)
	}
	handler := IngestHandler(store, IngestConfig{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func lokiTestEvent() LogEvent {
	return LogEvent{
		EventID:     "evt-loki-1",
		Time:        time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:       "warn",
		Message:     "command failed",
		Service:     "video-cloud-api",
		Env:         "staging",
		Version:     "test",
		Host:        "host-a",
		Unit:        "video-cloud-api.service",
		Source:      "journald",
		TraceID:     "trace-1",
		RequestID:   "request-1",
		OperationID: "operation-1",
		DeviceID:    "device-1",
		OrgID:       "org-1",
		UserID:      "user-1",
	}
}

func strconvTime(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}
