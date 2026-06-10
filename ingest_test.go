package cloudlogger

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIngestHandlerStoresEventsIdempotently(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	event := LogEvent{
		EventID: "evt-1",
		Time:    time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:   "info",
		Message: "started",
		Service: "account-manager",
		Env:     "staging",
		Version: "test",
		Host:    "host-a",
		Unit:    "rtk-account-manager.service",
		Source:  "journald",
	}

	first := postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{event}})
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first status = %d, want %d", first.StatusCode, http.StatusAccepted)
	}
	second := postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{event}})
	if second.StatusCode != http.StatusAccepted {
		t.Fatalf("second status = %d, want %d", second.StatusCode, http.StatusAccepted)
	}

	if got := store.Count(); got != 1 {
		t.Fatalf("stored events = %d, want 1", got)
	}
	var response IngestResponse
	if err := json.NewDecoder(second.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Status != IngestStatusDuplicate {
		t.Fatalf("second response = %+v, want duplicate", response.Results)
	}
}

func TestIngestHandlerRejectsUnauthenticatedRequests(t *testing.T) {
	handler := IngestHandler(NewMemoryEventStore(), IngestConfig{Token: "secret"})
	response := postIngest(t, handler, "wrong", IngestRequest{})
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}
}

func TestIngestHandlerExposesPrometheusMetrics(t *testing.T) {
	handler := IngestHandler(NewMemoryEventStore(), IngestConfig{Token: "secret"})
	req := httptest.NewRequest(http.MethodGet, "/metrics/prometheus", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"# HELP rtk_cloud_logger_up Whether the Cloud Logger backend is serving metrics.",
		"# TYPE rtk_cloud_logger_up gauge",
		"rtk_cloud_logger_up 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestIngestHandlerQueriesByCorrelationFields(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	event := LogEvent{
		EventID:     "evt-query",
		Time:        time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:       "warn",
		Message:     "command failed",
		Service:     "video-cloud-api",
		Env:         "staging",
		Version:     "test",
		Host:        "host-a",
		Unit:        "video_cloud-api.service",
		Source:      "journald",
		TraceID:     "trace-1",
		RequestID:   "request-1",
		OperationID: "operation-1",
		DeviceID:    "device-1",
		OrgID:       "org-1",
		UserID:      "user-1",
	}
	_ = postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{event}})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?service=video-cloud-api&trace_id=trace-1&operation_id=operation-1", nil)
	req.Header.Set("Authorization", "Bearer secret")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var response struct {
		Events []LogEvent `json:"events"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if len(response.Events) != 1 || response.Events[0].EventID != event.EventID {
		t.Fatalf("events = %+v, want %s", response.Events, event.EventID)
	}
}

func TestIngestHandlerQueriesPromotedJournalJSONCorrelationFields(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	record, err := ParseJournalRecord([]byte(`{"__CURSOR":"s=query-json","_BOOT_ID":"boot-a","_HOSTNAME":"host-a","_SYSTEMD_UNIT":"video_cloud-certissuer.service","MESSAGE":"{\"msg\":\"certificate issued\",\"service\":\"certissuer\",\"env\":\"staging\",\"version\":\"release-1\",\"request_id\":\"20260603T123846Z-rtk-0001\",\"device_id\":\"rtk-0001\",\"operation_id\":\"factory-enroll\"}","PRIORITY":"6","__REALTIME_TIMESTAMP":"1780297323000000"}`), JournalParseConfig{})
	if err != nil {
		t.Fatalf("ParseJournalRecord: %v", err)
	}
	_ = postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{record.Event}})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?request_id=20260603T123846Z-rtk-0001&device_id=rtk-0001", nil)
	req.Header.Set("Authorization", "Bearer secret")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var response struct {
		Events []LogEvent `json:"events"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if len(response.Events) != 1 || response.Events[0].RequestID != "20260603T123846Z-rtk-0001" || response.Events[0].DeviceID != "rtk-0001" {
		t.Fatalf("events = %+v, want promoted query match", response.Events)
	}
}

func TestIngestHandlerRedactsSensitiveUnknownFields(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	event := LogEvent{
		EventID: "evt-redact",
		Time:    time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:   "info",
		Message: "payload",
		Service: "api",
		Env:     "staging",
		Version: "test",
		Host:    "host-a",
		Unit:    "api.service",
		Source:  "journald",
		Fields: map[string]any{
			"access_token": "raw-token",
			"safe":         "ok",
			"nested": map[string]any{
				"client_secret": "raw-secret",
			},
		},
	}
	_ = postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{event}})

	events, err := store.QueryEvents(context.Background(), EventQuery{})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Fields["access_token"] != RedactedValue {
		t.Fatalf("access_token = %v, want redacted", events[0].Fields["access_token"])
	}
	if events[0].Fields["safe"] != "ok" {
		t.Fatalf("safe field = %v, want ok", events[0].Fields["safe"])
	}
	nested := events[0].Fields["nested"].(map[string]any)
	if nested["client_secret"] != RedactedValue {
		t.Fatalf("client_secret = %v, want redacted", nested["client_secret"])
	}
}

func postIngest(t *testing.T, handler http.Handler, token string, body IngestRequest) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/logs/ingest", &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder.Result()
}
