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

func TestIngestHandlerRejectsOversizedBody(t *testing.T) {
	handler := IngestHandler(NewMemoryEventStore(), IngestConfig{Token: "secret", MaxBodyBytes: 8})
	req := httptest.NewRequest(http.MethodPost, "/v1/logs/ingest", strings.NewReader(`{"events":[]}`))
	req.Header.Set("Authorization", "Bearer secret")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%q", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "request body too large") {
		t.Fatalf("body = %q, want request body too large", recorder.Body.String())
	}
}

func TestIngestHandlerRejectsTooManyEvents(t *testing.T) {
	handler := IngestHandler(NewMemoryEventStore(), IngestConfig{Token: "secret", MaxEventsPerBatch: 1})
	response := postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{
		queryTestEvent("evt-1", time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC)),
		queryTestEvent("evt-2", time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC)),
	}})
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
	body := new(bytes.Buffer)
	_, _ = body.ReadFrom(response.Body)
	if !strings.Contains(body.String(), "too many events") {
		t.Fatalf("body = %q, want too many events", body.String())
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

func TestIngestHandlerQueriesByAdminAuditFields(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	event := LogEvent{
		EventID:     "evt-admin",
		Time:        time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:       "info",
		Message:     "device command completed",
		Service:     "video-cloud-api",
		Env:         "staging",
		Version:     "test",
		Host:        "host-a",
		Unit:        "video_cloud-api.service",
		Source:      "journald",
		ActorID:     "admin-1",
		ActorType:   "cloud_admin",
		Outcome:     "success",
		StatusCode:  "200",
		StatusClass: "2xx",
	}
	_ = postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{event}})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?actor_id=admin-1&actor_type=cloud_admin&outcome=success&status_code=200&status_class=2xx", nil)
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

func TestIngestHandlerQueriesByEventIDComponentAndSource(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	event := LogEvent{
		EventID:   "evt-device-runtime-1",
		Time:      time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:     "info",
		Message:   "ready",
		Service:   "video-cloud",
		Env:       "staging",
		Version:   "test",
		Host:      "host-a",
		Unit:      "video_cloud-logingester.service",
		Source:    "device-runtime",
		DeviceID:  "device-1",
		Component: "device_runtime_log",
	}
	other := event
	other.EventID = "evt-device-runtime-2"
	other.Source = "journald"
	other.Component = "service_log"
	_ = postIngest(t, handler, "secret", IngestRequest{Events: []LogEvent{event, other}})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?event_id=evt-device-runtime-1&component=device_runtime_log&source=device-runtime", nil)
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

func TestIngestHandlerQueriesWithDefaultDescendingOrderAndLimit(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	events := []LogEvent{
		queryTestEvent("evt-old", time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC)),
		queryTestEvent("evt-mid", time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC)),
		queryTestEvent("evt-new", time.Date(2026, 6, 1, 3, 0, 0, 0, time.UTC)),
	}
	_ = postIngest(t, handler, "secret", IngestRequest{Events: events})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?service=query-api&limit=2", nil)
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
	got := eventIDs(response.Events)
	want := []string{"evt-new", "evt-mid"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("event ids = %v, want %v", got, want)
	}
}

func TestIngestHandlerQueriesWithAscendingOrder(t *testing.T) {
	store := NewMemoryEventStore()
	handler := IngestHandler(store, IngestConfig{Token: "secret"})
	events := []LogEvent{
		queryTestEvent("evt-old", time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC)),
		queryTestEvent("evt-new", time.Date(2026, 6, 1, 3, 0, 0, 0, time.UTC)),
		queryTestEvent("evt-mid", time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC)),
	}
	_ = postIngest(t, handler, "secret", IngestRequest{Events: events})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?service=query-api&order=asc", nil)
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
	got := eventIDs(response.Events)
	want := []string{"evt-old", "evt-mid", "evt-new"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("event ids = %v, want %v", got, want)
	}
}

func TestIngestHandlerRejectsInvalidQueryParameters(t *testing.T) {
	handler := IngestHandler(NewMemoryEventStore(), IngestConfig{Token: "secret"})
	tests := []string{
		"/v1/logs?since=not-a-time",
		"/v1/logs?until=not-a-time",
		"/v1/logs?limit=0",
		"/v1/logs?limit=1001",
		"/v1/logs?limit=abc",
		"/v1/logs?order=sideways",
	}
	for _, target := range tests {
		t.Run(target, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set("Authorization", "Bearer secret")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%q", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if !strings.Contains(recorder.Body.String(), "invalid query parameter") {
				t.Fatalf("body = %q, want invalid query parameter", recorder.Body.String())
			}
		})
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

func queryTestEvent(eventID string, ts time.Time) LogEvent {
	return LogEvent{
		EventID: eventID,
		Time:    ts,
		Level:   "info",
		Message: "query event",
		Service: "query-api",
		Env:     "staging",
		Version: "test",
		Host:    "host-a",
		Unit:    "query-api.service",
		Source:  "journald",
	}
}

func eventIDs(events []LogEvent) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.EventID)
	}
	return out
}

func TestRedactEventRedactsNestedArraysAndSensitiveValues(t *testing.T) {
	event := LogEvent{
		EventID: "evt-nested-redact",
		Time:    time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
		Level:   "info",
		Message: "safe message",
		Service: "api",
		Env:     "staging",
		Version: "test",
		Host:    "host-a",
		Unit:    "api.service",
		Source:  "journald",
		Fields: map[string]any{
			"items": []any{
				map[string]any{"client_secret": "raw-secret", "safe": "ok"},
				"Bearer raw-token",
			},
			"nested": map[string]any{
				"values": []any{
					map[string]any{"password": "raw-password"},
				},
			},
		},
	}

	redacted := RedactEvent(event)

	items := redacted.Fields["items"].([]any)
	first := items[0].(map[string]any)
	if first["client_secret"] != RedactedValue || first["safe"] != "ok" {
		t.Fatalf("first item = %+v, want secret redacted and safe preserved", first)
	}
	if items[1] != RedactedValue {
		t.Fatalf("second item = %v, want redacted", items[1])
	}
	nestedValues := redacted.Fields["nested"].(map[string]any)["values"].([]any)
	nestedSecret := nestedValues[0].(map[string]any)
	if nestedSecret["password"] != RedactedValue {
		t.Fatalf("nested password = %v, want redacted", nestedSecret["password"])
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
