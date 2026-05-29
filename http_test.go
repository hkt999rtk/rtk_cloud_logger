package cloudlogger

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestHTTPMiddlewareLogsCompletedRequest(t *testing.T) {
	var out bytes.Buffer
	logger, err := newForTest(Config{Service: "account-manager"}, zapcore.AddSync(&out))
	if err != nil {
		t.Fatalf("New test logger: %v", err)
	}

	handler := HTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/devices?token=secret&filter=online", nil)
	req.RemoteAddr = "203.0.113.10:12345"
	req.Header.Set("X-Request-Id", "req-123")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	event := decodeHTTPLogEvent(t, out.Bytes())
	if event["msg"] != "http request" {
		t.Fatalf("msg = %v, want http request", event["msg"])
	}
	if event["method"] != http.MethodPost {
		t.Fatalf("method = %v, want %s", event["method"], http.MethodPost)
	}
	if event["path"] != "/v1/devices?filter=online&token=[REDACTED]" {
		t.Fatalf("path was not sanitized: %#v", event["path"])
	}
	if event["status"] != float64(http.StatusCreated) {
		t.Fatalf("status = %v, want %d", event["status"], http.StatusCreated)
	}
	if event["remote_addr"] != "203.0.113.10" {
		t.Fatalf("remote_addr = %v, want peer host", event["remote_addr"])
	}
	if event["request_id"] != "req-123" {
		t.Fatalf("request_id = %v, want req-123", event["request_id"])
	}
	if duration, ok := event["duration_ms"].(float64); !ok || duration < 0 {
		t.Fatalf("duration_ms = %#v, want non-negative number", event["duration_ms"])
	}
}

func TestHTTPMiddlewareDefaultsStatusToOK(t *testing.T) {
	var out bytes.Buffer
	logger, err := newForTest(Config{}, zapcore.AddSync(&out))
	if err != nil {
		t.Fatalf("New test logger: %v", err)
	}

	handler := HTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/health", nil))

	event := decodeHTTPLogEvent(t, out.Bytes())
	if event["status"] != float64(http.StatusOK) {
		t.Fatalf("status = %v, want %d", event["status"], http.StatusOK)
	}
}

func TestHTTPMiddlewareLogsErrorStatus(t *testing.T) {
	var out bytes.Buffer
	logger, err := newForTest(Config{}, zapcore.AddSync(&out))
	if err != nil {
		t.Fatalf("New test logger: %v", err)
	}

	handler := HTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/missing", nil))

	event := decodeHTTPLogEvent(t, out.Bytes())
	if event["status"] != float64(http.StatusNotFound) {
		t.Fatalf("status = %v, want %d", event["status"], http.StatusNotFound)
	}
}

func TestHTTPMiddlewareDoesNotLogRequestBodyOrSensitiveHeaders(t *testing.T) {
	var out bytes.Buffer
	logger, err := newForTest(Config{}, zapcore.AddSync(&out))
	if err != nil {
		t.Fatalf("New test logger: %v", err)
	}

	handler := HTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	req := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader("raw-password-body"))
	req.Header.Set("Authorization", "Bearer raw-token")
	req.Header.Set("Cookie", "session=raw-cookie")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	logLine := out.String()
	for _, sensitive := range []string{"raw-password-body", "raw-token", "raw-cookie", "Authorization", "Cookie"} {
		if strings.Contains(logLine, sensitive) {
			t.Fatalf("sensitive request data %q leaked into log line %q", sensitive, logLine)
		}
	}
}

func TestSanitizePathRedactsSensitiveQueryValues(t *testing.T) {
	tests := map[string]string{
		"/v1/devices":                     "/v1/devices",
		"/v1/callback?token=abc&state=ok": "/v1/callback?state=ok&token=[REDACTED]",
		"/v1/callback?Access_Token=abc&refresh_token=def&state=ok":                 "/v1/callback?Access_Token=[REDACTED]&refresh_token=[REDACTED]&state=ok",
		"/v1/callback?api_key=a&apikey=b&password=c&client_secret=d&visible=value": "/v1/callback?api_key=[REDACTED]&apikey=[REDACTED]&client_secret=[REDACTED]&password=[REDACTED]&visible=value",
		"/v1/callback?token=a&token=b":                                             "/v1/callback?token=[REDACTED]&token=[REDACTED]",
	}

	for input, want := range tests {
		if got := SanitizePath(input); got != want {
			t.Fatalf("SanitizePath(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestSanitizedLogsDoNotContainSensitiveQueryValues(t *testing.T) {
	var out bytes.Buffer
	logger, err := newForTest(Config{}, zapcore.AddSync(&out))
	if err != nil {
		t.Fatalf("New test logger: %v", err)
	}

	handler := HTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/login?password=hunter2&client_secret=raw-secret&state=ok", nil))

	logLine := out.String()
	for _, sensitive := range []string{"hunter2", "raw-secret"} {
		if strings.Contains(logLine, sensitive) {
			t.Fatalf("sensitive value %q leaked into log line %q", sensitive, logLine)
		}
	}
}

func decodeHTTPLogEvent(t *testing.T, line []byte) map[string]any {
	t.Helper()

	var event map[string]any
	if err := json.Unmarshal(line, &event); err != nil {
		t.Fatalf("log output is not JSON: %v\n%s", err, string(line))
	}
	return event
}
