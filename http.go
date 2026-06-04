package cloudlogger

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
)

const redactedMarker = "[REDACTED]"

var sensitiveQueryParams = map[string]struct{}{
	"token":         {},
	"access_token":  {},
	"refresh_token": {},
	"api_key":       {},
	"apikey":        {},
	"password":      {},
	"client_secret": {},
}

// HTTPMiddleware logs one structured event after each request completes.
func HTTPMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = Nop()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(recorder, r)

			fields := []zap.Field{
				zap.String("method", r.Method),
				zap.String("path", SanitizePath(r.URL.RequestURI())),
				zap.Int("status", recorder.status),
				zap.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000.0),
				zap.String("remote_addr", remoteAddr(r.RemoteAddr)),
			}
			if requestID := strings.TrimSpace(r.Header.Get("X-Request-Id")); requestID != "" {
				fields = append(fields, zap.String("request_id", requestID))
			}

			logger.Info("http request", fields...)
		})
	}
}

// SanitizePath returns a path/query string with sensitive query values redacted.
func SanitizePath(path string) string {
	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return path
	}

	query := parsed.Query()
	if len(query) == 0 {
		return parsed.RequestURI()
	}

	for key, values := range query {
		if _, ok := sensitiveQueryParams[strings.ToLower(key)]; !ok {
			continue
		}
		for i := range values {
			values[i] = redactedMarker
		}
		query[key] = values
	}
	parsed.RawQuery = query.Encode()

	return strings.ReplaceAll(parsed.RequestURI(), url.QueryEscape(redactedMarker), redactedMarker)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not implement http.Hijacker")
	}
	return hijacker.Hijack()
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *statusRecorder) Push(target string, opts *http.PushOptions) error {
	pusher, ok := r.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func remoteAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}
