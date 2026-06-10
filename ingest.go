package cloudlogger

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	IngestStatusAccepted  = "accepted"
	IngestStatusDuplicate = "duplicate"
	IngestStatusRejected  = "rejected"
)

type IngestConfig struct {
	Token string
}

type IngestRequest struct {
	Events []LogEvent `json:"events"`
}

type IngestResult struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
}

type IngestResponse struct {
	Results []IngestResult `json:"results"`
}

func IngestHandler(store EventStore, cfg IngestConfig) http.Handler {
	if store == nil {
		store = NewMemoryEventStore()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if checker, ok := store.(HealthChecker); ok {
			if err := checker.Health(r.Context()); err != nil {
				http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/metrics/prometheus", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(`# HELP rtk_cloud_logger_up Whether the Cloud Logger backend is serving metrics.
# TYPE rtk_cloud_logger_up gauge
rtk_cloud_logger_up 1
`))
	})
	mux.HandleFunc("/v1/logs/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if cfg.Token != "" && bearerToken(r.Header.Get("Authorization")) != cfg.Token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var request IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		response := IngestResponse{Results: make([]IngestResult, 0, len(request.Events))}
		for _, event := range request.Events {
			result := IngestResult{EventID: event.EventID}
			err := store.InsertEvent(r.Context(), event)
			switch {
			case err == nil:
				result.Status = IngestStatusAccepted
			case errors.Is(err, ErrDuplicateEvent):
				result.Status = IngestStatusDuplicate
			default:
				result.Status = IngestStatusRejected
				result.Error = err.Error()
			}
			response.Results = append(response.Results, result)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(response)
	})
	mux.HandleFunc("/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if cfg.Token != "" && bearerToken(r.Header.Get("Authorization")) != cfg.Token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		events, err := store.QueryEvents(r.Context(), queryFromRequest(r))
		if err != nil {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Events []LogEvent `json:"events"`
		}{Events: events})
	})
	return mux
}

func queryFromRequest(r *http.Request) EventQuery {
	values := r.URL.Query()
	return EventQuery{
		Since:       parseQueryTime(values.Get("since")),
		Until:       parseQueryTime(values.Get("until")),
		Env:         values.Get("env"),
		Service:     values.Get("service"),
		Host:        values.Get("host"),
		Unit:        values.Get("unit"),
		Level:       values.Get("level"),
		TraceID:     values.Get("trace_id"),
		RequestID:   values.Get("request_id"),
		OperationID: values.Get("operation_id"),
		DeviceID:    values.Get("device_id"),
		OrgID:       values.Get("org_id"),
		UserID:      values.Get("user_id"),
	}
}

func parseQueryTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func bearerToken(header string) string {
	prefix := "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, prefix))
}
