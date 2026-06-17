package cloudlogger

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	IngestStatusAccepted  = "accepted"
	IngestStatusDuplicate = "duplicate"
	IngestStatusRejected  = "rejected"
	DefaultQueryLimit     = 100
	MaxQueryLimit         = 1000
	DefaultMaxBodyBytes   = 10 << 20
	DefaultMaxEventsBatch = 1000
)

type IngestConfig struct {
	Token             string
	MaxBodyBytes      int64
	MaxEventsPerBatch int
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
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes(cfg))).Decode(&request); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if len(request.Events) > maxEventsPerBatch(cfg) {
			http.Error(w, "too many events", http.StatusBadRequest)
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
		query, err := queryFromRequest(r)
		if err != nil {
			http.Error(w, "invalid query parameter: "+err.Error(), http.StatusBadRequest)
			return
		}
		events, err := store.QueryEvents(r.Context(), query)
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

func maxBodyBytes(cfg IngestConfig) int64 {
	if cfg.MaxBodyBytes <= 0 {
		return DefaultMaxBodyBytes
	}
	return cfg.MaxBodyBytes
}

func maxEventsPerBatch(cfg IngestConfig) int {
	if cfg.MaxEventsPerBatch <= 0 {
		return DefaultMaxEventsBatch
	}
	return cfg.MaxEventsPerBatch
}

func queryFromRequest(r *http.Request) (EventQuery, error) {
	values := r.URL.Query()
	since, err := parseQueryTime("since", values.Get("since"))
	if err != nil {
		return EventQuery{}, err
	}
	until, err := parseQueryTime("until", values.Get("until"))
	if err != nil {
		return EventQuery{}, err
	}
	limit, err := parseQueryLimit(values.Get("limit"))
	if err != nil {
		return EventQuery{}, err
	}
	order, err := parseQueryOrder(values.Get("order"))
	if err != nil {
		return EventQuery{}, err
	}
	return EventQuery{
		Since:       since,
		Until:       until,
		Limit:       limit,
		Order:       order,
		EventID:     values.Get("event_id"),
		Env:         values.Get("env"),
		Service:     values.Get("service"),
		Host:        values.Get("host"),
		Unit:        values.Get("unit"),
		Level:       values.Get("level"),
		Source:      values.Get("source"),
		TraceID:     values.Get("trace_id"),
		RequestID:   values.Get("request_id"),
		OperationID: values.Get("operation_id"),
		DeviceID:    values.Get("device_id"),
		OrgID:       values.Get("org_id"),
		UserID:      values.Get("user_id"),
		Component:   values.Get("component"),
		ActorID:     values.Get("actor_id"),
		ActorType:   values.Get("actor_type"),
		Outcome:     values.Get("outcome"),
		StatusCode:  values.Get("status_code"),
		StatusClass: values.Get("status_class"),
	}, nil
}

func parseQueryTime(name string, value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339", name)
	}
	return parsed, nil
}

func parseQueryLimit(value string) (int, error) {
	if value == "" {
		return DefaultQueryLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("limit must be an integer")
	}
	if limit < 1 || limit > MaxQueryLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", MaxQueryLimit)
	}
	return limit, nil
}

func parseQueryOrder(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return QueryOrderDesc, nil
	}
	switch value {
	case QueryOrderAsc, QueryOrderDesc:
		return value, nil
	default:
		return "", fmt.Errorf("order must be asc or desc")
	}
}

func bearerToken(header string) string {
	prefix := "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, prefix))
}
