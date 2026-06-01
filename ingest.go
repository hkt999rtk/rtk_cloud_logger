package cloudlogger

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type IngestServerConfig struct {
	Token string
}

type IngestRequest struct {
	Events []LogEvent `json:"events"`
}

type IngestResponse struct {
	Results []IngestResult `json:"results"`
}

type QueryResponse struct {
	Events []LogEvent `json:"events"`
}

type IngestServer struct {
	store *MemoryStore
	token string
	mux   *http.ServeMux
}

func NewIngestServer(store *MemoryStore, cfg IngestServerConfig) *IngestServer {
	if store == nil {
		store = NewMemoryStore()
	}
	server := &IngestServer{store: store, token: cfg.Token, mux: http.NewServeMux()}
	server.mux.HandleFunc("/healthz", server.handleHealth)
	server.mux.HandleFunc("/readyz", server.handleHealth)
	server.mux.HandleFunc("/v1/logs/ingest", server.handleIngest)
	server.mux.HandleFunc("/v1/logs/query", server.handleQuery)
	return server
}

func (s *IngestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *IngestServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (s *IngestServer) handleIngest(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var request IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	writeJSON(w, http.StatusOK, IngestResponse{Results: s.store.InsertBatch(request.Events)})
}

func (s *IngestServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	query, err := parseLogQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, QueryResponse{Events: s.store.Query(query)})
}

func (s *IngestServer) authorized(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "Bearer "+s.token {
		return true
	}
	return r.Header.Get("X-Logger-Token") == s.token
}

func parseLogQuery(r *http.Request) (LogQuery, error) {
	values := r.URL.Query()
	query := LogQuery{
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
	var err error
	if value := values.Get("from"); value != "" {
		query.From, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return LogQuery{}, err
		}
	}
	if value := values.Get("to"); value != "" {
		query.To, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return LogQuery{}, err
		}
	}
	return query, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
