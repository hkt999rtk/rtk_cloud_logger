package cloudlogger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type LokiStoreConfig struct {
	BaseURL string
	Client  *http.Client
}

type LokiEventStore struct {
	baseURL string
	client  *http.Client
	mu      sync.Mutex
	seen    map[string]struct{}
	lastTS  map[string]time.Time
}

func NewLokiEventStore(cfg LokiStoreConfig) (*LokiEventStore, error) {
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		return nil, errors.New("loki base url is required")
	}
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	return &LokiEventStore{baseURL: base, client: client, seen: map[string]struct{}{}, lastTS: map[string]time.Time{}}, nil
}

func (s *LokiEventStore) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/ready", nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("loki health status %d", resp.StatusCode)
	}
	return nil
}

func (s *LokiEventStore) InsertEvent(ctx context.Context, event LogEvent) error {
	event = RedactEvent(event)
	if err := event.Validate(); err != nil {
		return err
	}
	labels := lokiLabels(event)
	labelKey := lokiLabelKey(labels)
	pushTime := event.Time.UTC()
	s.mu.Lock()
	if _, ok := s.seen[event.EventID]; ok {
		s.mu.Unlock()
		return ErrDuplicateEvent
	}
	if previous := s.lastTS[labelKey]; !previous.IsZero() && !pushTime.After(previous) {
		pushTime = previous.Add(time.Nanosecond)
	}
	s.seen[event.EventID] = struct{}{}
	s.lastTS[labelKey] = pushTime
	s.mu.Unlock()

	line, err := json.Marshal(event)
	if err != nil {
		return err
	}
	payload := map[string]any{
		"streams": []map[string]any{{
			"stream": labels,
			"values": [][]string{{
				strconv.FormatInt(pushTime.UnixNano(), 10),
				string(line),
			}},
		}},
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/loki/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		s.forget(event.EventID)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.forget(event.EventID)
		return fmt.Errorf("loki push status %d", resp.StatusCode)
	}
	return nil
}

func lokiLabelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		parts = append(parts, key, labels[key])
	}
	return strings.Join(parts, "\x00")
}

func (s *LokiEventStore) QueryEvents(ctx context.Context, query EventQuery) ([]LogEvent, error) {
	values := url.Values{}
	values.Set("query", lokiSelector(query))
	if !query.Since.IsZero() {
		values.Set("start", strconv.FormatInt(query.Since.UnixNano(), 10))
	}
	if !query.Until.IsZero() {
		values.Set("end", strconv.FormatInt(query.Until.UnixNano(), 10))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/loki/api/v1/query_range?"+values.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("loki query status %d", resp.StatusCode)
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	out := []LogEvent{}
	for _, stream := range parsed.Data.Result {
		for _, value := range stream.Values {
			if len(value) < 2 {
				continue
			}
			var event LogEvent
			if err := json.Unmarshal([]byte(value[1]), &event); err != nil {
				continue
			}
			event = RedactEvent(event)
			if !query.matches(event) {
				continue
			}
			if _, ok := seen[event.EventID]; ok {
				continue
			}
			seen[event.EventID] = struct{}{}
			out = append(out, event)
		}
	}
	return applyQueryOrderAndLimit(out, query), nil
}

func (s *LokiEventStore) forget(eventID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.seen, eventID)
}

func lokiLabels(event LogEvent) map[string]string {
	return map[string]string{
		"env":     event.Env,
		"service": event.Service,
		"host":    event.Host,
		"unit":    event.Unit,
		"level":   event.Level,
	}
}

func lokiSelector(query EventQuery) string {
	labels := map[string]string{}
	if query.Env != "" {
		labels["env"] = query.Env
	}
	if query.Service != "" {
		labels["service"] = query.Service
	}
	if query.Host != "" {
		labels["host"] = query.Host
	}
	if query.Unit != "" {
		labels["unit"] = query.Unit
	}
	if query.Level != "" {
		labels["level"] = query.Level
	}
	if len(labels) == 0 {
		return `{service=~".+"}`
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf(`%s=%q`, key, labels[key]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
