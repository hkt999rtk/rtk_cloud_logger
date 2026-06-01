package cloudlogger

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// LogEvent is the central backend representation for one service log record.
type LogEvent struct {
	EventID       string         `json:"event_id"`
	TS            time.Time      `json:"ts"`
	Level         string         `json:"level"`
	Msg           string         `json:"msg"`
	Service       string         `json:"service"`
	Env           string         `json:"env"`
	Version       string         `json:"version"`
	Host          string         `json:"host"`
	Unit          string         `json:"unit"`
	Source        string         `json:"source"`
	TraceID       string         `json:"trace_id,omitempty"`
	RequestID     string         `json:"request_id,omitempty"`
	OperationID   string         `json:"operation_id,omitempty"`
	DeviceID      string         `json:"device_id,omitempty"`
	OrgID         string         `json:"org_id,omitempty"`
	UserID        string         `json:"user_id,omitempty"`
	Component     string         `json:"component,omitempty"`
	ErrorCategory string         `json:"error_category,omitempty"`
	Body          map[string]any `json:"body,omitempty"`
}

// LogQuery contains supported backend query filters.
type LogQuery struct {
	From        time.Time
	To          time.Time
	Env         string
	Service     string
	Host        string
	Unit        string
	Level       string
	TraceID     string
	RequestID   string
	OperationID string
	DeviceID    string
	OrgID       string
	UserID      string
}

func (e LogEvent) Validate() error {
	missing := make([]string, 0)
	required := map[string]string{
		"event_id": e.EventID,
		"level":    e.Level,
		"msg":      e.Msg,
		"service":  e.Service,
		"env":      e.Env,
		"version":  e.Version,
		"host":     e.Host,
		"unit":     e.Unit,
		"source":   e.Source,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, field)
		}
	}
	if e.TS.IsZero() {
		missing = append(missing, "ts")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func StableEventID(hostID, bootID, unit, cursor string) string {
	sum := sha256.Sum256([]byte(hostID + "\x00" + bootID + "\x00" + unit + "\x00" + cursor))
	return hex.EncodeToString(sum[:])
}

func eventFromJournalRecord(record JournalRecord) (LogEvent, error) {
	ts, err := parseEventTime(record.Fields["ts"])
	if err != nil {
		return LogEvent{}, err
	}
	event := LogEvent{
		EventID: StableEventID(record.HostID, record.BootID, record.Unit, record.Cursor),
		TS:      ts,
		Level:   stringField(record.Fields, "level"),
		Msg:     stringField(record.Fields, "msg"),
		Service: stringField(record.Fields, "service"),
		Env:     stringField(record.Fields, "env"),
		Version: stringField(record.Fields, "version"),
		Host:    record.Host,
		Unit:    record.Unit,
		Source:  record.Source,
		Body:    make(map[string]any),
	}

	for _, field := range []struct {
		key string
		set func(string)
	}{
		{"trace_id", func(v string) { event.TraceID = v }},
		{"request_id", func(v string) { event.RequestID = v }},
		{"operation_id", func(v string) { event.OperationID = v }},
		{"device_id", func(v string) { event.DeviceID = v }},
		{"org_id", func(v string) { event.OrgID = v }},
		{"user_id", func(v string) { event.UserID = v }},
		{"component", func(v string) { event.Component = v }},
		{"error_category", func(v string) { event.ErrorCategory = v }},
	} {
		if value := stringField(record.Fields, field.key); value != "" {
			field.set(value)
		}
	}

	for key, value := range record.Fields {
		if isKnownEventField(key) {
			continue
		}
		event.Body[key] = value
	}
	if len(event.Body) == 0 {
		event.Body = nil
	}
	return event, event.Validate()
}

func parseEventTime(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed, nil
	case string:
		if typed == "" {
			return time.Time{}, errors.New("missing ts")
		}
		return time.Parse(time.RFC3339Nano, typed)
	default:
		return time.Time{}, fmt.Errorf("unsupported ts value %T", value)
	}
}

func stringField(fields map[string]any, key string) string {
	value, ok := fields[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func isKnownEventField(key string) bool {
	switch key {
	case "event_id", "ts", "level", "msg", "service", "env", "version", "host", "unit", "source",
		"trace_id", "request_id", "operation_id", "device_id", "org_id", "user_id", "component", "error_category":
		return true
	default:
		return false
	}
}
