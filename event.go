package cloudlogger

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

type LogEvent struct {
	EventID       string         `json:"event_id"`
	Time          time.Time      `json:"ts"`
	Level         string         `json:"level"`
	Message       string         `json:"msg"`
	Service       string         `json:"service"`
	Env           string         `json:"env"`
	Version       string         `json:"version"`
	Host          string         `json:"host"`
	Unit          string         `json:"unit"`
	Source        string         `json:"source"`
	Stream        string         `json:"stream,omitempty"`
	TraceID       string         `json:"trace_id,omitempty"`
	RequestID     string         `json:"request_id,omitempty"`
	OperationID   string         `json:"operation_id,omitempty"`
	DeviceID      string         `json:"device_id,omitempty"`
	OrgID         string         `json:"org_id,omitempty"`
	UserID        string         `json:"user_id,omitempty"`
	Component     string         `json:"component,omitempty"`
	ErrorCategory string         `json:"error_category,omitempty"`
	ActorID       string         `json:"actor_id,omitempty"`
	ActorType     string         `json:"actor_type,omitempty"`
	Outcome       string         `json:"outcome,omitempty"`
	StatusCode    string         `json:"status_code,omitempty"`
	StatusClass   string         `json:"status_class,omitempty"`
	Fields        map[string]any `json:"fields,omitempty"`
}

func (e LogEvent) Validate() error {
	switch {
	case strings.TrimSpace(e.EventID) == "":
		return errors.New("event_id is required")
	case e.Time.IsZero():
		return errors.New("ts is required")
	case strings.TrimSpace(e.Level) == "":
		return errors.New("level is required")
	case strings.TrimSpace(e.Message) == "":
		return errors.New("msg is required")
	case strings.TrimSpace(e.Service) == "":
		return errors.New("service is required")
	case strings.TrimSpace(e.Env) == "":
		return errors.New("env is required")
	case strings.TrimSpace(e.Version) == "":
		return errors.New("version is required")
	case strings.TrimSpace(e.Host) == "":
		return errors.New("host is required")
	case strings.TrimSpace(e.Unit) == "":
		return errors.New("unit is required")
	case strings.TrimSpace(e.Source) == "":
		return errors.New("source is required")
	default:
		return nil
	}
}

func EventIDFromJournalMetadata(hostID string, bootID string, unit string, cursor string) string {
	sum := sha256.Sum256([]byte(hostID + "\x00" + bootID + "\x00" + unit + "\x00" + cursor))
	return hex.EncodeToString(sum[:])
}
