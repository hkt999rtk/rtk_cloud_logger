package cloudlogger

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type JournalParseConfig struct {
	DefaultService string
	DefaultEnv     string
	DefaultVersion string
}

type JournalctlSource struct {
	Units        []string
	InitialSince string
	Config       JournalParseConfig
}

func (s JournalctlSource) Read(ctx context.Context, after string, limit int) ([]JournalRecord, error) {
	args := []string{"-o", "json", "--no-pager"}
	if limit > 0 {
		args = append(args, "-n", strconv.Itoa(limit))
	}
	if after != "" {
		args = append(args, "--after-cursor", after)
	} else if since := journalSinceArg(s.InitialSince); since != "" {
		args = append(args, "--since", since)
	}
	for _, unit := range s.Units {
		args = append(args, "-u", unit)
	}
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	records, scanErr := ParseJournalRecords(out, limit, s.Config)
	waitErr := cmd.Wait()
	if scanErr != nil {
		return nil, scanErr
	}
	if waitErr != nil {
		return nil, waitErr
	}
	return records, nil
}

func journalSinceArg(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if duration, err := time.ParseDuration(value); err == nil {
		return time.Now().UTC().Add(-duration).Format(time.RFC3339)
	}
	return value
}

func ParseJournalRecords(reader io.Reader, limit int, cfg JournalParseConfig) ([]JournalRecord, error) {
	var records []JournalRecord
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		record, err := ParseJournalRecord(scanner.Bytes(), cfg)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
		if limit > 0 && len(records) >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func ParseJournalRecord(line []byte, cfg JournalParseConfig) (JournalRecord, error) {
	var raw map[string]any
	if err := json.Unmarshal(line, &raw); err != nil {
		return JournalRecord{}, err
	}
	cursor := stringField(raw, "__CURSOR")
	bootID := stringField(raw, "_BOOT_ID")
	host := firstNonEmpty(stringField(raw, "_HOSTNAME"), "unknown-host")
	unit := firstNonEmpty(stringField(raw, "_SYSTEMD_UNIT"), stringField(raw, "SYSLOG_IDENTIFIER"), "unknown-unit")
	message := firstNonEmpty(stringField(raw, "MESSAGE"), "journal event")
	messageFields := parseJournalMessageFields(message)
	service := firstNonEmpty(messageStringField(messageFields, "service"), stringField(raw, "SERVICE"), cfg.DefaultService, unit)
	env := firstNonEmpty(messageStringField(messageFields, "env"), stringField(raw, "ENV"), cfg.DefaultEnv, "unknown")
	version := firstNonEmpty(messageStringField(messageFields, "version"), stringField(raw, "VERSION"), cfg.DefaultVersion, "unknown")
	event := LogEvent{
		EventID:       EventIDFromJournalMetadata(host, bootID, unit, cursor),
		Time:          journalTime(raw),
		Level:         firstNonEmpty(messageStringField(messageFields, "level"), journalLevel(stringField(raw, "PRIORITY"))),
		Message:       firstNonEmpty(messageStringField(messageFields, "msg"), message),
		Service:       service,
		Env:           env,
		Version:       version,
		Host:          host,
		Unit:          unit,
		Source:        "journald",
		TraceID:       firstNonEmpty(messageStringField(messageFields, "trace_id"), stringField(raw, "TRACE_ID")),
		RequestID:     firstNonEmpty(messageStringField(messageFields, "request_id"), stringField(raw, "REQUEST_ID")),
		OperationID:   firstNonEmpty(messageStringField(messageFields, "operation_id"), stringField(raw, "OPERATION_ID")),
		DeviceID:      firstNonEmpty(messageStringField(messageFields, "device_id"), stringField(raw, "DEVICE_ID")),
		OrgID:         firstNonEmpty(messageStringField(messageFields, "org_id"), stringField(raw, "ORG_ID")),
		UserID:        firstNonEmpty(messageStringField(messageFields, "user_id"), stringField(raw, "USER_ID")),
		Component:     firstNonEmpty(messageStringField(messageFields, "component"), stringField(raw, "COMPONENT")),
		ErrorCategory: messageStringField(messageFields, "error_category"),
		Fields:        journalMessageExtraFields(messageFields),
	}
	event = RedactEvent(event)
	if err := event.Validate(); err != nil {
		return JournalRecord{}, fmt.Errorf("invalid journal event: %w", err)
	}
	return JournalRecord{Cursor: cursor, Event: event}, nil
}

func parseJournalMessageFields(message string) map[string]any {
	message = strings.TrimSpace(message)
	if message == "" || !strings.HasPrefix(message, "{") {
		return nil
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(message), &fields); err != nil {
		return nil
	}
	return fields
}

func messageStringField(fields map[string]any, key string) string {
	if len(fields) == 0 {
		return ""
	}
	switch value := fields[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case float64:
		return strconv.FormatInt(int64(value), 10)
	default:
		return ""
	}
}

func journalMessageExtraFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range fields {
		if journalMessagePromotedField(key) {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func journalMessagePromotedField(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "level",
		"msg",
		"service",
		"env",
		"version",
		"trace_id",
		"request_id",
		"operation_id",
		"device_id",
		"org_id",
		"user_id",
		"component",
		"error_category":
		return true
	default:
		return false
	}
}

func stringField(raw map[string]any, key string) string {
	switch value := raw[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case float64:
		return strconv.FormatInt(int64(value), 10)
	default:
		return ""
	}
}

func journalTime(raw map[string]any) time.Time {
	text := stringField(raw, "__REALTIME_TIMESTAMP")
	if text == "" {
		return time.Now().UTC()
	}
	micros, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return time.Now().UTC()
	}
	return time.Unix(0, micros*int64(time.Microsecond)).UTC()
}

func journalLevel(priority string) string {
	switch priority {
	case "0", "1", "2", "3":
		return "error"
	case "4":
		return "warn"
	case "7":
		return "debug"
	default:
		return "info"
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
