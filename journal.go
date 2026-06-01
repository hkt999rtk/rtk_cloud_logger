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
	Units  []string
	Config JournalParseConfig
}

func (s JournalctlSource) Read(ctx context.Context, after string, limit int) ([]JournalRecord, error) {
	args := []string{"-o", "json", "--no-pager"}
	if after != "" {
		args = append(args, "--after-cursor", after)
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
	service := firstNonEmpty(stringField(raw, "SERVICE"), cfg.DefaultService, unit)
	env := firstNonEmpty(stringField(raw, "ENV"), cfg.DefaultEnv, "unknown")
	version := firstNonEmpty(stringField(raw, "VERSION"), cfg.DefaultVersion, "unknown")
	event := LogEvent{
		EventID:     EventIDFromJournalMetadata(host, bootID, unit, cursor),
		Time:        journalTime(raw),
		Level:       journalLevel(stringField(raw, "PRIORITY")),
		Message:     firstNonEmpty(stringField(raw, "MESSAGE"), "journal event"),
		Service:     service,
		Env:         env,
		Version:     version,
		Host:        host,
		Unit:        unit,
		Source:      "journald",
		TraceID:     stringField(raw, "TRACE_ID"),
		RequestID:   stringField(raw, "REQUEST_ID"),
		OperationID: stringField(raw, "OPERATION_ID"),
		DeviceID:    stringField(raw, "DEVICE_ID"),
		OrgID:       stringField(raw, "ORG_ID"),
		UserID:      stringField(raw, "USER_ID"),
		Component:   stringField(raw, "COMPONENT"),
	}
	if err := event.Validate(); err != nil {
		return JournalRecord{}, fmt.Errorf("invalid journal event: %w", err)
	}
	return JournalRecord{Cursor: cursor, Event: event}, nil
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
