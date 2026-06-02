package cloudlogger

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

type DockerLogParseConfig struct {
	Container   string
	Service     string
	Env         string
	Version     string
	Host        string
	Unit        string
	Source      string
	Component   string
	OperationID string
}

type DockerLogsSource struct {
	Container    string
	InitialSince string
	Config       DockerLogParseConfig
}

func (s DockerLogsSource) Read(ctx context.Context, after string, limit int) ([]JournalRecord, error) {
	if strings.TrimSpace(s.Container) == "" {
		return nil, fmt.Errorf("docker container is required")
	}
	args := []string{"logs", "--timestamps"}
	if after != "" {
		args = append(args, "--since", after)
	} else if s.InitialSince != "" {
		args = append(args, "--since", s.InitialSince)
	}
	if limit > 0 {
		args = append(args, "--tail", fmt.Sprint(limit))
	}
	args = append(args, s.Container)
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	records, scanErr := ParseDockerLogRecords(out, limit, s.Config)
	waitErr := cmd.Wait()
	if scanErr != nil {
		return nil, scanErr
	}
	if waitErr != nil {
		return nil, waitErr
	}
	return records, nil
}

func ParseDockerLogRecords(reader io.Reader, limit int, cfg DockerLogParseConfig) ([]JournalRecord, error) {
	var records []JournalRecord
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		record, err := ParseDockerLogRecord(scanner.Text(), cfg)
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

func ParseDockerLogRecord(line string, cfg DockerLogParseConfig) (JournalRecord, error) {
	timestamp, message := splitDockerTimestamp(line)
	eventTime, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		eventTime = time.Now().UTC()
		timestamp = eventTime.Format(time.RFC3339Nano)
	}
	message = RedactText(strings.TrimSpace(message))
	event := LogEvent{
		EventID:     DockerLogEventID(cfg.Container, timestamp, message),
		Time:        eventTime.UTC(),
		Level:       dockerLogLevel(message),
		Message:     firstNonEmpty(dockerLogMessage(message), "docker log event"),
		Service:     firstNonEmpty(cfg.Service, cfg.Container, "docker"),
		Env:         firstNonEmpty(cfg.Env, "unknown"),
		Version:     firstNonEmpty(cfg.Version, "unknown"),
		Host:        firstNonEmpty(cfg.Host, "unknown-host"),
		Unit:        firstNonEmpty(cfg.Unit, cfg.Container),
		Source:      firstNonEmpty(cfg.Source, "docker"),
		OperationID: cfg.OperationID,
		Component:   cfg.Component,
		Fields: map[string]any{
			"container": cfg.Container,
			"log_line":  message,
		},
	}
	if err := event.Validate(); err != nil {
		return JournalRecord{}, fmt.Errorf("invalid docker log event: %w", err)
	}
	return JournalRecord{Cursor: timestamp, Event: RedactEvent(event)}, nil
}

func splitDockerTimestamp(line string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(line), " ", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", line
}

func DockerLogEventID(container, timestamp, message string) string {
	sum := sha256.Sum256([]byte(container + "\x00" + timestamp + "\x00" + message))
	return "docker-log-" + hex.EncodeToString(sum[:12])
}

func dockerLogLevel(message string) string {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "[error]") || strings.Contains(lower, " error"):
		return "error"
	case strings.Contains(lower, "[warning]") || strings.Contains(lower, " warning"):
		return "warn"
	case strings.Contains(lower, "[debug]") || strings.Contains(lower, " debug"):
		return "debug"
	default:
		return "info"
	}
}

func dockerLogMessage(message string) string {
	if strings.Contains(strings.ToLower(message), "publish") || strings.Contains(strings.ToLower(message), "subscribe") {
		return "emqx broker mqtt trace"
	}
	return "emqx broker log"
}
