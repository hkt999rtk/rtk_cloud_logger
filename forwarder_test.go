package cloudlogger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestForwarderAdvancesCursorOnlyAfterSuccessfulSend(t *testing.T) {
	source := &fakeSource{records: []JournalRecord{
		{
			Cursor: "cursor-1",
			Event:  LogEvent{Time: time.Now().UTC(), Level: "info", Message: "one", Service: "svc", Env: "staging", Version: "v1", Host: "host", Unit: "svc.service", Source: "journald"},
		},
		{
			Cursor: "cursor-2",
			Event:  LogEvent{Time: time.Now().UTC(), Level: "info", Message: "two", Service: "svc", Env: "staging", Version: "v1", Host: "host", Unit: "svc.service", Source: "journald"},
		},
	}}
	cursor := NewMemoryCursorStore()
	sink := &fakeSink{err: errors.New("backend down")}
	forwarder := NewForwarder(source, sink, cursor, ForwarderConfig{BatchSize: 10})

	if err := forwarder.RunOnce(context.Background()); err == nil {
		t.Fatalf("RunOnce succeeded, want backend error")
	}
	if got := cursor.Cursor(); got != "" {
		t.Fatalf("cursor = %q, want empty after failed send", got)
	}
	if !forwarder.Status().Degraded {
		t.Fatalf("forwarder status degraded = false, want true")
	}

	sink.err = nil
	if err := forwarder.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce after recovery: %v", err)
	}
	if got := cursor.Cursor(); got != "cursor-2" {
		t.Fatalf("cursor = %q, want cursor-2 after ack", got)
	}
	if forwarder.Status().Degraded {
		t.Fatalf("forwarder status degraded = true, want false")
	}
	if len(sink.sent) != 2 {
		t.Fatalf("sent events = %d, want 2", len(sink.sent))
	}
}

func TestEventIDFromJournalMetadataIsStable(t *testing.T) {
	first := EventIDFromJournalMetadata("host-a", "boot-a", "svc.service", "cursor-a")
	second := EventIDFromJournalMetadata("host-a", "boot-a", "svc.service", "cursor-a")
	other := EventIDFromJournalMetadata("host-a", "boot-a", "svc.service", "cursor-b")
	if first == "" {
		t.Fatalf("event id is empty")
	}
	if first != second {
		t.Fatalf("event id is not stable: %q != %q", first, second)
	}
	if first == other {
		t.Fatalf("event id did not change when cursor changed")
	}
}

func TestParseJournalRecordBuildsEvent(t *testing.T) {
	line := []byte(`{"__CURSOR":"s=abc","_BOOT_ID":"boot-a","_HOSTNAME":"host-a","_SYSTEMD_UNIT":"svc.service","MESSAGE":"started","PRIORITY":"6","__REALTIME_TIMESTAMP":"1780297323000000","SERVICE":"svc","ENV":"staging","VERSION":"v1","TRACE_ID":"trace-1","REQUEST_ID":"req-1","OPERATION_ID":"op-1"}`)
	record, err := ParseJournalRecord(line, JournalParseConfig{DefaultService: "fallback", DefaultEnv: "dev", DefaultVersion: "dev"})
	if err != nil {
		t.Fatalf("ParseJournalRecord: %v", err)
	}
	if record.Cursor != "s=abc" {
		t.Fatalf("cursor = %q, want s=abc", record.Cursor)
	}
	if record.Event.EventID == "" {
		t.Fatalf("event id is empty")
	}
	if record.Event.Level != "info" || record.Event.Message != "started" || record.Event.Service != "svc" {
		t.Fatalf("event = %+v", record.Event)
	}
	if record.Event.TraceID != "trace-1" || record.Event.RequestID != "req-1" || record.Event.OperationID != "op-1" {
		t.Fatalf("correlation fields = %+v", record.Event)
	}
}

func TestJournalctlSourceLimitsBatchAndUsesInitialSince(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	journalctl := filepath.Join(dir, "journalctl")
	script := "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > " + shellPath(argsPath) + "\ncat <<'JSON'\n{\"__CURSOR\":\"s=abc\",\"_BOOT_ID\":\"boot-a\",\"_HOSTNAME\":\"host-a\",\"_SYSTEMD_UNIT\":\"svc.service\",\"MESSAGE\":\"started\",\"PRIORITY\":\"6\",\"__REALTIME_TIMESTAMP\":\"1780297323000000\"}\nJSON\n"
	if err := os.WriteFile(journalctl, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake journalctl: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	source := JournalctlSource{Units: []string{"svc.service"}, InitialSince: "10 minutes ago"}
	records, err := source.Read(context.Background(), "", 7)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	argsBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	args := string(argsBytes)
	for _, want := range []string{"-n 7", "--since 10 minutes ago", "-u svc.service"} {
		if !strings.Contains(args, want) {
			t.Fatalf("journalctl args %q missing %q", args, want)
		}
	}
}

func TestJournalctlSourceUsesAfterCursorInsteadOfInitialSince(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	journalctl := filepath.Join(dir, "journalctl")
	script := "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > " + shellPath(argsPath) + "\n"
	if err := os.WriteFile(journalctl, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake journalctl: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	source := JournalctlSource{Units: []string{"svc.service"}, InitialSince: "10m"}
	if _, err := source.Read(context.Background(), "cursor-1", 3); err != nil {
		t.Fatalf("Read: %v", err)
	}
	argsBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	args := string(argsBytes)
	if !strings.Contains(args, "--after-cursor cursor-1") {
		t.Fatalf("journalctl args %q missing after cursor", args)
	}
	if strings.Contains(args, "--since") {
		t.Fatalf("journalctl args %q unexpectedly used --since with cursor", args)
	}
}

func TestJournalctlSourceNormalizesDurationInitialSince(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	journalctl := filepath.Join(dir, "journalctl")
	script := "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > " + shellPath(argsPath) + "\n"
	if err := os.WriteFile(journalctl, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake journalctl: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	source := JournalctlSource{Units: []string{"svc.service"}, InitialSince: "10m"}
	if _, err := source.Read(context.Background(), "", 3); err != nil {
		t.Fatalf("Read: %v", err)
	}
	argsBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	args := string(argsBytes)
	if strings.Contains(args, "--since 10m") {
		t.Fatalf("journalctl args %q passed raw duration to --since", args)
	}
	if !strings.Contains(args, "--since ") {
		t.Fatalf("journalctl args %q missing --since", args)
	}
}

func shellPath(path string) string {
	return "'" + strings.ReplaceAll(path, "'", "'\\''") + "'"
}

func TestFileSpoolFlushesQueuedEvents(t *testing.T) {
	dir := t.TempDir()
	spool := FileSpool{Dir: dir, MaxBytes: 1 << 20}
	event := LogEvent{EventID: "evt-spooled", Time: time.Now().UTC(), Level: "info", Message: "spooled", Service: "svc", Env: "staging", Version: "v1", Host: "host", Unit: "svc.service", Source: "journald"}
	if err := spool.Enqueue(context.Background(), []LogEvent{event}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	sink := &fakeSink{}
	if err := spool.Flush(context.Background(), sink); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(sink.sent) != 1 || sink.sent[0].EventID != event.EventID {
		t.Fatalf("sent events = %+v", sink.sent)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool files remain = %d, want 0", len(entries))
	}
}

func TestFileSpoolEnforcesByteLimit(t *testing.T) {
	dir := t.TempDir()
	spool := FileSpool{Dir: dir, MaxBytes: 1}
	event := LogEvent{EventID: "evt-spooled", Time: time.Now().UTC(), Level: "info", Message: "spooled", Service: "svc", Env: "staging", Version: "v1", Host: "host", Unit: "svc.service", Source: "journald"}
	if err := spool.Enqueue(context.Background(), []LogEvent{event}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("spool files = %d, want 0 after enforcing tiny limit", len(matches))
	}
}

type fakeSource struct {
	records []JournalRecord
}

func (s *fakeSource) Read(ctx context.Context, after string, limit int) ([]JournalRecord, error) {
	var start int
	for i, record := range s.records {
		if record.Cursor == after {
			start = i + 1
			break
		}
	}
	if limit <= 0 || start+limit > len(s.records) {
		limit = len(s.records) - start
	}
	out := make([]JournalRecord, limit)
	copy(out, s.records[start:start+limit])
	return out, nil
}

type fakeSink struct {
	err  error
	sent []LogEvent
}

func (s *fakeSink) Send(ctx context.Context, events []LogEvent) error {
	if s.err != nil {
		return s.err
	}
	s.sent = append(s.sent, events...)
	return nil
}
