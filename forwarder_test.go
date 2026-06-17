package cloudlogger

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

func TestParseJournalRecordPromotesJSONMessageFields(t *testing.T) {
	line := []byte(`{"__CURSOR":"s=json","_BOOT_ID":"boot-a","_HOSTNAME":"host-a","_SYSTEMD_UNIT":"video_cloud-certissuer.service","MESSAGE":"{\"level\":\"info\",\"msg\":\"certificate issued\",\"service\":\"certissuer\",\"env\":\"staging\",\"version\":\"release-1\",\"trace_id\":\"trace-json\",\"request_id\":\"20260603T123846Z-rtk-0001\",\"operation_id\":\"factory-enroll\",\"device_id\":\"rtk-0001\",\"org_id\":\"org-rtk\",\"user_id\":\"user-1\",\"component\":\"issuer\",\"error_category\":\"\",\"actor_id\":\"factoryenroll\",\"actor_type\":\"system\",\"outcome\":\"success\",\"status_code\":200,\"status_class\":\"2xx\",\"method\":\"POST\",\"path\":\"/v1/certificates/issue\",\"status\":200,\"duration_ms\":3.4,\"remote_addr\":\"10.42.1.5\",\"caller_identity\":\"factoryenroll\"}","PRIORITY":"6","__REALTIME_TIMESTAMP":"1780297323000000"}`)
	record, err := ParseJournalRecord(line, JournalParseConfig{DefaultService: "fallback", DefaultEnv: "dev", DefaultVersion: "dev"})
	if err != nil {
		t.Fatalf("ParseJournalRecord: %v", err)
	}
	event := record.Event
	if event.Message != "certificate issued" || event.Service != "certissuer" || event.Env != "staging" || event.Version != "release-1" {
		t.Fatalf("promoted basic fields = %+v", event)
	}
	if event.TraceID != "trace-json" || event.RequestID != "20260603T123846Z-rtk-0001" || event.OperationID != "factory-enroll" {
		t.Fatalf("promoted request fields = %+v", event)
	}
	if event.DeviceID != "rtk-0001" || event.OrgID != "org-rtk" || event.UserID != "user-1" || event.Component != "issuer" {
		t.Fatalf("promoted identity fields = %+v", event)
	}
	if event.ActorID != "factoryenroll" || event.ActorType != "system" || event.Outcome != "success" || event.StatusCode != "200" || event.StatusClass != "2xx" {
		t.Fatalf("promoted admin audit fields = %+v", event)
	}
	if event.Fields["method"] != "POST" || event.Fields["path"] != "/v1/certificates/issue" || event.Fields["status"] != float64(200) {
		t.Fatalf("fields missing HTTP details: %+v", event.Fields)
	}
	if event.Fields["caller_identity"] != "factoryenroll" {
		t.Fatalf("fields missing certissuer details: %+v", event.Fields)
	}
}

func TestParseJournalRecordKeepsUppercaseJournaldFallbackWhenJSONOmitFields(t *testing.T) {
	line := []byte(`{"__CURSOR":"s=fallback","_BOOT_ID":"boot-a","_HOSTNAME":"host-a","_SYSTEMD_UNIT":"svc.service","MESSAGE":"{\"msg\":\"handled\"}","PRIORITY":"6","__REALTIME_TIMESTAMP":"1780297323000000","SERVICE":"svc","ENV":"staging","VERSION":"v1","TRACE_ID":"trace-uppercase","REQUEST_ID":"req-uppercase","OPERATION_ID":"op-uppercase","DEVICE_ID":"device-uppercase","ORG_ID":"org-uppercase","USER_ID":"user-uppercase","COMPONENT":"component-uppercase"}`)
	record, err := ParseJournalRecord(line, JournalParseConfig{DefaultService: "fallback", DefaultEnv: "dev", DefaultVersion: "dev"})
	if err != nil {
		t.Fatalf("ParseJournalRecord: %v", err)
	}
	event := record.Event
	if event.Message != "handled" {
		t.Fatalf("message = %q, want handled", event.Message)
	}
	if event.TraceID != "trace-uppercase" || event.RequestID != "req-uppercase" || event.OperationID != "op-uppercase" {
		t.Fatalf("uppercase request fallback fields = %+v", event)
	}
	if event.DeviceID != "device-uppercase" || event.OrgID != "org-uppercase" || event.UserID != "user-uppercase" || event.Component != "component-uppercase" {
		t.Fatalf("uppercase identity fallback fields = %+v", event)
	}
}

func TestParseJournalRecordRedactsSensitiveJSONMessageFields(t *testing.T) {
	line := []byte(`{"__CURSOR":"s=redact","_BOOT_ID":"boot-a","_HOSTNAME":"host-a","_SYSTEMD_UNIT":"svc.service","MESSAGE":"{\"msg\":\"issued private key\",\"service\":\"svc\",\"env\":\"staging\",\"version\":\"v1\",\"request_id\":\"req-redact\",\"private_key_pem\":\"-----BEGIN EC PRIVATE KEY-----\\nsecret\\n-----END EC PRIVATE KEY-----\",\"access_token\":\"raw-token\",\"safe\":\"ok\"}","PRIORITY":"6","__REALTIME_TIMESTAMP":"1780297323000000"}`)
	record, err := ParseJournalRecord(line, JournalParseConfig{DefaultService: "fallback", DefaultEnv: "dev", DefaultVersion: "dev"})
	if err != nil {
		t.Fatalf("ParseJournalRecord: %v", err)
	}
	if record.Event.Message != RedactedValue {
		t.Fatalf("message = %q, want redacted", record.Event.Message)
	}
	if record.Event.Fields["private_key_pem"] != RedactedValue || record.Event.Fields["access_token"] != RedactedValue {
		t.Fatalf("sensitive fields not redacted: %+v", record.Event.Fields)
	}
	if record.Event.Fields["safe"] != "ok" {
		t.Fatalf("safe field = %v, want ok", record.Event.Fields["safe"])
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

func TestFileSpoolStatsReportsBacklog(t *testing.T) {
	dir := t.TempDir()
	spool := FileSpool{Dir: dir, MaxBytes: 1 << 20}
	oldPath := filepath.Join(dir, "old.json")
	newPath := filepath.Join(dir, "new.json")
	if err := os.WriteFile(oldPath, []byte(`{"events":[]}`), 0o600); err != nil {
		t.Fatalf("write old spool: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"events":[]}`), 0o600); err != nil {
		t.Fatalf("write new spool: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(oldPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes old spool: %v", err)
	}

	stats, err := spool.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.FileCount != 2 {
		t.Fatalf("file_count = %d, want 2", stats.FileCount)
	}
	if stats.Bytes <= 0 {
		t.Fatalf("bytes = %d, want > 0", stats.Bytes)
	}
	if stats.OldestAgeSeconds < 60 {
		t.Fatalf("oldest_age_seconds = %d, want at least 60", stats.OldestAgeSeconds)
	}
}

func TestFileSpoolQuarantinesCorruptFileAndFlushesValidEvents(t *testing.T) {
	dir := t.TempDir()
	spool := FileSpool{Dir: dir, MaxBytes: 1 << 20}
	badPath := filepath.Join(dir, "000-bad.json")
	goodPath := filepath.Join(dir, "001-good.json")
	if err := os.WriteFile(badPath, []byte(`not json`), 0o600); err != nil {
		t.Fatalf("write bad spool: %v", err)
	}
	event := LogEvent{EventID: "evt-valid-spool", Time: time.Now().UTC(), Level: "info", Message: "spooled", Service: "svc", Env: "staging", Version: "v1", Host: "host", Unit: "svc.service", Source: "journald"}
	data, err := json.Marshal(IngestRequest{Events: []LogEvent{event}})
	if err != nil {
		t.Fatalf("marshal valid spool: %v", err)
	}
	if err := os.WriteFile(goodPath, data, 0o600); err != nil {
		t.Fatalf("write good spool: %v", err)
	}

	sink := &fakeSink{}
	if err := spool.Flush(context.Background(), sink); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(sink.sent) != 1 || sink.sent[0].EventID != event.EventID {
		t.Fatalf("sent events = %+v, want valid event", sink.sent)
	}
	if _, err := os.Stat(filepath.Join(dir, "bad", "000-bad.json")); err != nil {
		t.Fatalf("bad spool was not quarantined: %v", err)
	}
	if _, err := os.Stat(goodPath); !os.IsNotExist(err) {
		t.Fatalf("good spool still exists or stat failed: %v", err)
	}
}

func TestFileSpoolDoesNotQuarantineOnSinkFailure(t *testing.T) {
	dir := t.TempDir()
	spool := FileSpool{Dir: dir, MaxBytes: 1 << 20}
	event := LogEvent{EventID: "evt-send-failure", Time: time.Now().UTC(), Level: "info", Message: "spooled", Service: "svc", Env: "staging", Version: "v1", Host: "host", Unit: "svc.service", Source: "journald"}
	data, err := json.Marshal(IngestRequest{Events: []LogEvent{event}})
	if err != nil {
		t.Fatalf("marshal valid spool: %v", err)
	}
	spoolPath := filepath.Join(dir, "valid.json")
	if err := os.WriteFile(spoolPath, data, 0o600); err != nil {
		t.Fatalf("write valid spool: %v", err)
	}

	err = spool.Flush(context.Background(), &fakeSink{err: errors.New("backend down")})
	if err == nil {
		t.Fatalf("Flush succeeded, want sink error")
	}
	if _, statErr := os.Stat(spoolPath); statErr != nil {
		t.Fatalf("valid spool should remain after sink failure: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "bad")); !os.IsNotExist(statErr) {
		t.Fatalf("bad directory exists after sink failure: %v", statErr)
	}
}

func TestForwarderStatusHandlerReportsHealthAndSpoolStats(t *testing.T) {
	dir := t.TempDir()
	spool := FileSpool{Dir: dir, MaxBytes: 1 << 20}
	event := LogEvent{EventID: "evt-status-spool", Time: time.Now().UTC(), Level: "info", Message: "spooled", Service: "svc", Env: "staging", Version: "v1", Host: "host", Unit: "svc.service", Source: "journald"}
	if err := spool.Enqueue(context.Background(), []LogEvent{event}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	forwarder := NewForwarder(&fakeSource{}, &fakeSink{}, NewMemoryCursorStore(), ForwarderConfig{}).WithSpool(spool)
	handler := ForwarderStatusHandler(forwarder)

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusNoContent {
		t.Fatalf("health status = %d, want %d", health.Code, http.StatusNoContent)
	}

	statusRec := httptest.NewRecorder()
	handler.ServeHTTP(statusRec, httptest.NewRequest(http.MethodGet, "/status", nil))
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", statusRec.Code, http.StatusOK)
	}
	var status ForwarderStatus
	if err := json.NewDecoder(statusRec.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.SpoolFileCount != 1 || status.SpoolBytes <= 0 {
		t.Fatalf("status spool stats = %+v, want one queued file", status)
	}

	degraded := NewForwarder(&fakeSource{records: []JournalRecord{{Cursor: "cursor-1", Event: event}}}, &fakeSink{err: errors.New("backend down")}, NewMemoryCursorStore(), ForwarderConfig{})
	if err := degraded.RunOnce(context.Background()); err == nil {
		t.Fatalf("RunOnce succeeded, want backend error")
	}
	degradedHealth := httptest.NewRecorder()
	ForwarderStatusHandler(degraded).ServeHTTP(degradedHealth, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if degradedHealth.Code != http.StatusServiceUnavailable {
		t.Fatalf("degraded health status = %d, want %d", degradedHealth.Code, http.StatusServiceUnavailable)
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
