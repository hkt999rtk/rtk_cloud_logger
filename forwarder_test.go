package cloudlogger

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestForwarderDeliversJournalRecordsAndAdvancesCursorAfterAck(t *testing.T) {
	dir := t.TempDir()
	cursorStore := NewFileCursorStore(filepath.Join(dir, "cursor.json"))
	spool := NewDiskSpool(filepath.Join(dir, "spool.jsonl"), 10)
	delivery := &recordingDelivery{}
	forwarder := NewForwarder(ForwarderConfig{
		Source:      NewSliceRecordSource([]JournalRecord{testJournalRecord("cursor-1")}),
		CursorStore: cursorStore,
		Spool:       spool,
		Delivery:    delivery,
		Now:         fixedNow,
	})

	if err := forwarder.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("ProcessOnce: %v", err)
	}

	status := forwarder.Status()
	if status.Cursor != "cursor-1" {
		t.Fatalf("cursor = %q, want cursor-1", status.Cursor)
	}
	if status.LastUploadTime.IsZero() {
		t.Fatalf("LastUploadTime was not set: %#v", status)
	}
	if status.Degraded {
		t.Fatalf("status degraded after successful upload: %#v", status)
	}
	if len(delivery.events) != 1 {
		t.Fatalf("delivered %d events, want 1", len(delivery.events))
	}
	if delivery.events[0].EventID != StableEventID("host-1", "boot-1", "video-cloud-api.service", "cursor-1") {
		t.Fatalf("event_id = %q, want stable journal id", delivery.events[0].EventID)
	}
}

func TestForwarderKeepsCursorAndReportsDegradedWhenBackendUnavailable(t *testing.T) {
	dir := t.TempDir()
	cursorStore := NewFileCursorStore(filepath.Join(dir, "cursor.json"))
	delivery := &recordingDelivery{err: errors.New("backend unavailable")}
	forwarder := NewForwarder(ForwarderConfig{
		Source:      NewSliceRecordSource([]JournalRecord{testJournalRecord("cursor-1")}),
		CursorStore: cursorStore,
		Spool:       NewDiskSpool(filepath.Join(dir, "spool.jsonl"), 10),
		Delivery:    delivery,
		Now:         fixedNow,
	})

	if err := forwarder.ProcessOnce(context.Background()); err == nil {
		t.Fatal("ProcessOnce succeeded, want delivery error")
	}

	status := forwarder.Status()
	if status.Cursor != "" {
		t.Fatalf("cursor advanced to %q before ack", status.Cursor)
	}
	if !status.Degraded {
		t.Fatalf("status should be degraded after delivery failure: %#v", status)
	}
	if status.SpoolRecords != 1 {
		t.Fatalf("spool records = %d, want 1", status.SpoolRecords)
	}
}

func TestForwarderRetriesWithBackoffBeforeReportingSuccess(t *testing.T) {
	dir := t.TempDir()
	delivery := &recordingDelivery{failuresBeforeSuccess: 2}
	backoffs := make([]time.Duration, 0)
	forwarder := NewForwarder(ForwarderConfig{
		Source:      NewSliceRecordSource([]JournalRecord{testJournalRecord("cursor-1")}),
		CursorStore: NewFileCursorStore(filepath.Join(dir, "cursor.json")),
		Spool:       NewDiskSpool(filepath.Join(dir, "spool.jsonl"), 10),
		Delivery:    delivery,
		MaxAttempts: 3,
		Backoff: func(attempt int) time.Duration {
			return time.Duration(attempt) * time.Millisecond
		},
		Sleep: func(ctx context.Context, d time.Duration) error {
			backoffs = append(backoffs, d)
			return nil
		},
		Now: fixedNow,
	})

	if err := forwarder.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("ProcessOnce: %v", err)
	}
	if len(backoffs) != 2 || backoffs[0] != time.Millisecond || backoffs[1] != 2*time.Millisecond {
		t.Fatalf("backoffs = %#v, want [1ms 2ms]", backoffs)
	}
	if status := forwarder.Status(); status.Degraded || status.Cursor != "cursor-1" {
		t.Fatalf("status = %#v, want non-degraded cursor-1", status)
	}
}

func TestDiskSpoolDropsOldestRecordsWhenBounded(t *testing.T) {
	spool := NewDiskSpool(filepath.Join(t.TempDir(), "spool.jsonl"), 2)
	if err := spool.Append(validLogEvent("event-1")); err != nil {
		t.Fatalf("append event-1: %v", err)
	}
	if err := spool.Append(validLogEvent("event-2")); err != nil {
		t.Fatalf("append event-2: %v", err)
	}
	if err := spool.Append(validLogEvent("event-3")); err != nil {
		t.Fatalf("append event-3: %v", err)
	}

	events, err := spool.Load()
	if err != nil {
		t.Fatalf("load spool: %v", err)
	}
	if len(events) != 2 || events[0].EventID != "event-2" || events[1].EventID != "event-3" {
		t.Fatalf("spooled events = %#v, want event-2,event-3", events)
	}
	if spool.DroppedRecords() != 1 {
		t.Fatalf("dropped records = %d, want 1", spool.DroppedRecords())
	}
}

func TestFileRecordSourceReadsJSONLinesAfterCursor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	records := []JournalRecord{
		testJournalRecord("cursor-1"),
		testJournalRecord("cursor-2"),
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create records file: %v", err)
	}
	for _, record := range records {
		if err := json.NewEncoder(file).Encode(record); err != nil {
			t.Fatalf("encode record: %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close records file: %v", err)
	}

	source := NewFileRecordSource(path)
	record, ok, err := source.Next(context.Background(), "cursor-1")
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if !ok || record.Cursor != "cursor-2" {
		t.Fatalf("record = %#v ok=%v, want cursor-2", record, ok)
	}
}

func TestForwarderStatusJSON(t *testing.T) {
	forwarder := NewForwarder(ForwarderConfig{
		Spool: NewDiskSpool(filepath.Join(t.TempDir(), "spool.jsonl"), 10),
		Now:   fixedNow,
	})
	forwarder.setStatus(ForwarderStatus{Cursor: "cursor-1", Degraded: true, LastError: "backend unavailable"})

	statusJSON, err := forwarder.StatusJSON()
	if err != nil {
		t.Fatalf("StatusJSON: %v", err)
	}
	for _, want := range []string{`"cursor":"cursor-1"`, `"degraded":true`, `"last_error":"backend unavailable"`} {
		if !bytesContains(statusJSON, want) {
			t.Fatalf("status json %s does not contain %s", string(statusJSON), want)
		}
	}
}

type recordingDelivery struct {
	events                []LogEvent
	err                   error
	failuresBeforeSuccess int
}

func (d *recordingDelivery) Deliver(ctx context.Context, events []LogEvent) ([]IngestResult, error) {
	if d.failuresBeforeSuccess > 0 {
		d.failuresBeforeSuccess--
		return nil, errors.New("temporary backend unavailable")
	}
	if d.err != nil {
		return nil, d.err
	}
	d.events = append(d.events, events...)
	results := make([]IngestResult, len(events))
	for i, event := range events {
		results[i] = IngestResult{EventID: event.EventID, Accepted: true}
	}
	return results, nil
}

func testJournalRecord(cursor string) JournalRecord {
	return JournalRecord{
		Cursor: cursor,
		BootID: "boot-1",
		HostID: "host-1",
		Host:   "api-1",
		Unit:   "video-cloud-api.service",
		Source: "journald",
		Fields: map[string]any{
			"ts":       fixedNow().Format(time.RFC3339Nano),
			"level":    "info",
			"msg":      "event",
			"service":  "video-cloud-api",
			"env":      "staging",
			"version":  "v1",
			"trace_id": "trace-1",
		},
	}
}

func fixedNow() time.Time {
	return time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC)
}

func bytesContains(data []byte, want string) bool {
	return strings.Contains(string(data), want)
}
