package cloudlogger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func testBillingInbox(t *testing.T) *BillingInbox {
	t.Helper()
	s, err := OpenBillingInbox(filepath.Join(privateBillingDir(t), "inbox.db"), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func privateBillingDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func billingEvent(id string) LogEvent {
	e := queryTestEvent(id, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	e.Stream, e.Source = "billing_usage", "billing_usage"
	e.Fields = map[string]any{"usage_event": map[string]any{"usage_id": id, "quantity": json.Number("9007199254740993")}}
	return e
}

func TestBillingInboxConcurrentReplayAndConflicts(t *testing.T) {
	ctx := context.Background()
	s := testBillingInbox(t)
	event := billingEvent("same-id")
	results := make(chan error, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- s.InsertEvent(ctx, event) }()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrDuplicateEvent) {
			t.Fatal(err)
		}
	}
	if accepted != 1 {
		t.Fatal("concurrent duplicates accepted", accepted)
	}
	retry := event
	retry.Version = "new-release"
	retry.Host = "replacement-pod"
	if err := s.InsertEvent(ctx, retry); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatal("deployment metadata broke replay", err)
	}
	for _, change := range []func(*LogEvent){
		func(e *LogEvent) { e.Env = "another-env" },
		func(e *LogEvent) { e.Time = e.Time.Add(time.Second) },
		func(e *LogEvent) { e.Fields = map[string]any{"usage_event": map[string]any{"quantity": 2}} },
	} {
		changed := event
		change(&changed)
		if err := s.InsertEvent(ctx, changed); !errors.Is(err, ErrBillingConflict) {
			t.Fatal(err)
		}
	}
	page, err := s.Page(ctx, "", 1)
	if err != nil || len(page.Records) != 1 || page.HasMore {
		t.Fatal(page, err)
	}
	fields := page.Records[0].Event.Fields["usage_event"].(map[string]any)
	if fields["quantity"].(json.Number).String() != "9007199254740993" {
		t.Fatal("rounded quantity")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenBillingInbox(s.path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.InsertEvent(ctx, event); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatal("lost durable receipt", err)
	}
	end, err := s2.Page(ctx, page.NextCursor, 10)
	if err != nil || len(end.Records) != 0 || end.StoreID != page.StoreID {
		t.Fatal(end, err)
	}
}

func TestBillingInboxPagesStableHorizonAndLateEvents(t *testing.T) {
	ctx := context.Background()
	s := testBillingInbox(t)
	for i := 0; i < 1002; i++ {
		if err := s.InsertEvent(ctx, billingEvent(fmt.Sprint("evt-", i))); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.Page(ctx, "", 1000)
	if err != nil || len(first.Records) != 1000 || !first.HasMore || first.HighWater != 1002 {
		t.Fatal(err, len(first.Records), first.HighWater)
	}
	late := billingEvent("late")
	late.Time = late.Time.Add(-24 * time.Hour)
	if err := s.InsertEvent(ctx, late); err != nil {
		t.Fatal(err)
	}
	second, err := s.Page(ctx, first.NextCursor, 1000)
	if err != nil || len(second.Records) != 2 || second.HasMore || second.HighWater != 1002 || second.Records[0].Sequence != 1001 {
		t.Fatal(second, err)
	}
	third, err := s.Page(ctx, second.NextCursor, 1000)
	if err != nil || len(third.Records) != 1 || third.HighWater != 1003 || third.Records[0].Event.EventID != "late" {
		t.Fatal(third, err)
	}
	other := testBillingInbox(t)
	if _, err := other.Page(ctx, third.NextCursor, 10); !errors.Is(err, ErrBillingCursor) {
		t.Fatal("foreign store cursor accepted", err)
	}
	// A gap is storage damage, never evidence of an empty/complete page.
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("events")).Delete(sequenceKey(1001)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Page(ctx, first.NextCursor, 1000); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal("gap not detected", err)
	}
}

func TestBillingInboxCrashChild(t *testing.T) {
	if os.Getenv("LOGGER_INBOX_CRASH_CHILD") != "1" {
		t.Skip("subprocess fixture")
	}
	s, err := OpenBillingInbox(os.Getenv("LOGGER_INBOX_CRASH_PATH"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InsertEvent(context.Background(), billingEvent("crash")); err != nil {
		t.Fatal(err)
	}
	os.Exit(0) // intentionally no Close: only synced transactions survive
}

func TestBillingInboxRecoversAfterProcessExit(t *testing.T) {
	path := filepath.Join(privateBillingDir(t), "inbox.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestBillingInboxCrashChild$")
	cmd.Env = append(os.Environ(), "LOGGER_INBOX_CRASH_CHILD=1", "LOGGER_INBOX_CRASH_PATH="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child %v %s", err, out)
	}
	s, err := OpenBillingInbox(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.InsertEvent(context.Background(), billingEvent("crash")); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatal(err)
	}
	page, err := s.Page(context.Background(), "", 10)
	if err != nil || len(page.Records) != 1 {
		t.Fatal(page, err)
	}
}

func TestBillingInboxRejectsUnsafeStorageAndInvalidInput(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "missing.db")
	if _, err := OpenBillingInbox(path, false); err == nil {
		t.Fatal("silently initialized missing retained data")
	}
	if _, err := OpenBillingInbox("relative.db", true); err == nil {
		t.Fatal("relative path accepted")
	}
	s := testBillingInbox(t)
	if _, err := OpenBillingInbox(s.path, false); err == nil {
		t.Fatal("competing writer accepted")
	}
	for _, cursor := range []string{"bad", strings.Repeat("a", 513), "e30"} {
		if _, err := s.Page(ctx, cursor, 10); !errors.Is(err, ErrBillingCursor) {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{0, 1001} {
		if _, err := s.Page(ctx, "", limit); !errors.Is(err, ErrBillingCursor) {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.InsertEvent(canceled, billingEvent("cancel")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e := billingEvent("redacted")
	e.Fields["password"] = "forbidden"
	if err := s.InsertEvent(ctx, e); err == nil {
		t.Fatal("silently altered financial fields")
	}
	e = billingEvent("badstream")
	e.Source = "journald"
	if err := s.InsertEvent(ctx, e); err == nil {
		t.Fatal("source mismatch")
	}
	if err := os.Rename(s.path, s.path+".retained"); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertEvent(ctx, billingEvent("lost-path")); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal(err)
	}
}

func TestBillingInboxHTTPBoundaryAndPagination(t *testing.T) {
	s := testBillingInbox(t)
	operational := NewMemoryEventStore()
	handler := IngestHandler(operational, IngestConfig{Token: "ops", BillingToken: "bill", BillingInbox: s})
	for _, token := range []string{"", "ops"} {
		req := httptest.NewRequest("GET", "/v1/billing-usage/events", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != 401 {
			t.Fatal(res.Code)
		}
	}
	for _, test := range []struct {
		path, token string
		status      int
	}{
		{"/v1/billing-usage/events?cursor=bad", "bill", 409},
		{"/v1/billing-usage/events?since=2026-01-01", "bill", 400},
		{"/v1/logs?stream=billing_usage", "ops", 401},
		{"/v1/logs", "bill", 401},
		{"/v1/billing-usage/events", "bill", 200},
	} {
		req := httptest.NewRequest("GET", test.path, nil)
		req.Header.Set("Authorization", "Bearer "+test.token)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != test.status {
			t.Fatal(test.path, res.Code)
		}
	}
	for _, e := range []LogEvent{billingEvent("one"), billingEvent("one")} {
		res := postIngest(t, handler, "bill", IngestRequest{Events: []LogEvent{e}})
		if res.StatusCode != 202 {
			t.Fatal(res.StatusCode)
		}
		var receipt IngestResponse
		if err := json.NewDecoder(res.Body).Decode(&receipt); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if len(receipt.Results) != 1 || receipt.Results[0].Status == IngestStatusRejected {
			t.Fatal(receipt)
		}
	}
	page, err := s.Page(context.Background(), "", 10)
	if err != nil || len(page.Records) != 1 || operational.Count() != 0 {
		t.Fatal(page, err)
	}
	// Even a legacy billing record in the operational backend cannot leak via
	// unfiltered support queries.
	if err := operational.InsertEvent(context.Background(), billingEvent("legacy")); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/v1/logs", nil)
	req.Header.Set("Authorization", "Bearer ops")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != 200 || strings.Contains(res.Body.String(), "legacy") {
		t.Fatal(res.Code, res.Body.String())
	}
	without := IngestHandler(operational, IngestConfig{Token: "ops", BillingToken: "bill"})
	res = httptest.NewRecorder()
	without.ServeHTTP(res, httptest.NewRequest("GET", "/healthz", nil))
	if res.Code != http.StatusServiceUnavailable {
		t.Fatal(res.Code)
	}
}

func TestBillingInboxBoundsAndCorruptReceipt(t *testing.T) {
	s := testBillingInbox(t)
	ctx := context.Background()
	e := billingEvent("large")
	e.Fields["notes"] = strings.Repeat("a", 1<<20)
	if err := s.InsertEvent(ctx, e); err == nil {
		t.Fatal("oversized financial event accepted")
	}
	for i := 0; i < 10; i++ {
		e = billingEvent(fmt.Sprint("large-", i))
		e.Fields["notes"] = strings.Repeat("a", 900000)
		if err := s.InsertEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.Page(ctx, "", 1000)
	if err != nil || !page.HasMore || len(page.Records) != 9 {
		t.Fatal(len(page.Records), err)
	}
	last, err := s.Page(ctx, page.NextCursor, 1000)
	if err != nil || last.HasMore || len(last.Records) != 1 {
		t.Fatal(len(last.Records), err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("events")).Put(sequenceKey(1), []byte(`{}`)) }); err != nil {
		t.Fatal(err)
	}
	e = billingEvent("large-0")
	e.Fields["notes"] = strings.Repeat("a", 900000)
	if err := s.InsertEvent(ctx, e); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal("corrupt duplicate acknowledged", err)
	}
	if err := s.Health(ctx); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal("poisoned inbox healthy", err)
	}
	if err := s.InsertEvent(ctx, billingEvent("after-corruption")); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal(err)
	}
}

func TestBillingInboxRejectsSymlinksClosedAndMalformedFiles(t *testing.T) {
	dir := privateBillingDir(t)
	for _, name := range []string{"empty", "invalid"} {
		body := []byte{}
		if name == "invalid" {
			body = []byte("not a billing inbox")
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenBillingInbox(path, true); err == nil {
			t.Fatal("replaced malformed retained data")
		}
	}
	s := testBillingInbox(t)
	link := filepath.Join(dir, "symlink")
	if err := os.Symlink(s.path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBillingInbox(link, false); err == nil {
		t.Fatal("symlink accepted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertEvent(context.Background(), billingEvent("closed")); err == nil {
		t.Fatal("closed accepted")
	}
	if _, err := s.Page(context.Background(), "", 10); err == nil {
		t.Fatal("closed queried")
	}
}

func TestBillingCredentialCannotBypassWithEmptyOrMixedStream(t *testing.T) {
	s := testBillingInbox(t)
	ops := NewMemoryEventStore()
	h := IngestHandler(ops, IngestConfig{Token: "ops", BillingToken: "bill", BillingInbox: s})
	req := httptest.NewRequest("POST", "/v1/logs/ingest", strings.NewReader(`{"events":[]} {}`))
	req.Header.Set("Authorization", "Bearer bill")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, req)
	if recorder.Code != 400 {
		t.Fatal("trailing JSON accepted", recorder.Code)
	}
	for _, test := range []struct {
		token string
		event LogEvent
	}{
		{"bill", queryTestEvent("operational", time.Now())},
		{"ops", billingEvent("billing")},
	} {
		res := postIngest(t, h, test.token, IngestRequest{Events: []LogEvent{test.event}})
		var body IngestResponse
		_ = json.NewDecoder(res.Body).Decode(&body)
		res.Body.Close()
		if len(body.Results) != 1 || body.Results[0].Status != IngestStatusRejected {
			t.Fatal(body)
		}
	}
	for _, cfg := range []IngestConfig{{Token: "ops"}, {BillingToken: "bill"}} {
		res := postIngest(t, IngestHandler(ops, cfg), "", IngestRequest{})
		res.Body.Close()
		if res.StatusCode != 401 {
			t.Fatal("empty credential authenticated", res.StatusCode)
		}
	}
	for _, path := range []string{"/v1/billing-usage/events?limit=1001", "/v1/billing-usage/events?cursor=e30"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer bill")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != 400 && res.Code != 409 {
			t.Fatal(res.Code)
		}
	}
}
