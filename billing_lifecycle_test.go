package cloudlogger

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
)

type memoryArchive struct {
	mu      sync.Mutex
	objects map[string][]byte
	fail    bool
}

func (s *memoryArchive) PutImmutable(_ context.Context, key, path string, size int64, sha string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if int64(len(b)) != size || billingarchive.Digest(b) != sha {
		return errors.New("local mismatch")
	}
	if s.fail {
		return errors.New("injected upload failure")
	}
	if previous, ok := s.objects[key]; ok && !bytes.Equal(previous, b) {
		return errors.New("immutable collision")
	}
	s.objects[key] = b
	return nil
}
func lifecycleInbox(t *testing.T) (*BillingInbox, *age.X25519Identity, ed25519.PrivateKey, *memoryArchive) {
	t.Helper()
	s := testBillingInbox(t)
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	objects := &memoryArchive{objects: map[string][]byte{}}
	c := LifecycleConfig{Environment: "staging", Stack: "rtk", BackupEnabled: true, RetirementEnabled: true, CompactionEnabled: true, ScratchDir: privateBillingDir(t), ScratchCapacityBytes: 8 << 30, EncryptionKeyID: "key-v1", Recipients: []string{identity.Recipient().String()}, VerifierKeys: map[string]string{"verifier-v1": base64.StdEncoding.EncodeToString(pub)}, PartBytes: 1 << 20, ObjectStore: objects, authority: func(context.Context, RetirementStatus) error { return nil }}
	if err := s.ConfigureLifecycle(c); err != nil {
		t.Fatal(err)
	}
	return s, identity, key, objects
}
func canonicalBillingEvent(id string) LogEvent {
	e := billingEvent(id)
	e.Env = "staging"
	e.Fields = map[string]any{"usage_event": map[string]any{"usage_id": id, "service_code": "mqtt", "brand_cloud_id": "brand", "event_time": e.Time, "window_start": e.Time.Add(-time.Minute), "window_end": e.Time, "meter_epoch": "epoch", "sequence": json.Number("9007199254740993"), "source": "meter", "measurements": []any{map[string]any{"metric_code": "publish_bytes", "unit": "bytes", "quantity": json.Number("9007199254740993")}}}}
	return e
}
func migrateAll(t *testing.T, s *BillingInbox) {
	t.Helper()
	for {
		st, err := s.Migrate(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if st.Version == "2" {
			return
		}
	}
}
func verifyCapture(t *testing.T, s *BillingInbox, key ed25519.PrivateKey, st CaptureStatus) {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(st.ManifestB64)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := billingarchive.SignCompletion(billingarchive.PayloadFor(st.Manifest, b, "verifier-v1", "1", "test", time.Now()), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.VerifyCatalog(context.Background(), st.SetID, completion); err != nil {
		t.Fatal(err)
	}
}

func TestOnlineMigrationResumesAndPreservesOriginalBytes(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := s.InsertEvent(ctx, canonicalBillingEvent(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	var before [][]byte
	s.db.View(func(tx *bolt.Tx) error {
		for i := uint64(1); i <= 3; i++ {
			before = append(before, bytes.Clone(tx.Bucket([]byte("events")).Get(sequenceKey(i))))
		}
		return nil
	})
	st, err := s.Migrate(ctx, 1)
	if err != nil || st.Version != "2-migrating" || st.BackfilledThrough != 1 {
		t.Fatal(st, err)
	}
	if err := s.InsertEvent(ctx, canonicalBillingEvent("new")); err != nil {
		t.Fatal(err)
	}
	anchor := s.anchor
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := OpenBillingInbox(anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	migrateAll(t, resumed)
	after, _ := resumed.Migration(ctx)
	if after.LegacyThrough != 3 || after.BackfilledThrough != 3 || after.LegacyAgeFloor.IsZero() {
		t.Fatal(after)
	}
	resumed.db.View(func(tx *bolt.Tx) error {
		for i := uint64(1); i <= 3; i++ {
			if !bytes.Equal(before[i-1], tx.Bucket([]byte("events")).Get(sequenceKey(i))) {
				t.Fatal("mutated original event")
			}
			at, err := receiptTime(tx, i)
			if err != nil || !at.Equal(after.LegacyAgeFloor) {
				t.Fatal(at, err)
			}
		}
		if at, err := receiptTime(tx, 4); err != nil || !at.Before(after.LegacyAgeFloor) {
			t.Fatal("new insert lost real receipt", at, err)
		}
		return nil
	})
	if err := resumed.InsertEvent(ctx, canonicalBillingEvent("0")); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatal(err)
	}
	page, err := resumed.Page(ctx, "", 10)
	if err != nil || page.HighWater != 4 || len(page.Records) != 4 {
		t.Fatal(page, err)
	}
}

func TestCaptureSealsEncryptedPartsResumesAndRequiresIndependentSignature(t *testing.T) {
	s, identity, key, objects := lifecycleInbox(t)
	ctx := context.Background()
	migrateAll(t, s)
	for i := 0; i < 2; i++ {
		event := canonicalBillingEvent(fmt.Sprint(i))
		event.Fields["usage_event"].(map[string]any)["dimensions"] = map[string]string{"padding": strings.Repeat("z", 700<<10)}
		if err := s.InsertEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	objects.fail = true
	if _, err := s.CompactOnline(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := s.Capture(ctx)
	if err == nil || st.Status != "sealed" {
		t.Fatal(st.Status, err)
	}
	if _, err := os.Stat(filepath.Join(s.lifecycle.ScratchDir, st.SetID, "snapshot.db")); !os.IsNotExist(err) {
		t.Fatal("plaintext persisted after seal", err)
	}
	immutable := st.ManifestB64
	objects.fail = false
	retried, err := s.Capture(ctx)
	if err != nil || retried.SetID != st.SetID || retried.ManifestB64 != immutable || retried.Status != "uploaded" {
		t.Fatal(retried.Status, err)
	}
	var snapshot bytes.Buffer
	records := []billingarchive.ExportRecord{}
	for _, p := range st.Manifest.Parts {
		cipher := objects.objects[ArchivePartKey(st.Manifest, p)]
		var plain bytes.Buffer
		if err := billingarchive.DecodePart(ctx, &plain, bytes.NewReader(cipher), []age.Identity{identity}, p); err != nil {
			t.Fatal(err)
		}
		if p.Kind == "snapshot" {
			snapshot.Write(plain.Bytes())
		} else {
			d := json.NewDecoder(&plain)
			for {
				var record billingarchive.ExportRecord
				err := d.Decode(&record)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := ValidateBillingExportRecord(record); err != nil {
					t.Fatal(err)
				}
				records = append(records, record)
			}
		}
	}
	if len(records) != 2 || snapshot.Len() != int(st.Manifest.SnapshotBytes) || billingarchive.Digest(snapshot.Bytes()) != st.Manifest.SnapshotSHA256 {
		t.Fatal("incomplete snapshot/export")
	}
	snapshotPath := filepath.Join(privateBillingDir(t), "snapshot.db")
	if err := os.WriteFile(snapshotPath, snapshot.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBillingSnapshot(ctx, snapshotPath, st.Manifest); err != nil {
		t.Fatal(err)
	}
	var exportBytes bytes.Buffer
	for _, record := range records {
		b, _ := json.Marshal(record)
		exportBytes.Write(b)
		exportBytes.WriteByte('\n')
	}
	if err := ValidateBillingSnapshotExports(ctx, snapshotPath, st.Manifest, bytes.NewReader(exportBytes.Bytes())); err != nil {
		t.Fatal(err)
	}
	records[0].ReceivedAt = records[0].ReceivedAt.Add(-100 * 24 * time.Hour)
	var forgedExport bytes.Buffer
	for _, record := range records {
		b, _ := json.Marshal(record)
		forgedExport.Write(b)
		forgedExport.WriteByte('\n')
	}
	if err := ValidateBillingSnapshotExports(ctx, snapshotPath, st.Manifest, &forgedExport); err == nil {
		t.Fatal("signed age could be substituted without snapshot crosslink")
	}
	prepared := filepath.Join(privateBillingDir(t), "billing-inbox.db")
	if err := os.WriteFile(prepared, snapshot.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	state, err := PrepareBillingSnapshotRecovery(ctx, prepared, st.Manifest, s.lifecycle.keys)
	if err != nil || !state.Fenced || state.HighWater != 2 {
		t.Fatal(state, err)
	}
	staged, err := OpenBillingInbox(prepared, false)
	if err != nil {
		t.Fatal("prepared copied generation not loadable", err)
	}
	if err := staged.Health(ctx); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal("prepared restore not fenced", err)
	}
	staged.Close()
	if _, err := PrepareBillingSnapshotRecovery(ctx, prepared, st.Manifest, s.lifecycle.keys); !errors.Is(err, ErrLifecycleConflict) {
		t.Fatal("stage overwrote existing active metadata", err)
	}
	otherPub, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	_ = otherPub
	b, _ := base64.StdEncoding.DecodeString(st.ManifestB64)
	forged, _ := billingarchive.SignCompletion(billingarchive.PayloadFor(st.Manifest, b, "verifier-v1", "1", "test", time.Now()), otherKey)
	if _, err := s.VerifyCatalog(ctx, st.SetID, forged); !errors.Is(err, ErrLifecycleConflict) {
		t.Fatal("accepted writer-forged receipt", err)
	}
	verifyCapture(t, s, key, st)
	if status := s.Worker(ctx); status.ProtectionOverdue || status.ProtectedHighWater != 2 {
		t.Fatal(status)
	}
}

func TestRetireAbortAndArchivedCursorCachePreserveDedupe(t *testing.T) {
	s, _, key, _ := lifecycleInbox(t)
	ctx := context.Background()
	migrateAll(t, s)
	event := canonicalBillingEvent("original")
	if err := s.InsertEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-91 * 24 * time.Hour)
	if err := s.update(func(t *mutationTx) error {
		return t.put("receipt_times", sequenceKey(1), []byte(old.Format(time.RFC3339Nano)))
	}); err != nil {
		t.Fatal(err)
	}
	st, err := s.Capture(ctx)
	if err != nil {
		t.Fatal(err)
	}
	verifyCapture(t, s, key, st)
	var raw []byte
	s.db.View(func(tx *bolt.Tx) error {
		raw = bytes.Clone(tx.Bucket([]byte("events")).Get(sequenceKey(1)))
		return nil
	})
	plan := RetirementPlan{OperationID: "retire-1", Environment: "staging", StoreID: st.Manifest.StoreID, FromSequence: 1, ThroughSequence: 1, PlanSHA256: strings.Repeat("a", 64), SetID: st.SetID}
	if _, err := s.PlanRetirement(ctx, plan); err != nil {
		t.Fatal(err)
	}
	s.lifecycle.authority = func(context.Context, RetirementStatus) error { return errors.New("authority unreachable") }
	if _, err := s.ApplyRetirement(ctx, plan.OperationID); err == nil {
		t.Fatal("deleted without live authority")
	}
	s.lifecycle.authority = func(context.Context, RetirementStatus) error { return nil }
	done, err := s.ApplyRetirement(ctx, plan.OperationID)
	if err != nil || done.Status != "completed" || done.RetiredThrough != 1 || len(done.ReceiptSHA256) != 64 {
		t.Fatal(done, err)
	}
	again, err := s.ApplyRetirement(ctx, plan.OperationID)
	if err != nil || !equalJSON(done, again) {
		t.Fatal("terminal retry changed", again, err)
	}
	if _, err := s.Page(ctx, "", 10); !errors.Is(err, ErrBillingArchiveUnavailable) {
		t.Fatal(err)
	}
	if err := s.Health(ctx); err != nil {
		t.Fatal("archive miss poisoned health", err)
	}
	if err := s.InsertEvent(ctx, event); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatal("retired dedupe lost", err)
	}
	changed := event
	changed.Time = changed.Time.Add(time.Second)
	if err := s.InsertEvent(ctx, changed); !errors.Is(err, ErrBillingConflict) {
		t.Fatal(err)
	}
	r, _ := decodeBillingRecord(raw, 1)
	export := billingarchive.ExportRecord{Sequence: 1, ContentSHA256: r.ContentSHA256, ReceivedAt: old, Record: raw}
	var modified BillingRecord
	json.Unmarshal(raw, &modified)
	modified.Event.Host = "forged"
	altered, _ := json.Marshal(modified)
	export.Record = altered
	if err := s.Rehydrate(ctx, st.SetID, []billingarchive.ExportRecord{export}); err == nil {
		t.Fatal("accepted altered original bytes")
	}
	export.Record = raw
	if err := s.Rehydrate(ctx, st.SetID, []billingarchive.ExportRecord{export}); err != nil {
		t.Fatal(err)
	}
	page, err := s.Page(ctx, "", 10)
	if err != nil || len(page.Records) != 1 {
		t.Fatal(page, err)
	}
	if err := s.EvictCache(ctx, 1); err != nil {
		t.Fatal(err)
	}
	abortPlan := plan
	abortPlan.OperationID = "aborted-unknown"
	aborted, err := s.AbortRetirementPlan(ctx, abortPlan)
	if err != nil || aborted.Status != "aborted" {
		t.Fatal(aborted, err)
	}
	if _, err := s.PlanRetirement(ctx, abortPlan); err != nil {
		t.Fatal("bound abort tombstone should be observable", err)
	}
	if _, err := s.ApplyRetirement(ctx, abortPlan.OperationID); !errors.Is(err, ErrLifecycleConflict) {
		t.Fatal("late apply crossed abort tombstone", err)
	}
}

func TestDisabledRetirementStillAllowsAuthorizedPendingAndUnknownAbort(t *testing.T) {
	s, _, key, _ := lifecycleInbox(t)
	ctx := context.Background()
	migrateAll(t, s)
	if err := s.InsertEvent(ctx, canonicalBillingEvent("abort-preserves-body")); err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(tx *mutationTx) error {
		return tx.put("receipt_times", sequenceKey(1), []byte(time.Now().UTC().Add(-91*24*time.Hour).Format(time.RFC3339Nano)))
	}); err != nil {
		t.Fatal(err)
	}
	capture, err := s.Capture(ctx)
	if err != nil {
		t.Fatal(err)
	}
	verifyCapture(t, s, key, capture)
	pending := RetirementPlan{OperationID: "pending-disabled", Environment: "staging", StoreID: capture.Manifest.StoreID, FromSequence: 1, ThroughSequence: 1, PlanSHA256: strings.Repeat("a", 64), SetID: capture.SetID}
	if _, err := s.PlanRetirement(ctx, pending); err != nil {
		t.Fatal(err)
	}
	var statusMu sync.Mutex
	authorityStatus := "ACTIVE"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer abort-authority" {
			t.Error("missing dedicated authority credential")
		}
		statusMu.Lock()
		status := authorityStatus
		statusMu.Unlock()
		response := map[string]any{"operation_id": strings.TrimPrefix(r.URL.Path, "/v1/internal/billing/raw-retention/operations/"), "environment": pending.Environment, "store_id": pending.StoreID, "from_sequence": "1", "through_sequence": "1", "plan_sha256": pending.PlanSHA256, "set_id": pending.SetID, "status": status}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	cfg := s.lifecycle
	cfg.RetirementEnabled = false
	cfg.authority = nil
	cfg.AuthorityURL = server.URL
	cfg.AuthorityToken = "abort-authority"
	if err := s.ConfigureLifecycle(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyRetirement(ctx, pending.OperationID); !errors.Is(err, ErrLifecycleConflict) {
		t.Fatal("disabled retirement allowed payload deletion", err)
	}
	handler := LifecycleHandler(IngestConfig{BillingInbox: s, LifecycleToken: "abort-controller"})
	for _, operationID := range []string{pending.OperationID, "unknown-disabled"} {
		plan := pending
		plan.OperationID = operationID
		if _, err := s.PlanRetirement(ctx, plan); !errors.Is(err, ErrLifecycleConflict) {
			t.Fatal("disabled retirement accepted a new plan", err)
		}
		postAbort := func() *httptest.ResponseRecorder {
			body, _ := json.Marshal(plan)
			request := httptest.NewRequest(http.MethodPost, LifecyclePath+"retire/abort", bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer abort-controller")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			return recorder
		}
		statusMu.Lock()
		authorityStatus = "ACTIVE"
		statusMu.Unlock()
		if response := postAbort(); response.Code != http.StatusConflict {
			t.Fatal("abort accepted without live ABORT_REQUESTED", response.Code, response.Body.String())
		}
		statusMu.Lock()
		authorityStatus = "ABORT_REQUESTED"
		statusMu.Unlock()
		response := postAbort()
		var aborted RetirementStatus
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &aborted) != nil || aborted.Status != "aborted" || aborted.RetiredThrough != 0 || !validDigest(aborted.ReceiptSHA256) {
			t.Fatal("disabled retirement stranded an authorized abort", response.Code, response.Body.String())
		}
		statusMu.Lock()
		authorityStatus = "ACTIVE"
		statusMu.Unlock()
		again, err := s.AbortRetirementPlan(ctx, plan)
		if err != nil || !equalJSON(aborted, again) {
			t.Fatal("terminal abort retry changed receipt", again, err)
		}
		if _, err := s.ApplyRetirement(ctx, operationID); !errors.Is(err, ErrLifecycleConflict) {
			t.Fatal("late apply crossed permanent abort tombstone", err)
		}
	}
	if page, err := s.Page(ctx, "", 10); err != nil || len(page.Records) != 1 || page.Records[0].Event.EventID != "abort-preserves-body" {
		t.Fatal("abort deleted original body", page, err)
	}
}

func TestOnlineGenerationCompactionAndManifestFailClosed(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	ctx := context.Background()
	migrateAll(t, s)
	for i := 0; i < 250; i++ {
		if err := s.InsertEvent(ctx, canonicalBillingEvent(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := s.Page(ctx, "", 10)
	result, err := s.CompactOnline(ctx)
	if err != nil || result.Generation == "" || !result.QueueTargetMet {
		t.Fatal(result, err)
	}
	after, err := s.Page(ctx, before.NextCursor, 10)
	if err != nil || after.StoreID != before.StoreID || after.Records[0].Sequence != 11 {
		t.Fatal(after, err)
	}
	if err := s.InsertEvent(ctx, canonicalBillingEvent("after")); err != nil {
		t.Fatal(err)
	}
	anchor := s.anchor
	active := s.path
	if !strings.Contains(active, ".gen-") {
		t.Fatal(active)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		info, err := os.Stat(anchor)
		if err == nil && info.Size() < 1024 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("legacy anchor did not reclaim its database pages")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := s.CompactOnline(ctx); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		_, err := os.Stat(active)
		if os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("closed previous generation was not reclaimed")
		}
		time.Sleep(time.Millisecond)
	}
	s.Close()
	reopened, err := OpenBillingInbox(anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	page, err := reopened.Page(ctx, "", 1000)
	if err != nil || page.HighWater != 251 || page.StoreID != before.StoreID {
		t.Fatal(page, err)
	}
	reopened.Close()
	manifest := anchor + ".active.json"
	if err := os.Rename(manifest, manifest+".saved"); err != nil {
		t.Fatal(err)
	}
	if bad, err := OpenBillingInbox(anchor, false); err == nil {
		bad.Close()
		t.Fatal("silently rolled back to old generation")
	}
}

func TestLifecycleHTTPTokenIsolationAndArchiveMiss(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	handler := LifecycleHandler(IngestConfig{Token: "ops", BillingToken: "billing", LifecycleToken: "control", LifecycleReadToken: "read", BillingInbox: s})
	for _, token := range []string{"", "ops", "billing"} {
		r := httptest.NewRequest(http.MethodPost, LifecyclePath+"migrate", strings.NewReader(`{"batch":1}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatal(token, w.Code)
		}
	}
	for _, body := range []string{`{"batch":1,"unexpected":true}`, `{"batch":1}{}`} {
		r := httptest.NewRequest(http.MethodPost, LifecyclePath+"migrate", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer control")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest(http.MethodPost, LifecyclePath+"migrate", strings.NewReader(`{"batch":1}`))
	r.Header.Set("Authorization", "Bearer control")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}
