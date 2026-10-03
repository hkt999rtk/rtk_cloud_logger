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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
)

func TestRecoveryFenceSurvivesRestartAndIndependentAdmission(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	ctx := context.Background()
	migrateAll(t, s)
	if err := s.InsertEvent(ctx, canonicalBillingEvent("before")); err != nil {
		t.Fatal(err)
	}
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	config := s.lifecycle
	config.RecoveryKeys = map[string]string{"recover-v1": base64.StdEncoding.EncodeToString(pub)}
	if err := s.ConfigureLifecycle(config); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRecoveryMode(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertEvent(ctx, canonicalBillingEvent("blocked")); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal("restored copy admitted writes", err)
	}
	anchor := s.anchor
	s.Close()
	restored, err := OpenBillingInbox(anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.ConfigureLifecycle(config); err != nil {
		t.Fatal(err)
	}
	if err := restored.Health(ctx); !errors.Is(err, ErrBillingUnavailable) {
		t.Fatal("recovery flag clear bypassed persisted fence", err)
	}
	st, err := restored.RecoveryState(ctx)
	if err != nil || !st.Fenced {
		t.Fatal(st, err)
	}
	now := time.Now().UTC()
	approval := billingarchive.RecoveryApproval{Version: 1, Purpose: "billing-inbox-recovery-admission", RecoveryKeyID: "recover-v1", Environment: st.Environment, StoreID: st.StoreID, HighWater: st.HighWater, ArchiveFloor: st.ArchiveFloor, OperationHistorySHA256: st.OperationHistorySHA256, AllocationFrontier: st.HighWater, ConsumerCheckpointSHA256: strings.Repeat("a", 64), ArchiveDependencySHA256: st.ArchiveDependencySHA256, FinancialApprovalRef: "financial-approval", RecoveryApprovalRef: "recovery-approval", ApprovedAt: now, ExpiresAt: now.Add(5 * time.Minute)}
	wrong := approval
	wrong.StoreID = strings.Repeat("f", 32)
	signed, _ := billingarchive.SignRecoveryApproval(wrong, key)
	if _, err := restored.AdmitRecovery(ctx, signed); !errors.Is(err, ErrLifecycleConflict) {
		t.Fatal("admitted different snapshot", err)
	}
	signed, _ = billingarchive.SignRecoveryApproval(approval, key)
	admitted, err := restored.AdmitRecovery(ctx, signed)
	if err != nil || admitted.Fenced {
		t.Fatal(admitted, err)
	}
	if err := restored.InsertEvent(ctx, canonicalBillingEvent("after")); err != nil {
		t.Fatal(err)
	}
	page, err := restored.Page(ctx, "", 10)
	if err != nil || page.HighWater != 2 {
		t.Fatal(page, err)
	}
	badConfig := config
	badConfig.RecoveryKeys = map[string]string{"recover-v1": config.VerifierKeys["verifier-v1"]}
	if err := restored.ConfigureLifecycle(badConfig); err == nil {
		t.Fatal("verification-only key granted recovery authority")
	}
}

func TestAuthorityRequiresExactArchiveSetAndPrivateOrigin(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	st := RetirementStatus{OperationID: "operation", Environment: "staging", StoreID: strings.Repeat("a", 32), FromSequence: 1, ThroughSequence: 1, PlanSHA256: strings.Repeat("b", 64), SetID: "set"}
	response := map[string]any{"operation_id": st.OperationID, "environment": st.Environment, "store_id": st.StoreID, "from_sequence": "1", "through_sequence": "1", "plan_sha256": st.PlanSHA256, "set_id": "wrong-set", "status": "ACTIVE"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer isolated-authority" {
			t.Error("authority token missing")
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	s.lifecycle.authority = nil
	s.lifecycle.AuthorityURL = server.URL
	s.lifecycle.AuthorityToken = "isolated-authority"
	if err := s.validateAuthority(context.Background(), st, "ACTIVE"); !errors.Is(err, ErrLifecycleConflict) {
		t.Fatal("accepted wrong archive set", err)
	}
	response["set_id"] = "set"
	if err := s.validateAuthority(context.Background(), st, "ACTIVE"); err != nil {
		t.Fatal(err)
	}
	delete(response, "set_id")
	if err := s.validateAuthority(context.Background(), st, "ACTIVE"); !errors.Is(err, ErrLifecycleConflict) {
		t.Fatal("accepted missing archive set", err)
	}
	for _, origin := range []string{"http://external.example", "http://billing.ns.svc.cluster.local.attacker.example", "https://user:password@example.com", "https://example.com/v1?x=1"} {
		cfg := s.lifecycle
		cfg.AuthorityURL = origin
		if err := s.ConfigureLifecycle(cfg); err == nil {
			t.Fatal("unsafe authority origin", origin)
		}
	}
	cfg := s.lifecycle
	cfg.AuthorityURL = "http://billing.namespace.svc.cluster.local:8081"
	if err := s.ConfigureLifecycle(cfg); err != nil {
		t.Fatal("private qualified K8s origin rejected", err)
	}
}

func TestConcurrentGenerationCompactionPreservesJournalledWrites(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	ctx := context.Background()
	migrateAll(t, s)
	for i := 0; i < 150; i++ {
		if err := s.InsertEvent(ctx, canonicalBillingEvent(fmt.Sprintf("initial-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errorsChannel := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 80; i++ {
			if err := s.InsertEvent(ctx, canonicalBillingEvent(fmt.Sprintf("concurrent-%d", i))); err != nil {
				errorsChannel <- err
				return
			}
			if err := s.update(func(t *mutationTx) error {
				if err := t.put("meta", []byte("journal-test"), []byte(fmt.Sprint(i))); err != nil {
					return err
				}
				return t.del("meta", []byte("temporary-test"))
			}); err != nil {
				errorsChannel <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	result, err := s.CompactOnline(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Fatal(err)
	}
	page, err := s.Page(ctx, "", 1000)
	if err != nil || len(page.Records) != 230 || page.HighWater != 230 {
		t.Fatal(len(page.Records), page.HighWater, err)
	}
	s.db.View(func(tx *bolt.Tx) error {
		if string(tx.Bucket([]byte("meta")).Get([]byte("journal-test"))) != "79" {
			t.Fatal("journal mutation lost")
		}
		return nil
	})
	if !result.QueueTargetMet {
		t.Fatal(result)
	}
}

func TestPublicListenerRejectsEveryLifecycleRouteAndReadTokenCannotMutate(t *testing.T) {
	s, _, _, _ := lifecycleInbox(t)
	cfg := IngestConfig{Token: "ops", BillingToken: "billing", LifecycleToken: "control", LifecycleReadToken: "read", BillingInbox: s}
	public := IngestHandler(NewMemoryEventStore(), cfg)
	private := LifecycleHandler(cfg)
	for _, route := range []string{"migrate", "capture", "retire/abort", "status", "recovery"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			r := httptest.NewRequest(method, LifecyclePath+route, strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer control")
			w := httptest.NewRecorder()
			public.ServeHTTP(w, r)
			if w.Code != 404 {
				t.Fatal("public lifecycle exposure", route, w.Code)
			}
			r = httptest.NewRequest(method, LifecyclePath+route, strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer read")
			w = httptest.NewRecorder()
			private.ServeHTTP(w, r)
			if w.Code != 401 {
				t.Fatal("read credential crossed scope", route, w.Code)
			}
		}
	}
	r := httptest.NewRequest(http.MethodGet, LifecyclePath+"retire/unknown", nil)
	r.Header.Set("Authorization", "Bearer read")
	w := httptest.NewRecorder()
	private.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatal("terminal read not routed", w.Code)
	}
}

func TestRetirementTerminalDigestGolden(t *testing.T) {
	st := RetirementStatus{OperationID: "operation", Environment: "dev", StoreID: strings.Repeat("a", 32), FromSequence: 1, ThroughSequence: 1, PlanSHA256: "d97b684a97aad4e9b1ef6f7ba57e83436bb39e850a10cbac79385db0d1e76ebb", Status: "completed", CompletedAt: time.Date(2026, 10, 3, 1, 2, 3, 123456000, time.UTC), RetiredThrough: 1, SetID: "set"}
	sealStatus(&st)
	if st.ReceiptSHA256 != "131cca1d26463c3412d032b12e8041e1e2286aaff53dc7a84498af428e377fe6" {
		b, _ := json.Marshal(st)
		t.Fatal(st.ReceiptSHA256, string(b))
	}
	if bytes.Contains([]byte(st.ReceiptSHA256), []byte(" ")) {
		t.Fatal("invalid digest")
	}
}
