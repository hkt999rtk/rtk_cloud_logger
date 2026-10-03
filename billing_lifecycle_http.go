package cloudlogger

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
)

const LifecyclePath = "/v1/internal/billing-lifecycle/"

// LifecycleHandler must be served only on the isolated private listener.
// The public ingest mux intentionally has no lifecycle routes at any token.
func LifecycleHandler(cfg IngestConfig) http.Handler {
	mux := http.NewServeMux()
	registerLifecycleRoutes(mux, cfg)
	return mux
}

func registerLifecycleRoutes(mux *http.ServeMux, cfg IngestConfig) {
	mux.HandleFunc(LifecyclePath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		token := bearerToken(r.Header.Get("Authorization"))
		route := strings.TrimPrefix(r.URL.Path, LifecyclePath)
		terminalID := strings.TrimPrefix(route, "retire/")
		terminalRoute := strings.HasPrefix(route, "retire/") && billingarchive.SafeID(terminalID) && terminalID != "plan" && terminalID != "apply" && terminalID != "abort"
		control := cfg.LifecycleToken != "" && cfg.LifecycleToken != cfg.Token && cfg.LifecycleToken != cfg.BillingToken && subtle.ConstantTimeCompare([]byte(token), []byte(cfg.LifecycleToken)) == 1
		read := r.Method == http.MethodGet && terminalRoute && cfg.LifecycleReadToken != "" && cfg.LifecycleReadToken != cfg.LifecycleToken && cfg.LifecycleReadToken != cfg.Token && cfg.LifecycleReadToken != cfg.BillingToken && subtle.ConstantTimeCompare([]byte(token), []byte(cfg.LifecycleReadToken)) == 1
		if cfg.BillingInbox == nil || (!control && !read) {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.URL.RawQuery != "" {
			http.Error(w, "unsupported lifecycle query", 400)
			return
		}
		var result any
		var err error
		decode := func(v any) bool {
			if err := billingarchive.StrictDecode(http.MaxBytesReader(w, r.Body, 16<<20), 16<<20, v); err != nil {
				http.Error(w, "invalid bounded lifecycle JSON", 400)
				return false
			}
			return true
		}
		if r.Method == http.MethodGet {
			switch {
			case route == "migration":
				result, err = cfg.BillingInbox.Migration(r.Context())
			case route == "captures":
				result, err = cfg.BillingInbox.Captures(r.Context())
			case route == "status":
				result = cfg.BillingInbox.Worker(r.Context())
			case route == "recovery":
				result, err = cfg.BillingInbox.RecoveryState(r.Context())
			case strings.HasPrefix(route, "retire/"):
				id := strings.TrimPrefix(route, "retire/")
				result, err = cfg.BillingInbox.Retirement(r.Context(), id)
			default:
				http.NotFound(w, r)
				return
			}
		} else if r.Method == http.MethodPost {
			switch route {
			case "migrate":
				var request struct {
					Batch int `json:"batch"`
				}
				if !decode(&request) {
					return
				}
				if request.Batch == 0 {
					request.Batch = 1000
				}
				result, err = cfg.BillingInbox.Migrate(r.Context(), request.Batch)
			case "capture":
				var request struct{}
				if !decode(&request) {
					return
				}
				result, err = cfg.BillingInbox.Capture(r.Context())
			case "verify":
				var request struct {
					SetID      string                          `json:"set_id"`
					Completion billingarchive.SignedCompletion `json:"completion"`
				}
				if !decode(&request) {
					return
				}
				result, err = cfg.BillingInbox.VerifyCatalog(r.Context(), request.SetID, request.Completion)
			case "retire/plan":
				var request RetirementPlan
				if !decode(&request) {
					return
				}
				result, err = cfg.BillingInbox.PlanRetirement(r.Context(), request)
			case "retire/apply":
				var request struct {
					OperationID string `json:"operation_id"`
				}
				if !decode(&request) {
					return
				}
				result, err = cfg.BillingInbox.ApplyRetirement(r.Context(), request.OperationID)
			case "retire/abort":
				var request RetirementPlan
				if !decode(&request) {
					return
				}
				result, err = cfg.BillingInbox.AbortRetirementPlan(r.Context(), request)
			case "rehydrate":
				var request struct {
					SetID   string                        `json:"set_id"`
					Records []billingarchive.ExportRecord `json:"records"`
				}
				if !decode(&request) {
					return
				}
				err = cfg.BillingInbox.Rehydrate(r.Context(), request.SetID, request.Records)
				result = struct {
					Status string `json:"status"`
				}{"rehydrated"}
			case "cache/evict":
				var request struct {
					ThroughSequence uint64 `json:"through_sequence,string"`
				}
				if !decode(&request) {
					return
				}
				err = cfg.BillingInbox.EvictCache(r.Context(), request.ThroughSequence)
				result = struct {
					Status string `json:"status"`
				}{"evicted"}
			case "compact":
				var request struct{}
				if !decode(&request) {
					return
				}
				result, err = cfg.BillingInbox.CompactOnline(r.Context())
			case "recovery/admit":
				var request billingarchive.SignedRecoveryApproval
				if !decode(&request) {
					return
				}
				result, err = cfg.BillingInbox.AdmitRecovery(r.Context(), request)
			default:
				http.NotFound(w, r)
				return
			}
		} else {
			http.Error(w, "method not allowed", 405)
			return
		}
		if err != nil {
			switch {
			case errors.Is(err, ErrOperationNotFound):
				http.Error(w, "unknown retirement operation", 404)
			case errors.Is(err, ErrLifecycleConflict):
				http.Error(w, "lifecycle precondition failed", 409)
			default:
				http.Error(w, "lifecycle operation unavailable", 503)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
}
