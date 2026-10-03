package billingarchive

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const RecoverySignatureDomain = "rtk-billing-inbox-recovery-admission-v1\x00"

type RecoveryApproval struct {
	Version                  int       `json:"version"`
	Purpose                  string    `json:"purpose"`
	RecoveryKeyID            string    `json:"recovery_key_id"`
	Environment              string    `json:"environment"`
	StoreID                  string    `json:"store_id"`
	HighWater                uint64    `json:"high_water,string"`
	ArchiveFloor             uint64    `json:"archive_floor,string"`
	OperationHistorySHA256   string    `json:"operation_history_sha256"`
	AllocationFrontier       uint64    `json:"allocation_frontier,string"`
	ConsumerCheckpointSHA256 string    `json:"consumer_checkpoint_sha256"`
	ArchiveDependencySHA256  string    `json:"archive_dependency_sha256"`
	FinancialApprovalRef     string    `json:"financial_approval_ref"`
	RecoveryApprovalRef      string    `json:"recovery_approval_ref"`
	ApprovedAt               time.Time `json:"approved_at"`
	ExpiresAt                time.Time `json:"expires_at"`
}
type SignedRecoveryApproval struct {
	RecoveryKeyID string `json:"recovery_key_id"`
	PayloadB64    string `json:"payload_b64"`
	SignatureB64  string `json:"signature_b64"`
}

func (p RecoveryApproval) Validate(now time.Time) error {
	if p.Version != Version || p.Purpose != "billing-inbox-recovery-admission" || !SafeID(p.RecoveryKeyID) || !SafeID(p.Environment) || len(p.StoreID) != 32 || p.ArchiveFloor > p.HighWater || p.AllocationFrontier != p.HighWater || !validSHA(p.OperationHistorySHA256) || !validSHA(p.ConsumerCheckpointSHA256) || !validSHA(p.ArchiveDependencySHA256) || p.FinancialApprovalRef == "" || len(p.FinancialApprovalRef) > 512 || p.RecoveryApprovalRef == "" || len(p.RecoveryApprovalRef) > 512 || p.ApprovedAt.IsZero() || p.ApprovedAt.After(now.Add(time.Minute)) || !p.ExpiresAt.After(now) || !p.ExpiresAt.After(p.ApprovedAt) || p.ExpiresAt.Sub(p.ApprovedAt) > 15*time.Minute {
		return errors.New("invalid or expired recovery admission")
	}
	return nil
}
func SignRecoveryApproval(p RecoveryApproval, key ed25519.PrivateKey) (SignedRecoveryApproval, error) {
	if err := p.Validate(p.ApprovedAt); err != nil {
		return SignedRecoveryApproval{}, err
	}
	if len(key) != ed25519.PrivateKeySize {
		return SignedRecoveryApproval{}, errors.New("invalid recovery signing key")
	}
	b, err := json.Marshal(p)
	if err != nil {
		return SignedRecoveryApproval{}, err
	}
	sig := ed25519.Sign(key, append([]byte(RecoverySignatureDomain), b...))
	return SignedRecoveryApproval{p.RecoveryKeyID, base64.StdEncoding.EncodeToString(b), base64.StdEncoding.EncodeToString(sig)}, nil
}
func VerifyRecoveryApproval(c SignedRecoveryApproval, keys map[string]ed25519.PublicKey, now time.Time) (RecoveryApproval, error) {
	var p RecoveryApproval
	key := keys[c.RecoveryKeyID]
	b, err := base64.StdEncoding.DecodeString(c.PayloadB64)
	if err != nil || len(b) > 16384 || len(key) != ed25519.PublicKeySize {
		return p, errors.New("untrusted recovery custodian")
	}
	sig, err := base64.StdEncoding.DecodeString(c.SignatureB64)
	if err != nil || !ed25519.Verify(key, append([]byte(RecoverySignatureDomain), b...), sig) {
		return p, errors.New("invalid recovery admission signature")
	}
	if err := StrictDecode(strings.NewReader(string(b)), 16384, &p); err != nil {
		return p, err
	}
	if p.RecoveryKeyID != c.RecoveryKeyID {
		return p, errors.New("recovery key mismatch")
	}
	return p, p.Validate(now)
}
