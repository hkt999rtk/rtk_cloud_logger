package billingarchive

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const RangeSignatureDomain = "rtk-billing-backup-range-verification-v1\x00"

// RangeProof authorizes one bounded consumer reconciliation subset of an
// independently verified immutable archive. It is not a retirement permit.
type RangeProof struct {
	Version               int       `json:"version"`
	Purpose               string    `json:"purpose"`
	VerifierKeyID         string    `json:"verifier_key_id"`
	Environment           string    `json:"environment"`
	Stack                 string    `json:"stack"`
	StoreID               string    `json:"store_id"`
	SetID                 string    `json:"set_id"`
	ManifestSHA256        string    `json:"manifest_sha256"`
	HighWater             uint64    `json:"high_water,string"`
	FromSequence          uint64    `json:"from_sequence,string"`
	ThroughSequence       uint64    `json:"through_sequence,string"`
	RecordCount           uint64    `json:"record_count,string"`
	MaxReceivedAt         time.Time `json:"max_received_at"`
	RecordsBindingsSHA256 string    `json:"records_bindings_sha256"`
	VerifiedAt            time.Time `json:"verified_at"`
	PolicyVersion         string    `json:"policy_version"`
	VerifierVersion       string    `json:"verifier_version"`
}

func SignRangeProof(p RangeProof, key ed25519.PrivateKey) (SignedCompletion, error) {
	if err := p.Validate(); err != nil {
		return SignedCompletion{}, err
	}
	if len(key) != ed25519.PrivateKeySize {
		return SignedCompletion{}, errors.New("invalid signing key")
	}
	b, err := json.Marshal(p)
	if err != nil {
		return SignedCompletion{}, err
	}
	sig := ed25519.Sign(key, append([]byte(RangeSignatureDomain), b...))
	return SignedCompletion{p.VerifierKeyID, base64.StdEncoding.EncodeToString(b), base64.StdEncoding.EncodeToString(sig)}, nil
}
func (p RangeProof) Validate() error {
	if p.Version != Version || p.Purpose != "billing-raw-reconciliation" || !SafeID(p.VerifierKeyID) || !SafeID(p.Environment) || !SafeID(p.Stack) || !SafeID(p.SetID) || len(p.StoreID) != 32 || !validSHA(p.ManifestSHA256) || !validSHA(p.RecordsBindingsSHA256) || p.FromSequence == 0 || p.ThroughSequence < p.FromSequence || p.ThroughSequence > p.HighWater || p.RecordCount != p.ThroughSequence-p.FromSequence+1 || p.RecordCount > 1000 || p.MaxReceivedAt.IsZero() || p.VerifiedAt.IsZero() || p.MaxReceivedAt.After(p.VerifiedAt) || p.PolicyVersion == "" || p.VerifierVersion == "" {
		return errors.New("invalid reconciliation range proof")
	}
	return nil
}
func VerifyRangeProof(c SignedCompletion, keys map[string]ed25519.PublicKey) (RangeProof, error) {
	var p RangeProof
	key := keys[c.VerifierKeyID]
	b, err := base64.StdEncoding.DecodeString(c.PayloadB64)
	if err != nil || len(b) > 16384 || len(key) != ed25519.PublicKeySize {
		return p, errors.New("untrusted range proof key")
	}
	sig, err := base64.StdEncoding.DecodeString(c.SignatureB64)
	if err != nil || !ed25519.Verify(key, append([]byte(RangeSignatureDomain), b...), sig) {
		return p, errors.New("invalid range proof signature")
	}
	if err = StrictDecode(strings.NewReader(string(b)), 16384, &p); err != nil {
		return p, err
	}
	if p.VerifierKeyID != c.VerifierKeyID {
		return p, errors.New("range proof key mismatch")
	}
	return p, p.Validate()
}
