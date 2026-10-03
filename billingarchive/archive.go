// Package billingarchive is the versioned, bounded Billing backup codec shared
// by the public-only Logger writer and the independently trusted verifier.
package billingarchive

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/klauspost/compress/zstd"
)

const Version = 1
const DefaultPartBytes int64 = 256 << 20
const MaxCipherPartBytes int64 = 272 << 20
const MaxSetBytes int64 = 64 << 30
const MaxParts = 1024
const MaxManifestBytes = 4 << 20
const SignatureDomain = "rtk-billing-backup-verification-v1\x00"

type Part struct {
	Name         string `json:"name"`
	Kind         string `json:"kind"`
	Ordinal      int    `json:"ordinal"`
	Offset       int64  `json:"offset,string"`
	PlainBytes   int64  `json:"plain_bytes,string"`
	PlainSHA256  string `json:"plain_sha256"`
	CipherBytes  int64  `json:"cipher_bytes,string"`
	CipherSHA256 string `json:"cipher_sha256"`
}

type Manifest struct {
	Version                 int       `json:"version"`
	Environment             string    `json:"environment"`
	SourceEventEnvironments []string  `json:"source_event_environments"`
	Stack                   string    `json:"stack"`
	StoreID                 string    `json:"store_id"`
	SetID                   string    `json:"set_id"`
	CreatedAt               time.Time `json:"created_at"`
	HighWater               uint64    `json:"high_water,string"`
	FromSequence            uint64    `json:"from_sequence,string"`
	ThroughSequence         uint64    `json:"through_sequence,string"`
	RecordCount             uint64    `json:"record_count,string"`
	MaxReceivedAt           time.Time `json:"max_received_at"`
	RecordsBindingsSHA256   string    `json:"records_bindings_sha256"`
	SnapshotBytes           int64     `json:"snapshot_bytes,string"`
	SnapshotSHA256          string    `json:"snapshot_sha256"`
	EncryptionKeyID         string    `json:"encryption_key_id"`
	RecipientFingerprints   []string  `json:"recipient_fingerprints"`
	Parts                   []Part    `json:"parts"`
}

// ExportRecord leaves the collection HTTP BillingRecord shape unchanged.
type ExportRecord struct {
	Sequence      uint64          `json:"sequence,string"`
	ContentSHA256 string          `json:"content_sha256"`
	ReceivedAt    time.Time       `json:"received_at"`
	Record        json.RawMessage `json:"record"`
}

type VerificationPayload struct {
	Version               int       `json:"version"`
	Verdict               string    `json:"verdict"`
	VerifierKeyID         string    `json:"verifier_key_id"`
	Environment           string    `json:"environment"`
	Stack                 string    `json:"stack"`
	StoreID               string    `json:"store_id"`
	SetID                 string    `json:"set_id"`
	ManifestSHA256        string    `json:"manifest_sha256"`
	ObjectListSHA256      string    `json:"object_list_sha256"`
	HighWater             uint64    `json:"high_water,string"`
	FromSequence          uint64    `json:"from_sequence,string"`
	ThroughSequence       uint64    `json:"through_sequence,string"`
	RecordCount           uint64    `json:"record_count,string"`
	MaxReceivedAt         time.Time `json:"max_received_at"`
	RecordsBindingsSHA256 string    `json:"records_bindings_sha256"`
	EncryptionKeyID       string    `json:"encryption_key_id"`
	RecipientFingerprints []string  `json:"recipient_fingerprints"`
	VerifiedAt            time.Time `json:"verified_at"`
	PolicyVersion         string    `json:"policy_version"`
	VerifierVersion       string    `json:"verifier_version"`
}

type SignedCompletion struct {
	VerifierKeyID string `json:"verifier_key_id"`
	PayloadB64    string `json:"payload_b64"`
	SignatureB64  string `json:"signature_b64"`
}

// RecordBinding is the exact ordered consumer reconciliation proof input.
type RecordBinding struct {
	Sequence            uint64 `json:"sequence,string"`
	LoggerContentSHA256 string `json:"logger_content_sha256"`
	UsageID             string `json:"usage_id"`
	EventSHA256         string `json:"event_sha256"`
}

func BindingsDigest(refs []RecordBinding) string { b, _ := json.Marshal(refs); return Digest(b) }

func Digest(b []byte) string              { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func Fingerprint(recipient string) string { return Digest([]byte(recipient)) }
func validSHA(v string) bool {
	b, err := hex.DecodeString(v)
	return err == nil && len(b) == 32 && v == strings.ToLower(v)
}
func SafeID(v string) bool {
	if v == "" || len(v) > 128 {
		return false
	}
	for _, c := range v {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return v != "." && v != ".."
}

func (m Manifest) Validate() error {
	if len(m.SourceEventEnvironments) < 1 || len(m.SourceEventEnvironments) > 8 {
		return errors.New("explicit source event environments required")
	}
	seenEnv := map[string]bool{}
	for _, env := range m.SourceEventEnvironments {
		if !SafeID(env) || seenEnv[env] {
			return errors.New("invalid source event environment allowlist")
		}
		seenEnv[env] = true
	}
	if m.Version != Version || !SafeID(m.Environment) || !SafeID(m.Stack) || !SafeID(m.SetID) || !SafeID(m.EncryptionKeyID) || len(m.StoreID) != 32 || !validSHA(m.SnapshotSHA256) || !validSHA(m.RecordsBindingsSHA256) || m.SnapshotBytes < 1 || m.SnapshotBytes > MaxSetBytes || m.CreatedAt.IsZero() || len(m.Parts) < 1 || len(m.Parts) > MaxParts {
		return errors.New("invalid billing manifest identity or bounds")
	}
	if b, err := hex.DecodeString(m.StoreID); err != nil || len(b) != 16 || m.StoreID != strings.ToLower(m.StoreID) {
		return errors.New("invalid store identity")
	}
	if m.RecordCount == 0 {
		if m.FromSequence != 0 || m.ThroughSequence != 0 || !m.MaxReceivedAt.IsZero() {
			return errors.New("invalid empty range")
		}
	} else if m.FromSequence == 0 || m.ThroughSequence < m.FromSequence || m.ThroughSequence > m.HighWater || m.ThroughSequence-m.FromSequence+1 != m.RecordCount || m.MaxReceivedAt.IsZero() || m.MaxReceivedAt.After(m.CreatedAt) {
		return errors.New("invalid receipt range")
	}
	if len(m.RecipientFingerprints) == 0 || len(m.RecipientFingerprints) > 16 {
		return errors.New("recipient fingerprints required")
	}
	for _, v := range m.RecipientFingerprints {
		if !validSHA(v) {
			return errors.New("invalid recipient fingerprint")
		}
	}
	seen := map[string]bool{}
	offsets := map[string]int64{}
	ordinals := map[string]int{}
	var total int64
	for _, p := range m.Parts {
		if (p.Kind != "snapshot" && p.Kind != "events") || p.Name != path.Base(p.Name) || !SafeID(p.Name) || seen[p.Name] || p.Ordinal != ordinals[p.Kind]+1 || p.Offset != offsets[p.Kind] || p.PlainBytes < 1 || p.PlainBytes > DefaultPartBytes || p.CipherBytes < 1 || p.CipherBytes > MaxCipherPartBytes || !validSHA(p.PlainSHA256) || !validSHA(p.CipherSHA256) || total > MaxSetBytes-p.PlainBytes {
			return errors.New("invalid, unordered or oversized archive part")
		}
		seen[p.Name] = true
		ordinals[p.Kind]++
		offsets[p.Kind] += p.PlainBytes
		total += p.PlainBytes
	}
	if offsets["snapshot"] != m.SnapshotBytes || (m.RecordCount > 0 && ordinals["events"] == 0) || (m.RecordCount == 0 && ordinals["events"] != 0) {
		return errors.New("incomplete snapshot or export")
	}
	return nil
}

func MarshalManifest(m Manifest) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(m)
	if len(b) > MaxManifestBytes {
		return nil, errors.New("manifest exceeds bound")
	}
	return b, err
}
func DecodeManifest(r io.Reader) (Manifest, error) {
	var m Manifest
	err := StrictDecode(r, MaxManifestBytes, &m)
	if err == nil {
		err = m.Validate()
	}
	return m, err
}
func StrictDecode(r io.Reader, limit int64, value any) error {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > limit {
		return errors.New("JSON exceeds bound")
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err = d.Decode(value); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func PayloadFor(m Manifest, manifestBytes []byte, keyID, policy, release string, now time.Time) VerificationPayload {
	b, _ := json.Marshal(m.Parts)
	return VerificationPayload{Version: Version, Verdict: "verified", VerifierKeyID: keyID, Environment: m.Environment, Stack: m.Stack, StoreID: m.StoreID, SetID: m.SetID, ManifestSHA256: Digest(manifestBytes), ObjectListSHA256: Digest(b), HighWater: m.HighWater, FromSequence: m.FromSequence, ThroughSequence: m.ThroughSequence, RecordCount: m.RecordCount, MaxReceivedAt: m.MaxReceivedAt, RecordsBindingsSHA256: m.RecordsBindingsSHA256, EncryptionKeyID: m.EncryptionKeyID, RecipientFingerprints: m.RecipientFingerprints, VerifiedAt: now.UTC(), PolicyVersion: policy, VerifierVersion: release}
}
func SignCompletion(p VerificationPayload, key ed25519.PrivateKey) (SignedCompletion, error) {
	if len(key) != ed25519.PrivateKeySize || !SafeID(p.VerifierKeyID) || p.Version != Version || p.Verdict != "verified" || p.VerifiedAt.IsZero() || p.PolicyVersion == "" || p.VerifierVersion == "" {
		return SignedCompletion{}, errors.New("invalid signing inputs")
	}
	b, err := json.Marshal(p)
	if err != nil {
		return SignedCompletion{}, err
	}
	sig := ed25519.Sign(key, append([]byte(SignatureDomain), b...))
	return SignedCompletion{p.VerifierKeyID, base64.StdEncoding.EncodeToString(b), base64.StdEncoding.EncodeToString(sig)}, nil
}
func VerifyCompletion(c SignedCompletion, m Manifest, manifestBytes []byte, keys map[string]ed25519.PublicKey) (VerificationPayload, error) {
	var p VerificationPayload
	parsed, parseErr := DecodeManifest(strings.NewReader(string(manifestBytes)))
	if parseErr != nil {
		return p, parseErr
	}
	left, _ := json.Marshal(parsed)
	right, _ := json.Marshal(m)
	if string(left) != string(right) {
		return p, errors.New("manifest DTO differs from exact signed bytes")
	}
	key := keys[c.VerifierKeyID]
	b, err := base64.StdEncoding.DecodeString(c.PayloadB64)
	if err != nil || len(b) > 16384 || len(key) != ed25519.PublicKeySize {
		return p, errors.New("untrusted verification key or payload")
	}
	sig, err := base64.StdEncoding.DecodeString(c.SignatureB64)
	if err != nil || !ed25519.Verify(key, append([]byte(SignatureDomain), b...), sig) {
		return p, errors.New("invalid verification signature")
	}
	if err = StrictDecode(strings.NewReader(string(b)), 16384, &p); err != nil {
		return p, err
	}
	want := PayloadFor(m, manifestBytes, c.VerifierKeyID, p.PolicyVersion, p.VerifierVersion, p.VerifiedAt)
	a, _ := json.Marshal(p)
	z, _ := json.Marshal(want)
	if string(a) != string(z) || p.VerifierKeyID != c.VerifierKeyID || p.PolicyVersion == "" || p.VerifierVersion == "" || p.VerifiedAt.IsZero() {
		return p, errors.New("verification scope or manifest mismatch")
	}
	return p, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(b)
}

type limitedWriter struct {
	w         io.Writer
	remaining int64
}

func (w *limitedWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.remaining {
		return 0, errors.New("artifact exceeds bound")
	}
	n, err := w.w.Write(b)
	w.remaining -= int64(n)
	return n, err
}

// EncodePart compresses and encrypts one independently authenticated part.
// Caller owns its exclusive-create file, fsync and immutable publication.
func EncodePart(ctx context.Context, w io.Writer, r io.Reader, recipients []string, plainLimit int64) (int64, string, error) {
	if plainLimit < 1 || plainLimit > DefaultPartBytes {
		return 0, "", errors.New("invalid plaintext bound")
	}
	var rs []age.Recipient
	for _, v := range recipients {
		p, err := age.ParseX25519Recipient(v)
		if err != nil {
			return 0, "", err
		}
		rs = append(rs, p)
	}
	if len(rs) == 0 {
		return 0, "", errors.New("public recipient required")
	}
	a, err := age.Encrypt(&limitedWriter{w, MaxCipherPartBytes}, rs...)
	if err != nil {
		return 0, "", err
	}
	z, err := zstd.NewWriter(a, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(8<<20))
	if err != nil {
		return 0, "", err
	}
	h := sha256.New()
	n, copyErr := io.Copy(z, io.TeeReader(io.LimitReader(contextReader{ctx, r}, plainLimit+1), h))
	if copyErr == nil && n > plainLimit {
		copyErr = errors.New("plaintext exceeds bound")
	}
	closeZ := z.Close()
	closeA := a.Close()
	if copyErr != nil {
		return n, "", copyErr
	}
	if closeZ != nil {
		return n, "", closeZ
	}
	if closeA != nil {
		return n, "", closeA
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// DecodePart authenticates the complete encrypted and compressed stream and
// verifies both hashes. Output is untrusted until the call succeeds.
func DecodePart(ctx context.Context, w io.Writer, r io.Reader, identities []age.Identity, p Part) error {
	if p.CipherBytes < 1 || p.CipherBytes > MaxCipherPartBytes || p.PlainBytes < 1 || p.PlainBytes > DefaultPartBytes {
		return errors.New("invalid part bounds")
	}
	ch := sha256.New()
	count := &countReader{r: io.TeeReader(io.LimitReader(contextReader{ctx, r}, p.CipherBytes+1), ch)}
	a, err := age.Decrypt(count, identities...)
	if err != nil {
		return err
	}
	z, err := zstd.NewReader(a, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20), zstd.WithDecoderMaxWindow(8<<20))
	if err != nil {
		return err
	}
	defer z.Close()
	ph := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, ph), io.LimitReader(z, p.PlainBytes+1))
	if err != nil {
		return fmt.Errorf("plaintext authentication failure: %w", err)
	}
	if n != p.PlainBytes {
		return errors.New("plaintext length mismatch")
	}
	var extra [1]byte
	if k, e := z.Read(extra[:]); k != 0 || e != io.EOF {
		return errors.New("trailing compressed data")
	}
	if k, e := io.Copy(io.Discard, a); k != 0 || e != nil {
		return errors.New("trailing encrypted data or authentication failure")
	}
	if k, e := count.Read(extra[:]); k != 0 || e != io.EOF {
		return errors.New("trailing ciphertext")
	}
	if count.n != p.CipherBytes || hex.EncodeToString(ch.Sum(nil)) != p.CipherSHA256 || hex.EncodeToString(ph.Sum(nil)) != p.PlainSHA256 {
		return errors.New("part hash mismatch")
	}
	return nil
}

type countReader struct {
	r io.Reader
	n int64
}

func (r *countReader) Read(b []byte) (int, error) { n, e := r.r.Read(b); r.n += int64(n); return n, e }
