package billingarchive

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
)

func TestAuthenticatedPartRejectsTamperTruncationTrailingAndBounds(t *testing.T) {
	identity, _ := age.GenerateX25519Identity()
	plain := bytes.Repeat([]byte("billing-precision-9007199254740993\n"), 1000)
	var cipher bytes.Buffer
	n, sha, err := EncodePart(context.Background(), &cipher, bytes.NewReader(plain), []string{identity.Recipient().String()}, DefaultPartBytes)
	if err != nil {
		t.Fatal(err)
	}
	p := Part{Name: "events-000001.zst.age", Kind: "events", Ordinal: 1, PlainBytes: n, PlainSHA256: sha, CipherBytes: int64(cipher.Len()), CipherSHA256: Digest(cipher.Bytes())}
	var out bytes.Buffer
	if err := DecodePart(context.Background(), &out, bytes.NewReader(cipher.Bytes()), []age.Identity{identity}, p); err != nil || !bytes.Equal(out.Bytes(), plain) {
		t.Fatal(err)
	}
	for name, input := range map[string][]byte{"truncated": cipher.Bytes()[:cipher.Len()-1], "trailing": append(bytes.Clone(cipher.Bytes()), 1), "tampered": bytes.Clone(cipher.Bytes())} {
		if name == "tampered" {
			input[len(input)-5] ^= 1
		}
		if err := DecodePart(context.Background(), new(bytes.Buffer), bytes.NewReader(input), []age.Identity{identity}, p); err == nil {
			t.Fatal("accepted", name)
		}
	}
	bad := p
	bad.PlainBytes++
	if err := DecodePart(context.Background(), new(bytes.Buffer), bytes.NewReader(cipher.Bytes()), []age.Identity{identity}, bad); err == nil {
		t.Fatal("wrong plaintext bound")
	}
	if _, _, err := EncodePart(context.Background(), new(bytes.Buffer), bytes.NewReader(plain), []string{identity.String()}, DefaultPartBytes); err == nil {
		t.Fatal("writer accepted private identity")
	}
	if _, _, err := EncodePart(context.Background(), new(bytes.Buffer), bytes.NewReader(plain), []string{identity.Recipient().String()}, 1); err == nil {
		t.Fatal("plaintext limit bypass")
	}
}
func fixtureManifest() Manifest {
	return Manifest{Version: Version, Environment: "staging", Stack: "rtk", StoreID: strings.Repeat("a", 32), SetID: "set-1", CreatedAt: time.Now().UTC(), HighWater: 9007199254740993, FromSequence: 9007199254740993, ThroughSequence: 9007199254740993, RecordCount: 1, MaxReceivedAt: time.Now().UTC().Add(-time.Hour), RecordsBindingsSHA256: Digest([]byte("[]")), SnapshotBytes: 8, SnapshotSHA256: Digest([]byte("snapshot")), EncryptionKeyID: "key-1", RecipientFingerprints: []string{Digest([]byte("public"))}, Parts: []Part{{Name: "snapshot-000001.zst.age", Kind: "snapshot", Ordinal: 1, PlainBytes: 8, PlainSHA256: Digest([]byte("snapshot")), CipherBytes: 20, CipherSHA256: Digest([]byte("snapshotcipher"))}, {Name: "events-000001.zst.age", Kind: "events", Ordinal: 1, PlainBytes: 4, PlainSHA256: Digest([]byte("raw\n")), CipherBytes: 20, CipherSHA256: Digest([]byte("eventcipher"))}}}
}
func TestCompletionAndRangeProofBindExactScopeAndDomains(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	m := fixtureManifest()
	m.SourceEventEnvironments = []string{"staging"}
	b, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	p := PayloadFor(m, b, "verify-1", "1", "test", time.Now())
	signed, err := SignCompletion(p, key)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"verify-1": pub}
	if _, err := VerifyCompletion(signed, m, b, keys); err != nil {
		t.Fatal(err)
	}
	changed := m
	changed.SetID = "other"
	if _, err := VerifyCompletion(signed, changed, b, keys); err == nil {
		t.Fatal("accepted mismatched DTO")
	}
	if _, err := VerifyCompletion(signed, m, append(bytes.Clone(b), ' '), keys); err == nil {
		t.Fatal("manifest exact bytes not bound")
	}
	rangeProof := RangeProof{Version: Version, Purpose: "billing-raw-reconciliation", VerifierKeyID: "verify-1", Environment: m.Environment, Stack: m.Stack, StoreID: m.StoreID, SetID: m.SetID, ManifestSHA256: Digest(b), HighWater: m.HighWater, FromSequence: m.FromSequence, ThroughSequence: m.ThroughSequence, RecordCount: 1, MaxReceivedAt: m.MaxReceivedAt, RecordsBindingsSHA256: m.RecordsBindingsSHA256, VerifiedAt: time.Now().UTC(), PolicyVersion: "1", VerifierVersion: "test"}
	rangeSigned, err := SignRangeProof(rangeProof, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyRangeProof(rangeSigned, keys); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyRangeProof(signed, keys); err == nil {
		t.Fatal("completion substituted for range proof")
	}
	if _, err := VerifyCompletion(rangeSigned, m, b, keys); err == nil {
		t.Fatal("range proof substituted for completion")
	}
	if !bytes.Contains(b, []byte(`"9007199254740993"`)) {
		t.Fatal("large sequence lost precision")
	}
}

func TestSnapshotLargerThanFourGiBHasOnlyBoundedDescriptors(t *testing.T) {
	m := fixtureManifest()
	m.SourceEventEnvironments = []string{"staging"}
	m.HighWater = 0
	m.FromSequence = 0
	m.ThroughSequence = 0
	m.RecordCount = 0
	m.MaxReceivedAt = time.Time{}
	m.SnapshotBytes = 5 << 30
	m.Parts = nil
	for ordinal, offset := 1, int64(0); offset < m.SnapshotBytes; ordinal, offset = ordinal+1, offset+DefaultPartBytes {
		m.Parts = append(m.Parts, Part{Name: fmt.Sprintf("snapshot-%06d.zst.age", ordinal), Kind: "snapshot", Ordinal: ordinal, Offset: offset, PlainBytes: DefaultPartBytes, PlainSHA256: Digest([]byte("plain")), CipherBytes: MaxCipherPartBytes, CipherSHA256: Digest([]byte("cipher"))})
	}
	raw, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifest(bytes.NewReader(raw))
	if err != nil || decoded.SnapshotBytes != 5<<30 || len(decoded.Parts) != 20 {
		t.Fatal(err)
	}
	for _, part := range decoded.Parts {
		if part.CipherBytes > MaxCipherPartBytes || part.PlainBytes > DefaultPartBytes {
			t.Fatal("unbounded descriptor")
		}
	}
}
