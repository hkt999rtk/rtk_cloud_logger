package cloudlogger

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
)

type archiveRoundTrip func(*http.Request) (*http.Response, error)

func (f archiveRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestS3ArchiveStoreRequiresCanonicalDedicatedBucket(t *testing.T) {
	for _, tc := range []struct {
		name, environment, region string
		valid                     bool
	}{
		{"dev", "dev", "us-sea", true},
		{"staging", "staging", "sg-sin-2", true},
		{"prod", "prod", "us-iad", true},
		{"other logical environment", "integration", "us-sea", true},
		{"shared scope", "shared", "us-sea", false},
		{"empty scope", "", "us-sea", false},
		{"uppercase scope", "Dev", "us-sea", false},
		{"underscore scope", "dev_test", "us-sea", false},
		{"dot scope", "dev.test", "us-sea", false},
		{"leading scope hyphen", "-dev", "us-sea", false},
		{"trailing scope hyphen", "dev-", "us-sea", false},
		{"repeated scope hyphen", "dev--test", "us-sea", false},
		{"empty region", "dev", "", false},
		{"uppercase region", "dev", "US-sea", false},
		{"underscore region", "dev", "us_sea", false},
		{"dot region", "dev", "us.sea", false},
		{"leading region hyphen", "dev", "-us-sea", false},
		{"trailing region hyphen", "dev", "us-sea-", false},
		{"repeated region hyphen", "dev", "us--sea", false},
		{"63 character bucket", "dev", strings.Repeat("a", 34), true},
		{"64 character bucket", "dev", strings.Repeat("a", 35), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := S3ArchiveConfig{Endpoint: "https://objects.example", Environment: tc.environment, Region: tc.region, SigningRegion: "us-east-1", Bucket: "rtk-cloud-" + tc.environment + "-billing-backup-" + tc.region}
			_, err := NewS3ArchiveStore(cfg, "writer-access", "writer-secret")
			if (err == nil) != tc.valid {
				t.Fatalf("bucket %q: valid=%v, error=%v", cfg.Bucket, tc.valid, err)
			}
		})
	}
}

func TestS3CreateOnlySignatureAndFullDigestReconciliation(t *testing.T) {
	store, err := NewS3ArchiveStore(S3ArchiveConfig{Endpoint: "https://objects.example", Region: "us-sea", SigningRegion: "us-east-1", Environment: "staging", Bucket: "rtk-cloud-staging-billing-backup-us-sea"}, "writer-access", "writer-secret")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("sealed immutable bytes")
	file := filepath.Join(privateBillingDir(t), "sealed.age")
	if err := os.WriteFile(file, body, 0600); err != nil {
		t.Fatal(err)
	}
	sha := billingarchive.Digest(body)
	calls := 0
	remote := bytes.Clone(body)
	store.client = &http.Client{Transport: archiveRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		if !strings.Contains(r.Header.Get("Authorization"), "/us-east-1/s3/aws4_request") {
			t.Error("signing region used bucket region", r.Header.Get("Authorization"))
		}
		if !strings.HasPrefix(r.URL.Path, "/rtk-cloud-staging-billing-backup-us-sea/") {
			t.Error("bucket region changed")
		}
		if r.Method == http.MethodPut {
			if r.Header.Get("If-None-Match") != "*" || r.Header.Get("X-Amz-Content-Sha256") != sha {
				t.Error("unconditional or unsigned PUT")
			}
			return &http.Response{StatusCode: 412, Body: io.NopCloser(strings.NewReader("exists")), Header: http.Header{}}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(remote)), Header: http.Header{"Etag": []string{"untrusted-etag"}}}, nil
	})}
	if err := store.PutImmutable(context.Background(), "billing-raw/rtk/store/2026/10/03/set/part.age", file, int64(len(body)), sha); err != nil || calls != 2 {
		t.Fatal(calls, err)
	}
	remote[0] ^= 1
	if err := store.PutImmutable(context.Background(), "billing-raw/rtk/store/2026/10/03/set/part.age", file, int64(len(body)), sha); err == nil {
		t.Fatal("reconciled immutable collision")
	}
	if _, err := NewS3ArchiveStore(S3ArchiveConfig{Endpoint: "https://objects.example", Region: "us-sea", Environment: "prod", Bucket: "rtk-cloud-staging-billing-backup-us-sea"}, "writer-access", "writer-secret"); err == nil {
		t.Fatal("cross-environment bucket allowed")
	}
}
