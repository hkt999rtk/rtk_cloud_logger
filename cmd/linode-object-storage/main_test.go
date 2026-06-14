package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreFromEnvPrefersLinodeCredentials(t *testing.T) {
	t.Setenv("LINODE_OBJ_BUCKET", "release-bucket")
	t.Setenv("LINODE_OBJ_ENDPOINT", "https://us-sea-1.linodeobjects.com")
	t.Setenv("LINODE_OBJ_ACCESS_KEY_ID", "linode-access")
	t.Setenv("LINODE_OBJ_SECRET_ACCESS_KEY", "linode-secret")
	t.Setenv("AWS_ACCESS_KEY_ID", "aws-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")

	store, err := storeFromEnv()
	if err != nil {
		t.Fatalf("storeFromEnv() error = %v", err)
	}
	if store.accessKey != "linode-access" {
		t.Fatalf("accessKey = %q, want linode-access", store.accessKey)
	}
	if store.secretKey != "linode-secret" {
		t.Fatalf("secretKey = %q, want linode-secret", store.secretKey)
	}
	if store.region != "us-sea-1" {
		t.Fatalf("region = %q, want us-sea-1", store.region)
	}
}

func TestPutDownloadCatAndExistsUseSignedPathStyleRequests(t *testing.T) {
	var putBody []byte
	var seenMethods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenMethods = append(seenMethods, r.Method+" "+r.URL.EscapedPath())
		if got := r.Header.Get("Authorization"); !strings.HasPrefix(got, "AWS4-HMAC-SHA256 ") {
			t.Fatalf("Authorization header = %q", got)
		}
		if got := r.Header.Get("X-Amz-Content-Sha256"); got == "" {
			t.Fatal("missing X-Amz-Content-Sha256 header")
		}
		switch r.Method {
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			putBody = data
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write([]byte("downloaded"))
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	store := objectStore{
		bucket:    "bucket-a",
		endpoint:  server.URL,
		accessKey: "access",
		secretKey: "secret",
		region:    "us-sea",
	}
	source := filepath.Join(t.TempDir(), "bundle.tar.gz")
	if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := putObjectFromFile(store, "releases/app-v1/v1.tar.gz", source); err != nil {
		t.Fatalf("putObjectFromFile() error = %v", err)
	}
	if !bytes.Equal(putBody, []byte("payload")) {
		t.Fatalf("uploaded body = %q, want payload", string(putBody))
	}
	var cat bytes.Buffer
	if err := writeObject(store, "releases/app-v1/manifest.json", &cat); err != nil {
		t.Fatalf("writeObject() error = %v", err)
	}
	if cat.String() != "downloaded" {
		t.Fatalf("cat body = %q, want downloaded", cat.String())
	}
	out := filepath.Join(t.TempDir(), "manifest.json")
	if err := downloadObject(store, "releases/app-v1/manifest.json", out); err != nil {
		t.Fatalf("downloadObject() error = %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read download: %v", err)
	}
	if string(data) != "downloaded" {
		t.Fatalf("downloaded body = %q, want downloaded", string(data))
	}
	if err := objectExists(store, "releases/app-v1/manifest.json"); err != nil {
		t.Fatalf("objectExists() error = %v", err)
	}

	want := []string{
		"PUT /bucket-a/releases/app-v1/v1.tar.gz",
		"GET /bucket-a/releases/app-v1/manifest.json",
		"GET /bucket-a/releases/app-v1/manifest.json",
		"HEAD /bucket-a/releases/app-v1/manifest.json",
	}
	if strings.Join(seenMethods, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests = %#v, want %#v", seenMethods, want)
	}
}

func TestObjectExistsReportsServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	store := objectStore{
		bucket:    "bucket-a",
		endpoint:  server.URL,
		accessKey: "access",
		secretKey: "secret",
		region:    "us-sea",
	}
	err := objectExists(store, "blocked")
	if err == nil {
		t.Fatal("objectExists() error = nil, want HTTP error")
	}
	if !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("error = %q, want HTTP 403", err.Error())
	}
}
