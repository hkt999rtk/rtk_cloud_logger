package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type objectStore struct {
	bucket    string
	endpoint  string
	accessKey string
	secretKey string
	region    string
}

func main() {
	if len(os.Args) < 2 {
		die("usage: linode-object-storage <put|download|cat|exists> [options]")
	}
	store, err := storeFromEnv()
	if err != nil {
		die(err.Error())
	}
	switch os.Args[1] {
	case "put":
		fs := flag.NewFlagSet("put", flag.ExitOnError)
		file := fs.String("file", "", "source file")
		key := fs.String("key", "", "object key")
		_ = fs.Parse(os.Args[2:])
		if *file == "" || *key == "" {
			die("--file and --key are required")
		}
		if err := putObjectFromFile(store, *key, *file); err != nil {
			die(err.Error())
		}
	case "download":
		fs := flag.NewFlagSet("download", flag.ExitOnError)
		key := fs.String("key", "", "object key")
		out := fs.String("out", "", "output path")
		_ = fs.Parse(os.Args[2:])
		if *key == "" || *out == "" {
			die("--key and --out are required")
		}
		if err := downloadObject(store, *key, *out); err != nil {
			die(err.Error())
		}
	case "cat":
		fs := flag.NewFlagSet("cat", flag.ExitOnError)
		key := fs.String("key", "", "object key")
		_ = fs.Parse(os.Args[2:])
		if *key == "" {
			die("--key is required")
		}
		if err := writeObject(store, *key, os.Stdout); err != nil {
			die(err.Error())
		}
	case "exists":
		fs := flag.NewFlagSet("exists", flag.ExitOnError)
		key := fs.String("key", "", "object key")
		_ = fs.Parse(os.Args[2:])
		if *key == "" {
			die("--key is required")
		}
		if err := objectExists(store, *key); err != nil {
			die(err.Error())
		}
	default:
		die("unknown command: " + os.Args[1])
	}
}

func die(message string) {
	fmt.Fprintln(os.Stderr, "error: "+message)
	os.Exit(1)
}

func storeFromEnv() (objectStore, error) {
	store := objectStore{
		bucket:    strings.TrimSpace(os.Getenv("LINODE_OBJ_BUCKET")),
		endpoint:  strings.TrimRight(strings.TrimSpace(os.Getenv("LINODE_OBJ_ENDPOINT")), "/"),
		accessKey: firstNonEmpty(os.Getenv("LINODE_OBJ_ACCESS_KEY_ID"), os.Getenv("AWS_ACCESS_KEY_ID")),
		secretKey: firstNonEmpty(os.Getenv("LINODE_OBJ_SECRET_ACCESS_KEY"), os.Getenv("AWS_SECRET_ACCESS_KEY")),
		region:    strings.TrimSpace(os.Getenv("LINODE_OBJ_REGION")),
	}
	if store.bucket == "" {
		return store, errors.New("LINODE_OBJ_BUCKET is required")
	}
	if store.endpoint == "" {
		return store, errors.New("LINODE_OBJ_ENDPOINT is required")
	}
	if store.accessKey == "" || store.secretKey == "" {
		return store, errors.New("LINODE_OBJ_ACCESS_KEY_ID and LINODE_OBJ_SECRET_ACCESS_KEY are required")
	}
	if store.region == "" {
		store.region = regionFromEndpoint(store.endpoint)
	}
	return store, nil
}

func putObjectFromFile(store objectStore, key, file string) error {
	body, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	_, err = signedObjectRequest(store, http.MethodPut, key, body)
	return err
}

func downloadObject(store objectStore, key, out string) error {
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	fh, err := os.Create(out)
	if err != nil {
		return err
	}
	defer fh.Close()
	return writeObject(store, key, fh)
}

func writeObject(store objectStore, key string, out io.Writer) error {
	body, err := signedObjectRequest(store, http.MethodGet, key, nil)
	if err != nil {
		return err
	}
	_, err = out.Write(body)
	return err
}

func objectExists(store objectStore, key string) error {
	_, err := signedObjectRequest(store, http.MethodHead, key, nil)
	return err
}

func signedObjectRequest(store objectStore, method, key string, body []byte) ([]byte, error) {
	endpointURL, err := url.Parse(store.endpoint)
	if err != nil {
		return nil, err
	}
	canonicalURI := "/" + store.bucket
	if key != "" {
		canonicalURI += "/" + escapeObjectPath(key)
	}
	endpointURL.Path = canonicalURI
	endpointURL.RawQuery = ""

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	payloadHash := hexSHA256(body)
	req, err := http.NewRequest(method, endpointURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Host", endpointURL.Host)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	if method == http.MethodPut {
		req.Header.Set("Content-Type", contentType(key))
	}
	req.Host = endpointURL.Host

	signedHeaders, canonicalHeaders := canonicalHeaders(req.Header)
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := date + "/" + store.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(signingKey(store.secretKey, date, store.region), []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+store.accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Object Storage %s %s failed: HTTP %d", method, key, resp.StatusCode)
	}
	return data, nil
}

func canonicalHeaders(headers http.Header) (string, string) {
	names := make([]string, 0, len(headers))
	lowerToCanonical := make(map[string]string, len(headers))
	for name := range headers {
		lower := strings.ToLower(name)
		names = append(names, lower)
		lowerToCanonical[lower] = name
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		values := headers.Values(lowerToCanonical[name])
		for i := range values {
			values[i] = strings.Join(strings.Fields(values[i]), " ")
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.Join(values, ","))
		b.WriteByte('\n')
	}
	return strings.Join(names, ";"), b.String()
}

func escapeObjectPath(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func signingKey(secret, date, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func regionFromEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "us-east-1"
	}
	host := u.Hostname()
	if strings.HasSuffix(host, ".linodeobjects.com") {
		parts := strings.Split(host, ".")
		if len(parts) > 0 && parts[0] != "" {
			return parts[0]
		}
	}
	return "us-east-1"
}

func contentType(key string) string {
	switch {
	case strings.HasSuffix(key, ".json"):
		return "application/json"
	case strings.HasSuffix(key, ".sha256"):
		return "text/plain"
	default:
		return "application/gzip"
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
