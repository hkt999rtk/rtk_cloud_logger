package cloudlogger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
)

// S3ArchiveConfig contains no private decryption or verifier signing key.
// Access credentials are supplied separately by the restricted writer process.
type S3ArchiveConfig struct {
	Endpoint      string `json:"endpoint"`
	Region        string `json:"region"`
	SigningRegion string `json:"signing_region"`
	Bucket        string `json:"bucket"`
	Environment   string `json:"environment"`
}
type S3ArchiveStore struct {
	config         S3ArchiveConfig
	access, secret string
	client         *http.Client
}

func NewS3ArchiveStore(c S3ArchiveConfig, access, secret string) (*S3ArchiveStore, error) {
	if c.SigningRegion == "" {
		c.SigningRegion = "us-east-1"
	}
	u, e := url.Parse(c.Endpoint)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || !billingarchive.SafeID(c.Environment) || !billingarchive.SafeID(c.Region) || !billingarchive.SafeID(c.SigningRegion) || c.Bucket != "rtk-cloud-"+c.Environment+"-billing-backup-"+c.Region || access == "" || secret == "" {
		return nil, errors.New("invalid private environment-scoped backup object store")
	}
	return &S3ArchiveStore{c, access, secret, &http.Client{Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (s *S3ArchiveStore) request(ctx context.Context, method, key, sha string, size int64, body io.Reader) (*http.Response, error) {
	if !strings.HasPrefix(key, "billing-raw/") && !strings.HasPrefix(key, "billing-inbox-snapshots/") {
		return nil, ErrLifecycleConflict
	}
	for _, part := range strings.Split(key, "/") {
		if !billingarchive.SafeID(part) {
			return nil, ErrLifecycleConflict
		}
	}
	u, _ := url.Parse(s.config.Endpoint)
	u.Path = "/" + s.config.Bucket + "/" + key
	req, e := http.NewRequestWithContext(ctx, method, u.String(), body)
	if e != nil {
		return nil, e
	}
	req.ContentLength = size
	now := time.Now().UTC()
	amz := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amz)
	req.Header.Set("X-Amz-Content-Sha256", sha)
	headers := "host:" + u.Host + "\n"
	signed := "host;x-amz-content-sha256;x-amz-date"
	if method == http.MethodPut {
		req.Header.Set("If-None-Match", "*")
		headers += "if-none-match:*\n"
		signed = "host;if-none-match;x-amz-content-sha256;x-amz-date"
	}
	headers += "x-amz-content-sha256:" + sha + "\nx-amz-date:" + amz + "\n"
	canonical := method + "\n" + u.EscapedPath() + "\n\n" + headers + "\n" + signed + "\n" + sha
	scope := date + "/" + s.config.SigningRegion + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amz + "\n" + scope + "\n" + billingarchive.Digest([]byte(canonical))
	keyDate := mac([]byte("AWS4"+s.secret), date)
	keyRegion := mac(keyDate, s.config.SigningRegion)
	keyService := mac(keyRegion, "s3")
	keySigning := mac(keyService, "aws4_request")
	sig := hex.EncodeToString(mac(keySigning, toSign))
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s.access, scope, signed, sig))
	return s.client.Do(req)
}
func mac(key []byte, v string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(v))
	return h.Sum(nil)
}

// PutImmutable reconciles ambiguous or already-existing PUTs by comparing the
// whole remote object. ETag is never treated as a cryptographic content digest.
func (s *S3ArchiveStore) PutImmutable(ctx context.Context, key, path string, size int64, sha string) error {
	if size < 1 || size > billingarchive.MaxCipherPartBytes || len(sha) != 64 {
		return ErrLifecycleConflict
	}
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() != size {
		return ErrBillingUnavailable
	}
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	h := sha256.New()
	n, e := io.Copy(h, f)
	if e != nil || n != size || hex.EncodeToString(h.Sum(nil)) != sha {
		f.Close()
		return errors.New("sealed local artifact changed")
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		f.Close()
		return e
	}
	resp, putErr := s.request(ctx, http.MethodPut, key, sha, size, f)
	f.Close()
	if putErr == nil {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != 409 && resp.StatusCode != 412 && resp.StatusCode < 500 {
			return fmt.Errorf("immutable artifact PUT rejected (%d)", resp.StatusCode)
		}
	}
	resp, e = s.request(ctx, http.MethodGet, key, billingarchive.Digest(nil), 0, nil)
	if e != nil {
		return errors.New("artifact publication uncertain; retry exact sealed bytes")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("artifact publication not confirmed")
	}
	h = sha256.New()
	n, e = io.Copy(h, io.LimitReader(resp.Body, size+1))
	if e != nil || n != size || hex.EncodeToString(h.Sum(nil)) != sha {
		return errors.New("immutable remote artifact differs")
	}
	return nil
}
