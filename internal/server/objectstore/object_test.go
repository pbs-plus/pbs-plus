//go:build linux

package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/pbs-plus/pxar/datastore"
)

const roundTripKey = "backup.sql.gz"

func newRoundTripHandler(t *testing.T) (handler *Handler, root, indexPath string) {
	t.Helper()
	config := testConfig()
	config.Credentials[0].Grants[0].Delete = true
	handler, err := NewHandler(config, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	root = t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".chunks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	handler.resolveDatastore = func(string) (string, error) { return root, nil }
	if err := handler.OpenMultipartSpool(filepath.Join(t.TempDir(), "uploads")); err != nil {
		t.Fatal(err)
	}
	indexPath = filepath.Join(t.TempDir(), "index.db")
	if err := handler.OpenKeyIndex(indexPath); err != nil {
		t.Fatal(err)
	}

	previousLocks, previousActive := objectstoreLocksDir, objectstoreActiveOperationsDir
	objectstoreLocksDir = filepath.Join(t.TempDir(), "locks")
	objectstoreActiveOperationsDir = filepath.Join(t.TempDir(), "active-operations")
	t.Cleanup(func() {
		objectstoreLocksDir, objectstoreActiveOperationsDir = previousLocks, previousActive
		_ = handler.Close()
	})
	return handler, root, indexPath
}

func signedPut(t *testing.T, target string, body []byte, headers map[string]string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPut, target, bytes.NewReader(body))
	sum := sha256.Sum256(body)
	request.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	return signer.SignV4(*request, testAccessKey, testSecretKey, "", "us-west-2")
}

func signedObjectRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	request := signedRequest(t, method, target, testAccessKey, testSecretKey, "us-west-2")
	return request
}

func signedRangeRequest(t *testing.T, target, value string) *http.Request {
	t.Helper()
	request := signedObjectRequest(t, http.MethodGet, target)
	request.Header.Set("Range", value)
	return request
}

func serve(t *testing.T, handler *Handler, request *http.Request) *http.Response {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Result()
}

func mustStatus(t *testing.T, response *http.Response, status int) {
	t.Helper()
	if response.StatusCode != status {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d, want %d; body = %s", response.StatusCode, status, body)
	}
}

func TestObjectRoundTrip(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	body := []byte("mariadb dump payload for the s3 outpost round trip")
	base := "http://s3.test/mariadb/" + roundTripKey

	sum := sha256.Sum256(body)
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`

	response := serve(t, handler, signedPut(t, base, body, map[string]string{
		"Content-Type":   "application/gzip",
		"X-Amz-Meta-App": "mariadb-operator",
	}))
	mustStatus(t, response, http.StatusOK)
	if got := response.Header.Get("ETag"); got != wantETag {
		t.Fatalf("PUT etag = %q, want %q", got, wantETag)
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodGet, base))
	mustStatus(t, response, http.StatusOK)
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GET body = %q", got)
	}
	for name, want := range map[string]string{
		"ETag":           wantETag,
		"Content-Type":   "application/gzip",
		"X-Amz-Meta-App": "mariadb-operator",
		"Accept-Ranges":  "bytes",
		"Content-Length": strconv.Itoa(len(body)),
	} {
		if got := response.Header.Get(name); got != want {
			t.Fatalf("GET header %s = %q, want %q", name, got, want)
		}
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodHead, base))
	mustStatus(t, response, http.StatusOK)
	if got := response.Header.Get("Content-Length"); got != strconv.Itoa(len(body)) {
		t.Fatalf("HEAD content length = %q", got)
	}

	response = serve(t, handler, signedRangeRequest(t, base, "bytes=8-12"))
	mustStatus(t, response, http.StatusPartialContent)
	got, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body[8:13]) {
		t.Fatalf("range body = %q", got)
	}
	if got := response.Header.Get("Content-Range"); got != "bytes 8-12/"+strconv.Itoa(len(body)) {
		t.Fatalf("content range = %q", got)
	}

	response = serve(t, handler, signedRangeRequest(t, base, "bytes=-7"))
	mustStatus(t, response, http.StatusPartialContent)
	got, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body[len(body)-7:]) {
		t.Fatalf("suffix range body = %q", got)
	}

	mustStatus(t, serve(t, handler, signedRangeRequest(t, base, "bytes=100-200")), http.StatusRequestedRangeNotSatisfiable)

	response = serve(t, handler, signedObjectRequest(t, http.MethodGet, base+"?uploads"))
	mustStatus(t, response, http.StatusNotImplemented)
}

func TestObjectStreamingPut(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	body := []byte("streamed aws-chunked object payload with signed chunks")
	base := "http://s3.test/mariadb/" + roundTripKey

	sum := sha256.Sum256(body)
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`

	request, _ := signedStreamingRequest(t, body, "", "")
	response := serve(t, handler, request)
	mustStatus(t, response, http.StatusOK)
	if got := response.Header.Get("ETag"); got != wantETag {
		t.Fatalf("streaming PUT etag = %q, want %q", got, wantETag)
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodGet, base))
	mustStatus(t, response, http.StatusOK)
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GET body = %q", got)
	}
}

func TestObjectStreamingPutWithCrc32cTrailer(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	body := []byte("streamed object guarded by a crc32c trailer")
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	request, _ := signedStreamingRequest(t, body, "X-Amz-Checksum-Crc32c", base64.StdEncoding.EncodeToString(checksum[:]))
	mustStatus(t, serve(t, handler, request), http.StatusOK)

	response := serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/"+roundTripKey))
	mustStatus(t, response, http.StatusOK)
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GET body = %q", got)
	}
}

func TestObjectOverwriteAndDelete(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	base := "http://s3.test/mariadb/" + roundTripKey
	bucket := handler.config.Buckets[0]

	mustStatus(t, serve(t, handler, signedPut(t, base, []byte("first version"), nil)), http.StatusOK)
	mustStatus(t, serve(t, handler, signedPut(t, base, []byte("second version"), nil)), http.StatusOK)

	response := serve(t, handler, signedObjectRequest(t, http.MethodGet, base))
	mustStatus(t, response, http.StatusOK)
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second version" {
		t.Fatalf("GET body after overwrite = %q", got)
	}

	_, snapshotDir, err := handler.locateObject(context.Background(), bucket, roundTripKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, ".protected"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	mustStatus(t, serve(t, handler, signedObjectRequest(t, http.MethodDelete, base)), http.StatusForbidden)

	if err := os.Remove(filepath.Join(snapshotDir, ".protected")); err != nil {
		t.Fatal(err)
	}
	mustStatus(t, serve(t, handler, signedObjectRequest(t, http.MethodDelete, base)), http.StatusNoContent)
	mustStatus(t, serve(t, handler, signedObjectRequest(t, http.MethodGet, base)), http.StatusNotFound)

	dirs := 0
	entries, err := os.ReadDir(filepath.Dir(snapshotDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			dirs++
		}
	}
	if dirs != 0 {
		t.Fatalf("%d snapshot dirs remain after delete", dirs)
	}
}

func TestObjectGrants(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	base := "http://s3.test/mariadb/" + roundTripKey

	mustStatus(t, serve(t, handler, signedPut(t, base, []byte("granted write"), nil)), http.StatusOK)

	config := handler.config
	config.Credentials[0].Grants[0] = Grant{Bucket: "mariadb", Read: true, Delete: true}
	readOnly, err := NewHandler(config, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	readOnly.resolveDatastore = handler.resolveDatastore
	mustStatus(t, serve(t, readOnly, signedPut(t, base, []byte("denied write"), nil)), http.StatusForbidden)

	config.Credentials[0].Grants[0] = Grant{Bucket: "mariadb", Read: true}
	readerOnly, err := NewHandler(config, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	readerOnly.resolveDatastore = handler.resolveDatastore
	mustStatus(t, serve(t, readerOnly, signedObjectRequest(t, http.MethodDelete, base)), http.StatusForbidden)
	mustStatus(t, serve(t, readerOnly, signedObjectRequest(t, http.MethodGet, base)), http.StatusOK)
}

func TestObjectIndexSurvivesRestart(t *testing.T) {
	handler, _, indexPath := newRoundTripHandler(t)
	base := "http://s3.test/mariadb/" + roundTripKey
	body := []byte("indexed object survives a handler restart")
	mustStatus(t, serve(t, handler, signedPut(t, base, body, nil)), http.StatusOK)

	restarted, err := NewHandler(handler.config, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted.resolveDatastore = handler.resolveDatastore
	if err := restarted.OpenKeyIndex(indexPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })

	response := serve(t, restarted, signedObjectRequest(t, http.MethodGet, base))
	mustStatus(t, response, http.StatusOK)
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GET after restart = %q", got)
	}
}

func TestObjectManifestMetadata(t *testing.T) {
	handler, root, _ := newRoundTripHandler(t)
	base := "http://s3.test/mariadb/" + roundTripKey
	body := []byte("manifest carrying object metadata")
	mustStatus(t, serve(t, handler, signedPut(t, base, body, map[string]string{
		"Content-Type":   "text/plain",
		"X-Amz-Meta-Env": "production",
	})), http.StatusOK)

	snapshotDir := filepath.Join(root, "ns", "databases", "host", "mariadb")
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot string
	for _, entry := range entries {
		if entry.IsDir() {
			if snapshot != "" {
				t.Fatalf("expected one snapshot, found %q and %q", snapshot, entry.Name())
			}
			snapshot = entry.Name()
		}
	}
	if snapshot == "" {
		t.Fatal("no snapshot directory found")
	}
	raw, err := os.ReadFile(filepath.Join(snapshotDir, snapshot, "index.json.blob"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := datastore.DecodeBlob(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Unprotected json.RawMessage `json:"unprotected"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	var extra manifestUnprotected
	if err := json.Unmarshal(manifest.Unprotected, &extra); err != nil {
		t.Fatal(err)
	}
	if extra.Object.Key != roundTripKey || extra.Object.ContentType != "text/plain" {
		t.Fatalf("manifest metadata = %+v", extra.Object)
	}
	if extra.Object.UserMetadata["env"] != "production" {
		t.Fatalf("manifest user metadata = %v", extra.Object.UserMetadata)
	}
	sum := sha256.Sum256(body)
	if extra.Object.ETag != `"`+hex.EncodeToString(sum[:])+`"` {
		t.Fatalf("manifest etag = %q", extra.Object.ETag)
	}
}
