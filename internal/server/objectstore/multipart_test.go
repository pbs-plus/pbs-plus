//go:build linux

package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestMultipartFPutObjectRoundTrip(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	const partSize = 5 << 20
	body := make([]byte, partSize+partSize+2<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "dump.xb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	client := newMinioClient(t, handler)
	ctx := context.Background()
	info, err := client.FPutObject(ctx, "mariadb", "big/dump.xb", file.Name(), minio.PutObjectOptions{
		PartSize: uint64(partSize),
	})
	if err != nil {
		t.Fatalf("fput: %v", err)
	}
	if !strings.HasSuffix(info.ETag, "-3") {
		t.Fatalf("multipart etag = %q, want -3 suffix", info.ETag)
	}
	if info.Size != int64(len(body)) {
		t.Fatalf("upload size = %d, want %d", info.Size, len(body))
	}

	reader, err := client.GetObject(ctx, "mariadb", "big/dump.xb", minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = reader.Close()
	if !bytes.Equal(got, body) {
		t.Fatalf("multipart body mismatch: %d bytes", len(got))
	}

	opts := minio.GetObjectOptions{}
	opts.SetRange(int64(partSize-2), int64(partSize+2))
	reader, err = client.GetObject(ctx, "mariadb", "big/dump.xb", opts)
	if err != nil {
		t.Fatalf("ranged get: %v", err)
	}
	got, err = io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	_ = reader.Close()
	if !bytes.Equal(got, body[partSize-2:partSize+3]) {
		t.Fatalf("range across part boundary mismatch")
	}

	if err := client.RemoveObject(ctx, "mariadb", "big/dump.xb", minio.RemoveObjectOptions{}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	_, err = client.StatObject(ctx, "mariadb", "big/dump.xb", minio.StatObjectOptions{})
	if minio.ToErrorResponse(err).Code != "NoSuchKey" {
		t.Fatalf("stat after remove: %v", err)
	}
}

func startMultipartUpload(t *testing.T, handler *Handler, key string) (uploadID string) {
	t.Helper()
	response := serve(t, handler, signedObjectRequest(t, http.MethodPost, "http://s3.test/mariadb/"+key+"?uploads"))
	mustStatus(t, response, http.StatusOK)
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result initiateMultipartUploadResult
	if err := xml.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.UploadID == "" {
		t.Fatalf("create multipart: %s", data)
	}
	return result.UploadID
}

func uploadMultipartPart(t *testing.T, handler *Handler, key, uploadID string, part int, body []byte) string {
	t.Helper()
	target := "http://s3.test/mariadb/" + key + "?partNumber=" + strconv.Itoa(part) + "&uploadId=" + uploadID
	response := serve(t, handler, signedBodyRequest(t, http.MethodPut, target, body))
	mustStatus(t, response, http.StatusOK)
	etag := response.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("upload part: missing etag")
	}
	return etag
}

func TestMultipartManualCompleteAcrossRestart(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	key := "restarted.sql.gz"
	uploadID := startMultipartUpload(t, handler, key)
	etagOne := uploadMultipartPart(t, handler, key, uploadID, 1, []byte("part-one "))

	restarted, err := NewHandler(handler.config, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted.resolveDatastore = handler.resolveDatastore
	if err := restarted.OpenMultipartSpool(handler.multipartDir); err != nil {
		t.Fatal(err)
	}
	etagTwo := uploadMultipartPart(t, restarted, key, uploadID, 2, []byte("part-two"))

	data, err := xml.Marshal(completeMultipartUploadRequest{Parts: []completeRequestPart{
		{PartNumber: 1, ETag: etagOne},
		{PartNumber: 2, ETag: etagTwo},
	}})
	if err != nil {
		t.Fatal(err)
	}
	response := serve(t, restarted, signedBodyRequest(t, http.MethodPost, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID, data))
	mustStatus(t, response, http.StatusOK)

	response = serve(t, restarted, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/"+key))
	mustStatus(t, response, http.StatusOK)
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "part-one part-two" {
		t.Fatalf("body = %q", got)
	}
	if _, err := os.Stat(filepath.Join(handler.multipartDir, uploadID)); !os.IsNotExist(err) {
		t.Fatalf("upload spool survived completion")
	}

	response = serve(t, restarted, signedObjectRequest(t, http.MethodDelete, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID))
	mustStatus(t, response, http.StatusNotFound)
}

func TestMultipartAbortAndUnknownUpload(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	key := "aborted.sql.gz"
	uploadID := startMultipartUpload(t, handler, key)
	uploadMultipartPart(t, handler, key, uploadID, 1, []byte("part-one "))

	response := serve(t, handler, signedObjectRequest(t, http.MethodDelete, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID))
	mustStatus(t, response, http.StatusNoContent)
	if _, err := os.Stat(filepath.Join(handler.multipartDir, uploadID)); !os.IsNotExist(err) {
		t.Fatalf("upload spool survived abort")
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodDelete, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID))
	mustStatus(t, response, http.StatusNotFound)
}

func TestMultipartCompleteRejectsBadETag(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	key := "tampered.sql.gz"
	uploadID := startMultipartUpload(t, handler, key)
	etagOne := uploadMultipartPart(t, handler, key, uploadID, 1, []byte("part-one "))

	data, err := xml.Marshal(completeMultipartUploadRequest{Parts: []completeRequestPart{
		{PartNumber: 1, ETag: `"0` + strings.Trim(etagOne, `"`) + `"`},
	}})
	if err != nil {
		t.Fatal(err)
	}
	response := serve(t, handler, signedBodyRequest(t, http.MethodPost, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID, data))
	mustStatus(t, response, http.StatusBadRequest)
	if !strings.Contains(strings.ToLower(response.Status), "bad request") {
		t.Fatalf("status text = %q", response.Status)
	}
	if dir, err := handler.uploadDir(testConfig().Buckets[0], uploadID); err != nil {
		t.Fatal(err)
	} else if _, err := os.Stat(dir); err != nil {
		t.Fatalf("failed upload removed its spool: %v", err)
	}
}

func TestReapMultipartUploads(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	staleID := startMultipartUpload(t, handler, "stale.sql.gz")
	freshID := startMultipartUpload(t, handler, "fresh.sql.gz")

	staleDir := filepath.Join(handler.multipartDir, staleID)
	old := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(staleDir, old, old); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	if err := handler.ReapMultipartUploads(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staleDir); !os.IsNotExist(err) {
		t.Fatalf("stale upload survived reap")
	}
	if _, err := os.Stat(filepath.Join(handler.multipartDir, freshID)); err != nil {
		t.Fatalf("fresh upload was reaped")
	}
}

func TestMultipartSpoolsInsideDatastore(t *testing.T) {
	handler, root, _ := newRoundTripHandler(t)
	handler.multipartDir = ""

	key := "placed.sql.gz"
	uploadID := startMultipartUpload(t, handler, key)
	uploadMultipartPart(t, handler, key, uploadID, 1, []byte("part-one"))

	partPath := filepath.Join(root, ".pbs-plus", "objectstore", "uploads", uploadID, "part-1")
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("part not spooled inside the datastore: %v", err)
	}

	response := serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID))
	mustStatus(t, response, http.StatusOK)
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "<PartNumber>1</PartNumber>") {
		t.Fatalf("list parts = %s", data)
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodDelete, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID))
	mustStatus(t, response, http.StatusNoContent)
	if _, err := os.Stat(filepath.Dir(partPath)); !os.IsNotExist(err) {
		t.Fatalf("abort left the spool behind: %v", err)
	}
}

func TestListPartsAndUploads(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	key := "listed.sql.gz"
	otherKey := "other.sql.gz"
	uploadID := startMultipartUpload(t, handler, key)
	startMultipartUpload(t, handler, otherKey)
	uploadMultipartPart(t, handler, key, uploadID, 1, []byte("part-one "))
	uploadMultipartPart(t, handler, key, uploadID, 2, []byte("part-two"))

	response := serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID))
	mustStatus(t, response, http.StatusOK)
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	if !strings.Contains(body, "<PartNumber>1</PartNumber>") || !strings.Contains(body, "<PartNumber>2</PartNumber>") {
		t.Fatalf("list parts = %s", body)
	}
	if !strings.Contains(body, "<Size>9</Size>") || !strings.Contains(body, "<Size>8</Size>") {
		t.Fatalf("list parts sizes = %s", body)
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/"+key+"?uploadId="+uploadID+"&max-parts=1"))
	mustStatus(t, response, http.StatusOK)
	data, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "<IsTruncated>true</IsTruncated>") {
		t.Fatalf("paged list parts = %s", data)
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb?uploads"))
	mustStatus(t, response, http.StatusOK)
	data, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	body = string(data)
	if !strings.Contains(body, "<Key>"+key+"</Key>") || !strings.Contains(body, "<Key>"+otherKey+"</Key>") {
		t.Fatalf("list uploads = %s", body)
	}
	if !strings.Contains(body, "<UploadId>"+uploadID+"</UploadId>") {
		t.Fatalf("list uploads ids = %s", body)
	}
	if !strings.Contains(body, "<IsTruncated>false</IsTruncated>") {
		t.Fatalf("list uploads truncation = %s", body)
	}

	response = serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/"+key+"?uploadId=unknown"))
	mustStatus(t, response, http.StatusNotFound)
}
