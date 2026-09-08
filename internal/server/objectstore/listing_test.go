//go:build linux

package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/signer"
)

func newMinioClient(t *testing.T, handler *Handler) *minio.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		Creds:  credentials.NewStaticV4(testAccessKey, testSecretKey, ""),
		Region: "us-west-2",
		Secure: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func mustPutKey(t *testing.T, handler *Handler, key string, body []byte) {
	t.Helper()
	response := serve(t, handler, signedPut(t, "http://s3.test/mariadb/"+s3utils.EncodePath(key), body, nil))
	mustStatus(t, response, http.StatusOK)
}

func signedBodyRequest(t *testing.T, method, target string, body []byte) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, target, bytes.NewReader(body))
	sum := sha256.Sum256(body)
	request.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
	return signer.SignV4(*request, testAccessKey, testSecretKey, "", "us-west-2")
}

func TestListObjectsV2RoundTrip(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	keys := []string{"backup-1.sql.gz", "backup-2.sql.gz", "backup-3.sql.gz", "logs/app.log"}
	for _, key := range keys {
		mustPutKey(t, handler, key, []byte("body "+key))
	}

	client := newMinioClient(t, handler)
	ctx := context.Background()
	var got []minio.ObjectInfo
	for object := range client.ListObjects(ctx, "mariadb", minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			t.Fatalf("list: %v", object.Err)
		}
		got = append(got, object)
	}
	if len(got) != len(keys) {
		t.Fatalf("listed %d objects, want %d: %+v", len(got), len(keys), got)
	}
	for i, key := range keys {
		if got[i].Key != key {
			t.Fatalf("object %d = %q, want %q", i, got[i].Key, key)
		}
		if got[i].Size != int64(len("body "+key)) {
			t.Fatalf("object %q size = %d", key, got[i].Size)
		}
	}
}

func TestListObjectsV2PrefixAndDelimiter(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	for _, key := range []string{"backup-1.sql.gz", "backup-2.sql.gz", "logs/app.log", "logs/db.log"} {
		mustPutKey(t, handler, key, []byte("data"))
	}

	client := newMinioClient(t, handler)
	ctx := context.Background()

	var objects []minio.ObjectInfo
	for object := range client.ListObjects(ctx, "mariadb", minio.ListObjectsOptions{Prefix: "backup-"}) {
		if object.Err != nil {
			t.Fatalf("list: %v", object.Err)
		}
		objects = append(objects, object)
	}
	if len(objects) != 2 || objects[0].Key != "backup-1.sql.gz" || objects[1].Key != "backup-2.sql.gz" {
		t.Fatalf("prefix listing = %+v", objects)
	}

	objects = nil
	var prefixes []string
	for object := range client.ListObjects(ctx, "mariadb", minio.ListObjectsOptions{Recursive: false}) {
		if object.Err != nil {
			t.Fatalf("list: %v", object.Err)
		}
		if strings.HasSuffix(object.Key, "/") {
			prefixes = append(prefixes, object.Key)
		} else {
			objects = append(objects, object)
		}
	}
	if len(objects) != 2 || len(prefixes) != 1 || prefixes[0] != "logs/" {
		t.Fatalf("delimiter listing: objects=%+v prefixes=%v", objects, prefixes)
	}
}

func TestListObjectsV2Pagination(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	for i := range 7 {
		mustPutKey(t, handler, "backup-"+string(rune('a'+i))+".sql.gz", []byte("data"))
	}

	client := newMinioClient(t, handler)
	ctx := context.Background()
	var got []string
	for object := range client.ListObjects(ctx, "mariadb", minio.ListObjectsOptions{Recursive: true, MaxKeys: 3}) {
		if object.Err != nil {
			t.Fatalf("list: %v", object.Err)
		}
		got = append(got, object.Key)
	}
	if len(got) != 7 {
		t.Fatalf("paged listing returned %d keys: %v", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("listing not sorted: %v", got)
		}
	}
}

func TestListObjectsV2EncodesKeys(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	mustPutKey(t, handler, "daily backup.sql.gz", []byte("data"))

	response := serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/?list-type=2&encoding-type=url"))
	mustStatus(t, response, http.StatusOK)
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "daily%20backup.sql.gz") {
		t.Fatalf("url-encoded key missing from listing: %s", body)
	}

	client := newMinioClient(t, handler)
	ctx := context.Background()
	for object := range client.ListObjects(ctx, "mariadb", minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			t.Fatalf("list: %v", object.Err)
		}
		if object.Key != "daily backup.sql.gz" {
			t.Fatalf("client-decoded key = %q", object.Key)
		}
	}
}

func TestDeleteObjects(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	for _, key := range []string{"one.sql.gz", "two.sql.gz", "three.sql.gz"} {
		mustPutKey(t, handler, key, []byte("data"))
	}

	data, err := xml.Marshal(deleteObjectsRequest{Objects: []deleteRequestObject{
		{Key: "one.sql.gz"},
		{Key: "two.sql.gz"},
		{Key: "missing.sql.gz"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	response := serve(t, handler, signedBodyRequest(t, http.MethodPost, "http://s3.test/mariadb?delete", data))
	mustStatus(t, response, http.StatusOK)
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"one.sql.gz", "two.sql.gz", "missing.sql.gz"} {
		if !strings.Contains(string(body), "<Key>"+key+"</Key>") {
			t.Fatalf("deleted key %q missing from %s", key, body)
		}
	}
	for _, key := range []string{"one.sql.gz", "two.sql.gz"} {
		mustStatus(t, serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/"+key)), http.StatusNotFound)
	}
	mustStatus(t, serve(t, handler, signedObjectRequest(t, http.MethodGet, "http://s3.test/mariadb/three.sql.gz")), http.StatusOK)
}

func TestReconcileIndex(t *testing.T) {
	handler, _, _ := newRoundTripHandler(t)
	mustPutKey(t, handler, "kept.sql.gz", []byte("kept"))
	mustPutKey(t, handler, "gone.sql.gz", []byte("gone"))

	bucket := handler.config.Buckets[0]
	_, snapshotDir, err := handler.locateObject(context.Background(), bucket, "gone.sql.gz")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(snapshotDir); err != nil {
		t.Fatal(err)
	}

	if err := handler.ReconcileIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := handler.index.get(context.Background(), bucket.Name, "gone.sql.gz"); err != nil || ok {
		t.Fatalf("stale row survived reconcile: %v %v", ok, err)
	}
	object, ok, err := handler.index.get(context.Background(), bucket.Name, "kept.sql.gz")
	if err != nil || !ok || object.Key != "kept.sql.gz" {
		t.Fatalf("reconcile lost kept key: %+v %v %v", object, ok, err)
	}
}
