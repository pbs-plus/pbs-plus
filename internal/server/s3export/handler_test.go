//go:build linux

package s3export

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func writeFile(t *testing.T, fs billy.Filesystem, name, body string) {
	t.Helper()
	f, err := fs.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func newTestClient(t *testing.T, secret string) (*minio.Client, *Handler) {
	t.Helper()
	tlsOff := false
	h := NewHandler(Config{TLS: &tlsOff, Credentials: []Credential{{AccessKey: "reader", SecretKey: "reader-secret"}}})
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("reader", secret, ""),
		Region:       defaultRegion,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, h
}

func listKeys(t *testing.T, client *minio.Client, bucket string, opts minio.ListObjectsOptions) []string {
	t.Helper()
	var keys []string
	for object := range client.ListObjects(context.Background(), bucket, opts) {
		if object.Err != nil {
			t.Fatalf("list %s: %v", bucket, object.Err)
		}
		keys = append(keys, object.Key)
	}
	return keys
}

func TestExportServesSnapshotReadOnly(t *testing.T) {
	client, h := newTestClient(t, "reader-secret")
	snapshot := memfs.New()
	for name, body := range map[string]string{
		"a.txt":         "top",
		"a/x":           "nested-x",
		"a/y/deep.bin":  "0123456789",
		"b":             "bee",
		"etc/hosts":     "127.0.0.1 localhost\n",
		"etc/passwd":    "root:x:0:0\n",
		"zz/last":       "z",
		"a/y/deeper/q":  "q",
		"a-sibling/one": "1",
	} {
		writeFile(t, snapshot, name, body)
	}
	if err := h.Add("Host-Web01_2026", snapshot); err != nil {
		t.Fatal(err)
	}
	if err := h.Add("other", memfs.New()); err != nil {
		t.Fatal(err)
	}
	if err := h.Add("host-web01-2026", memfs.New()); err == nil {
		t.Fatal("colliding bucket name accepted")
	}

	buckets, err := client.ListBuckets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 || buckets[0].Name != "host-web01-2026" || buckets[1].Name != "other" {
		t.Fatalf("buckets = %+v", buckets)
	}

	want := []string{"a-sibling/one", "a.txt", "a/x", "a/y/deep.bin", "a/y/deeper/q", "b", "etc/hosts", "etc/passwd", "zz/last"}
	for _, maxKeys := range []int{0, 1, 2, 1000} {
		got := listKeys(t, client, "host-web01-2026", minio.ListObjectsOptions{Recursive: true, MaxKeys: maxKeys})
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("recursive list (max-keys %d) = %v, want %v", maxKeys, got, want)
		}
		v1 := listKeys(t, client, "host-web01-2026", minio.ListObjectsOptions{Recursive: true, MaxKeys: maxKeys, UseV1: true})
		if strings.Join(v1, ",") != strings.Join(want, ",") {
			t.Fatalf("v1 recursive list (max-keys %d) = %v", maxKeys, v1)
		}
	}

	top := listKeys(t, client, "host-web01-2026", minio.ListObjectsOptions{})
	sort.Strings(top)
	if strings.Join(top, ",") != "a-sibling/,a.txt,a/,b,etc/,zz/" {
		t.Fatalf("delimited list = %v", top)
	}
	prefixed := listKeys(t, client, "host-web01-2026", minio.ListObjectsOptions{Prefix: "a/y/", Recursive: true})
	if strings.Join(prefixed, ",") != "a/y/deep.bin,a/y/deeper/q" {
		t.Fatalf("prefix list = %v", prefixed)
	}
	partial := listKeys(t, client, "host-web01-2026", minio.ListObjectsOptions{Prefix: "etc/pa"})
	if strings.Join(partial, ",") != "etc/passwd" {
		t.Fatalf("partial prefix list = %v", partial)
	}
	after := listKeys(t, client, "host-web01-2026", minio.ListObjectsOptions{Recursive: true, StartAfter: "a/y/deep.bin"})
	if strings.Join(after, ",") != "a/y/deeper/q,b,etc/hosts,etc/passwd,zz/last" {
		t.Fatalf("start-after list = %v", after)
	}

	info, err := client.StatObject(context.Background(), "host-web01-2026", "a/y/deep.bin", minio.StatObjectOptions{})
	if err != nil || info.Size != 10 {
		t.Fatalf("stat = %+v, %v", info, err)
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(2, 5); err != nil {
		t.Fatal(err)
	}
	object, err := client.GetObject(context.Background(), "host-web01-2026", "a/y/deep.bin", opts)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(object)
	if err != nil || string(body) != "2345" {
		t.Fatalf("ranged get = %q, %v", body, err)
	}

	for _, key := range []string{"a", "a/", "missing", "a/../b"} {
		if _, err := client.StatObject(context.Background(), "host-web01-2026", key, minio.StatObjectOptions{}); err == nil {
			t.Fatalf("stat %q succeeded", key)
		}
	}
	if _, err := client.PutObject(context.Background(), "host-web01-2026", "new", bytes.NewReader([]byte("x")), 1, minio.PutObjectOptions{}); err == nil {
		t.Fatal("put succeeded on a read-only export")
	} else if minio.ToErrorResponse(err).Code != "AccessDenied" {
		t.Fatalf("put error = %v", err)
	}
	if err := client.RemoveObject(context.Background(), "host-web01-2026", "b", minio.RemoveObjectOptions{}); err == nil {
		t.Fatal("delete succeeded on a read-only export")
	}

	h.Remove("Host-Web01_2026")
	if exists, err := client.BucketExists(context.Background(), "host-web01-2026"); err != nil || exists {
		t.Fatalf("removed bucket exists = %v, %v", exists, err)
	}
}

func TestExportRejectsBadCredentials(t *testing.T) {
	client, h := newTestClient(t, "wrong-secret")
	if err := h.Add("share", memfs.New()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListBuckets(context.Background()); minio.ToErrorResponse(err).Code != "AccessDenied" {
		t.Fatalf("list with a bad secret = %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	tlsOff := false
	cases := map[string]Config{
		"no credentials":   {},
		"short secret":     {Credentials: []Credential{{AccessKey: "abc", SecretKey: "short"}}},
		"duplicate key":    {Credentials: []Credential{{AccessKey: "abc", SecretKey: "12345678"}, {AccessKey: "abc", SecretKey: "87654321"}}},
		"cert without key": {TLSCertFile: "/c", Credentials: []Credential{{AccessKey: "abc", SecretKey: "12345678"}}},
		"cert without tls": {TLS: &tlsOff, TLSCertFile: "/c", TLSKeyFile: "/k", Credentials: []Credential{{AccessKey: "abc", SecretKey: "12345678"}}},
	}
	for name, config := range cases {
		if err := config.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := (Config{Credentials: []Credential{{AccessKey: "abc", SecretKey: "12345678"}}}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
