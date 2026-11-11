//go:build linux

package outpost

import (
	"context"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pbs-plus/pbs-plus/internal/server/s3export"
)

func testS3ExportOutpost() Outpost {
	tlsOff := false
	return Outpost{
		Name:       "s3-export",
		Type:       TypeS3Export,
		ListenAddr: "127.0.0.1:0",
		S3Export:   &s3export.Config{TLS: &tlsOff, Credentials: []s3export.Credential{{AccessKey: "reader", SecretKey: "reader-secret"}}},
	}
}

func TestS3ExportValidate(t *testing.T) {
	if err := ValidateOutpost(testS3ExportOutpost()); err != nil {
		t.Fatal(err)
	}
	missing := testS3ExportOutpost()
	missing.S3Export = nil
	if err := ValidateOutpost(missing); err == nil || !strings.Contains(err.Error(), "s3_export config is required") {
		t.Fatalf("missing config error = %v", err)
	}
	onAgent := testS3ExportOutpost()
	onAgent.Agent = "host-1"
	if err := ValidateOutpost(onAgent); err != nil {
		t.Fatalf("agent placement rejected: %v", err)
	}
}

func TestS3ExportDriverServesAttachments(t *testing.T) {
	inst, err := (s3ExportDriver{}).Start(t.Context(), testS3ExportOutpost())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Stop() })

	snapshot := memfs.New()
	f, err := snapshot.Create("etc/hostname")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("web01\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := inst.Attach(Attachment{Name: "rw-share", FS: memfs.New()}); err == nil {
		t.Fatal("read-write attachment accepted")
	}
	if err := inst.Attach(Attachment{Name: "Web01_Root", ReadOnly: true, FS: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := inst.AttachSub("batch", "web01", Attachment{Name: "batch", ReadOnly: true, FS: snapshot}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(inst.Attached(), ","); got != "Web01_Root,batch" {
		t.Fatalf("attached = %s", got)
	}

	endpoint := inst.Endpoint("Web01_Root")
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") || !strings.HasSuffix(endpoint, "/web01-root") {
		t.Fatalf("endpoint = %s", endpoint)
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("reader", "reader-secret", ""),
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	for bucket, key := range map[string]string{"web01-root": "etc/hostname", "batch": "web01/etc/hostname"} {
		object, err := client.GetObject(context.Background(), bucket, key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(object)
		if err != nil || string(body) != "web01\n" {
			t.Fatalf("%s/%s = %q, %v", bucket, key, body, err)
		}
	}

	inst.DetachSub("batch", "web01")
	if got := strings.Join(inst.Attached(), ","); got != "Web01_Root" {
		t.Fatalf("attached after detaching the last sub mount = %s", got)
	}
}
