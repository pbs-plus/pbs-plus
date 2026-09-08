//go:build linux

package outpost

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
)

func testS3Outpost() Outpost {
	return Outpost{
		Name:       "s3-test",
		Type:       TypeS3,
		ListenAddr: "127.0.0.1:0",
		CreatedAt:  time.Now().Unix(),
		S3: &objectstore.Config{
			Region: "us-east-1",
			Buckets: []objectstore.Bucket{{
				Name: "backups", Datastore: "store", BackupType: "host", BackupID: "logical",
			}},
			Credentials: []objectstore.Credential{{
				AccessKey: "outpost-access",
				SecretKey: "outpost-secret-key",
				AuthID:    "backup@pbs!s3",
				Grants:    []objectstore.Grant{{Bucket: "backups", Read: true}},
			}},
		},
	}
}

func TestS3OutpostPersistence(t *testing.T) {
	oldPrefix := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() { conf.StatePrefix = oldPrefix })
	configured := testS3Outpost()
	if err := SaveOutpost(configured); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := LoadOutpost(configured.Name)
	if err != nil || !ok {
		t.Fatalf("LoadOutpost() ok = %v, err = %v", ok, err)
	}
	if loaded.S3 == nil || loaded.S3.Credentials[0].SecretKey != "outpost-secret-key" || loaded.S3.Buckets[0].Datastore != "store" {
		t.Fatalf("loaded S3 config = %+v", loaded.S3)
	}
	info, err := os.Stat(filepath.Join(conf.StatePrefix, "outposts", configured.Name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o, want 600", info.Mode().Perm())
	}
}

func TestS3DriverRoundTrip(t *testing.T) {
	configured := testS3Outpost()
	instance, err := (s3Driver{}).Start(t.Context(), configured)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Stop() })

	endpoint, err := url.Parse(instance.Endpoint(""))
	if err != nil {
		t.Fatal(err)
	}
	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("outpost-access", "outpost-secret-key", ""),
		Secure:       false,
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	buckets, err := client.ListBuckets(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 1 || buckets[0].Name != "backups" {
		t.Fatalf("buckets = %+v", buckets)
	}
}
