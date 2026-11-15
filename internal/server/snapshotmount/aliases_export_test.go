//go:build linux

package snapshotmount

import (
	"io"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
	"github.com/pbs-plus/pbs-plus/internal/server/s3export"
)

func TestSharedAliasesReleaseOnceForNFSAndS3(t *testing.T) {
	old := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() { conf.StatePrefix = old })
	for _, kind := range []string{outpost.TypeNFS, outpost.TypeS3Export} {
		t.Run(kind, func(t *testing.T) {
			tlsOff := false
			o := outpost.Outpost{Name: "layout-" + kind, Type: kind, ListenAddr: "127.0.0.1:0"}
			if kind == outpost.TypeS3Export {
				o.S3Export = &s3export.Config{TLS: &tlsOff, Credentials: []s3export.Credential{{AccessKey: "reader", SecretKey: "reader-secret"}}}
			}
			if err := outpost.ApplyConfig(t.Context(), o); err != nil {
				t.Fatal(err)
			}
			defer outpost.StopOutpost(o.Name)
			source := memfs.New()
			f, err := source.Create("contents.txt")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write([]byte("shared source")); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			var releases atomic.Int64
			s := Session{Outpost: o.Name, ShareName: "collection", SubPath: "Project A/D-100", Aliases: []string{"Project B/D-100"}}
			att := outpost.Attachment{Name: "collection", ReadOnly: true, FS: source, Release: func() { releases.Add(1) }}
			if err := attachSessionPaths(s, "collection", att); err != nil {
				t.Fatal(err)
			}
			if err := outpost.AttachSub(o.Name, "collection", s.SubPath, att); err == nil {
				t.Fatal("overwrote another attachment")
			}
			if err := outpost.AttachSub(o.Name, "collection", "Project A", att); err == nil {
				t.Fatal("overlapping attachment accepted")
			}
			if kind == outpost.TypeS3Export {
				u, err := url.Parse(outpost.EndpointOf(o.Name, "collection"))
				if err != nil {
					t.Fatal(err)
				}
				client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4("reader", "reader-secret", ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
				if err != nil {
					t.Fatal(err)
				}
				for _, sub := range sessionPaths(s) {
					object, err := client.GetObject(t.Context(), "collection", sub+"/contents.txt", minio.GetObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(object)
					if err != nil {
						t.Fatal(err)
					}
					if err := object.Close(); err != nil {
						t.Fatal(err)
					}
					if string(body) != "shared source" {
						t.Fatalf("alias %s contents = %q", sub, body)
					}
				}
			}
			var failedReleases atomic.Int64
			other := Session{Outpost: o.Name, ShareName: "collection", SubPath: "Other/D-200", Aliases: []string{s.SubPath}}
			failed := outpost.Attachment{Name: "collection", ReadOnly: true, FS: source, Release: func() { failedReleases.Add(1) }}
			if err := attachSessionPaths(other, "collection", failed); err == nil {
				t.Fatal("partial fanout overwrote an existing attachment")
			}
			if failedReleases.Load() != 1 || releases.Load() != 0 {
				t.Fatal("failed fanout leaked its source or closed the existing source")
			}
			outpost.DetachSub(o.Name, "collection", s.SubPath)
			if got := releases.Load(); got != 0 {
				t.Fatalf("closed source while alias remained: %d", got)
			}
			outpost.StopOutpost(o.Name)
			if got := releases.Load(); got != 1 {
				t.Fatalf("source releases = %d, want 1", got)
			}
		})
	}
}
