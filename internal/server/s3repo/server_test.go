//go:build linux

package s3repo

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
)

func testRepository() Repository {
	tlsDisabled := false
	return Repository{
		Name:       "s3-test",
		ListenAddr: "127.0.0.1:0",
		CreatedAt:  time.Now().Unix(),
		Config: objectstore.Config{
			Region: "us-east-1",
			TLS:    &tlsDisabled,
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

func TestS3DriverRejectsMissingTLSCertificate(t *testing.T) {
	configured := testRepository()
	tlsEnabled := true
	configured.Config.TLS = &tlsEnabled
	configured.Config.TLSCertFile = filepath.Join(t.TempDir(), "missing.crt")
	configured.Config.TLSKeyFile = filepath.Join(t.TempDir(), "missing.key")

	_, err := start(t.Context(), configured)
	if err == nil || !strings.Contains(err.Error(), "s3 repository tls") {
		t.Fatalf("Start() error = %v, want synchronous TLS certificate error", err)
	}
}

func TestS3DriverTLSRoundTrip(t *testing.T) {
	certPath := filepath.Join(t.TempDir(), "server.crt")
	keyPath := filepath.Join(t.TempDir(), "server.key")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	configured := testRepository()
	tlsEnabled := true
	configured.Config.TLS = &tlsEnabled
	configured.Config.TLSCertFile = certPath
	configured.Config.TLSKeyFile = keyPath
	instance, err := start(t.Context(), configured)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.stop() })

	if !strings.HasPrefix(instance.endpoint(""), "https://") {
		t.Fatalf("endpoint = %q", instance.endpoint(""))
	}
	endpoint, err := url.Parse(instance.endpoint(""))
	if err != nil {
		t.Fatal(err)
	}
	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("outpost-access", "outpost-secret-key", ""),
		Secure:       true,
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
		},
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

func TestS3OutpostPersistence(t *testing.T) {
	oldPrefix := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() { conf.StatePrefix = oldPrefix })
	configured := testRepository()
	if err := Save(configured); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := Load(configured.Name)
	if err != nil || !ok {
		t.Fatalf("Load() ok = %v, err = %v", ok, err)
	}
	if loaded.Config.Credentials[0].SecretKey != "outpost-secret-key" || loaded.Config.Buckets[0].Datastore != "store" {
		t.Fatalf("loaded config = %+v", loaded.Config)
	}
	info, err := os.Stat(filepath.Join(conf.StatePrefix, "s3-repositories", configured.Name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o, want 600", info.Mode().Perm())
	}
}

func TestS3DriverRoundTrip(t *testing.T) {
	configured := testRepository()
	instance, err := start(t.Context(), configured)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.stop() })

	endpoint, err := url.Parse(instance.endpoint(""))
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
