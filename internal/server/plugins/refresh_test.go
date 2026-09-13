//go:build linux

package plugins

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestRefresh(t *testing.T) {
	ctx := context.Background()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	index := testIndex(t, key, "org.pbs-plus.official")
	server := indexServer(t, key, index)
	defer server.Close()

	db := testStore(t, "refresh.db")
	repository := testRepository(t, key, "org.pbs-plus.official", server.URL+"/official.toml")
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}

	fetcher := targetplugin.Fetcher{Client: server.Client()}
	parsed, changed, err := Refresh(ctx, db, fetcher, repository.ID)
	if err != nil || !changed {
		t.Fatalf("Refresh = %v, %v", changed, err)
	}
	if len(parsed.Releases) != 1 || parsed.Releases[0].PluginID != "org.pbs-plus.test" {
		t.Fatalf("index = %#v", parsed)
	}
	stored, err := db.GetPluginRepository(ctx, repository.ID)
	if err != nil {
		t.Fatalf("GetPluginRepository: %v", err)
	}
	if stored.ETag != `"v1"` || stored.LastError != "" || stored.LastRefreshedAt.IsZero() {
		t.Fatalf("stored = %#v", stored)
	}

	if _, changed, err = Refresh(ctx, db, fetcher, repository.ID); err != nil || changed {
		t.Fatalf("conditional Refresh = %v, %v", changed, err)
	}
	stored, err = db.GetPluginRepository(ctx, repository.ID)
	if err != nil {
		t.Fatalf("GetPluginRepository: %v", err)
	}
	if stored.ETag != `"v1"` || stored.LastError != "" {
		t.Fatalf("stored after conditional refresh = %#v", stored)
	}
}

func TestRefreshRecordsFailures(t *testing.T) {
	ctx := context.Background()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	foreign, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := indexServer(t, key, testIndex(t, key, "org.pbs-plus.other"))
	defer server.Close()

	db := testStore(t, "refresh-failure.db")
	tests := []struct {
		name       string
		id         string
		signingKey *ecdsa.PrivateKey
		disabled   bool
		wantError  string
	}{
		{name: "identity mismatch", id: "org.pbs-plus.official", signingKey: key, wantError: "identifies"},
		{name: "foreign key", id: "org.pbs-plus.other", signingKey: foreign, wantError: "verify repository index"},
		{name: "disabled repository", id: "org.pbs-plus.disabled", signingKey: key, disabled: true, wantError: "disabled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := testRepository(t, test.signingKey, test.id, server.URL+"/"+test.id+".toml")
			repository.Enabled = !test.disabled
			if err := db.CreatePluginRepository(ctx, repository); err != nil {
				t.Fatalf("CreatePluginRepository: %v", err)
			}
			_, _, err := Refresh(ctx, db, targetplugin.Fetcher{Client: server.Client()}, repository.ID)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Refresh error = %v, want %q", err, test.wantError)
			}
			stored, err := db.GetPluginRepository(ctx, repository.ID)
			if err != nil {
				t.Fatalf("GetPluginRepository: %v", err)
			}
			if test.disabled {
				if stored.LastError != "" {
					t.Fatalf("disabled repository recorded %q", stored.LastError)
				}
				return
			}
			if !strings.Contains(stored.LastError, test.wantError) {
				t.Fatalf("stored last error = %q", stored.LastError)
			}
		})
	}
}

func testStore(t *testing.T, name string) *coredb.Store {
	t.Helper()
	db, err := coredb.Initialize(context.Background(), filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testRepository(t *testing.T, key *ecdsa.PrivateKey, id, url string) coredb.PluginRepository {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	return coredb.PluginRepository{
		ID:        id,
		Name:      id,
		URL:       url,
		PublicKey: der,
		Enabled:   true,
	}
}

func testIndex(t *testing.T, key *ecdsa.PrivateKey, repositoryID string) string {
	t.Helper()
	digest := strings.Repeat("ab", 32)
	artifactSignature := signBase64(t, key, []byte("artifact-bytes"))
	return fmt.Sprintf(`format_version = %d
repository_id = %q

[[release]]
plugin_id = "org.pbs-plus.test"
version = "1.0.0"
publisher = "PBS Plus"
publisher_key_fingerprint = %q
minimum_host_version = "0.1.0"
maximum_host_version = "9.9.9"
protocol = %d
target_types = ["test"]
manifest_url = "manifest.toml"
manifest_sha256 = %q
channel = "stable"

[[release.artifact]]
os = "linux"
arch = "amd64"
url = "artifacts/plugin"
size = 1024
sha256 = %q
signature = %q
`, targetplugin.RepositoryFormatVersion, repositoryID, digest, targetplugin.CurrentProtocolVersion, digest, digest, artifactSignature)
}

func signBase64(t *testing.T, key *ecdsa.PrivateKey, message []byte) string {
	t.Helper()
	digest := sha256.Sum256(message)
	signature, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("SignASN1: %v", err)
	}
	return base64.StdEncoding.EncodeToString(signature)
}

func indexServer(t *testing.T, key *ecdsa.PrivateKey, index string) *httptest.Server {
	t.Helper()
	encoded := signBase64(t, key, []byte(index))
	return httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, ".sig") {
			_, _ = io.WriteString(writer, encoded)
			return
		}
		if request.Header.Get("If-None-Match") == `"v1"` {
			writer.WriteHeader(http.StatusNotModified)
			return
		}
		writer.Header().Set("ETag", `"v1"`)
		_, _ = io.WriteString(writer, index)
	}))
}
