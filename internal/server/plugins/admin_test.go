//go:build linux

package plugins

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestAddRepository(t *testing.T) {
	ctx := context.Background()
	db := testStore(t, "admin-add.db")
	key, encoded := testPublicKeyPEM(t)
	fingerprint, err := targetplugin.PublicKeyFingerprint(key)
	if err != nil {
		t.Fatalf("PublicKeyFingerprint: %v", err)
	}
	request := AddRepositoryRequest{
		ID:           "org.pbs-plus.official",
		Name:         "PBS Plus",
		URL:          "https://plugins.example.test/index.toml",
		PublicKeyPEM: encoded,
		Fingerprint:  strings.ToUpper(fingerprint),
	}
	stored, err := AddRepository(ctx, db, request)
	if err != nil || stored != fingerprint {
		t.Fatalf("AddRepository = %q, %v", stored, err)
	}
	repository, err := db.GetPluginRepository(ctx, request.ID)
	if err != nil || !repository.Enabled || len(repository.PublicKey) == 0 {
		t.Fatalf("repository = %#v, %v", repository, err)
	}

	tests := []struct {
		name      string
		mutate    func(*AddRepositoryRequest)
		wantError string
	}{
		{name: "missing name", mutate: func(r *AddRepositoryRequest) { r.Name = "" }, wantError: "identity and name"},
		{name: "plaintext url", mutate: func(r *AddRepositoryRequest) { r.URL = "http://plugins.example.test/index.toml" }, wantError: "absolute HTTPS"},
		{name: "unconfirmed fingerprint", mutate: func(r *AddRepositoryRequest) { r.Fingerprint = "" }, wantError: "must be confirmed"},
		{name: "wrong fingerprint", mutate: func(r *AddRepositoryRequest) { r.Fingerprint = strings.Repeat("ab", 32) }, wantError: "not the confirmed value"},
		{name: "invalid key", mutate: func(r *AddRepositoryRequest) { r.PublicKeyPEM = "not-a-key" }, wantError: "PEM public key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := request
			candidate.ID = "org.pbs-plus." + strings.ReplaceAll(test.name, " ", "-")
			test.mutate(&candidate)
			if _, err := AddRepository(ctx, db, candidate); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("AddRepository error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestRemoveRepositoryAndUninstallVersion(t *testing.T) {
	ctx := context.Background()
	db := testStore(t, "admin-remove.db")
	key, _ := testPublicKeyPEM(t)
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	repository := coredb.PluginRepository{
		ID:        "org.pbs-plus.official",
		Name:      "PBS Plus",
		URL:       "https://plugins.example.test/index.toml",
		PublicKey: der,
		Enabled:   true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}

	root := t.TempDir()
	pluginID := "org.pbs-plus.test"
	for _, version := range []string{"1.0.0", "1.1.0"} {
		directory := filepath.Join(root, pluginID, version)
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := db.RegisterPluginVersion(ctx, repository.ID, coredb.InstalledPluginVersion{
			PluginID:        pluginID,
			Version:         version,
			Platform:        "linux/amd64",
			InstallPath:     filepath.Join(directory, "plugin"),
			Manifest:        []byte("manifest"),
			ArtifactSHA256:  strings.Repeat("ab", 32),
			InstalledAt:     time.Unix(1_700_000_000, 0),
			HealthState:     coredb.PluginHealthHealthy,
			HealthCheckedAt: time.Unix(1_700_000_000, 0),
		}, version == "1.1.0"); err != nil {
			t.Fatalf("RegisterPluginVersion(%s): %v", version, err)
		}
	}

	if err := RemoveRepository(ctx, db, repository.ID); err == nil || !strings.Contains(err.Error(), "is installed from repository") {
		t.Fatalf("RemoveRepository error = %v", err)
	}
	if err := UninstallVersion(ctx, db, root, pluginID, "1.1.0"); err == nil || !strings.Contains(err.Error(), "is active") {
		t.Fatalf("UninstallVersion active error = %v", err)
	}
	if err := UninstallVersion(ctx, db, "plugins", pluginID, "1.0.0"); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("UninstallVersion relative root error = %v", err)
	}
	if err := UninstallVersion(ctx, db, t.TempDir(), pluginID, "1.0.0"); err == nil || !strings.Contains(err.Error(), "is outside") {
		t.Fatalf("UninstallVersion foreign root error = %v", err)
	}

	if err := UninstallVersion(ctx, db, root, pluginID, "1.0.0"); err != nil {
		t.Fatalf("UninstallVersion: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, pluginID, "1.0.0")); !os.IsNotExist(err) {
		t.Fatalf("version directory still exists: %v", err)
	}
	versions, err := db.ListInstalledPluginVersions(ctx, pluginID)
	if err != nil {
		t.Fatalf("ListInstalledPluginVersions: %v", err)
	}
	if len(versions) != 1 || versions[0].Version != "1.1.0" {
		t.Fatalf("versions = %#v", versions)
	}

	if _, err := db.ClearPluginActivation(ctx, pluginID); err != nil {
		t.Fatalf("ClearPluginActivation: %v", err)
	}
	if err := UninstallVersion(ctx, db, root, pluginID, "1.1.0"); err != nil {
		t.Fatalf("UninstallVersion remaining: %v", err)
	}
	if err := RemoveRepository(ctx, db, repository.ID); err != nil {
		t.Fatalf("RemoveRepository: %v", err)
	}
	if err := RemoveRepository(ctx, db, repository.ID); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("RemoveRepository missing error = %v", err)
	}
}

func testPublicKeyPEM(t *testing.T) (*ecdsa.PublicKey, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	return &key.PublicKey, string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}
