//go:build linux

package targetapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestInstalledPluginAdminHandlers(t *testing.T) {
	ctx := context.Background()
	db, err := coredb.Initialize(ctx, filepath.Join(t.TempDir(), "plugin-admin.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreatePluginRepository(ctx, coredb.PluginRepository{
		ID: "example.repository", Name: "Example", URL: "https://example.test/index.toml", PublicKey: []byte("key"), Enabled: true,
	}); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}

	root := t.TempDir()
	oldRoot := conf.PluginsBasePath
	conf.PluginsBasePath = root
	t.Cleanup(func() { conf.PluginsBasePath = oldRoot })
	pluginID := "example.plugin"
	for _, version := range []string{"1.0.0", "1.1.0"} {
		directory := filepath.Join(root, pluginID, version)
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := db.RegisterPluginVersion(ctx, "example.repository", coredb.InstalledPluginVersion{
			PluginID: pluginID, Version: version, Platform: "linux/amd64", InstallPath: filepath.Join(directory, "plugin"),
			Manifest: []byte("manifest"), ArtifactSHA256: strings.Repeat("ab", 32), InstalledAt: time.Now(),
			HealthState: coredb.PluginHealthHealthy, HealthCheckedAt: time.Now(),
		}, version == "1.1.0"); err != nil {
			t.Fatalf("RegisterPluginVersion(%s): %v", version, err)
		}
	}
	supervisor, err := targetplugin.NewSupervisor(1, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	app := &application.Runtime{CoreDB: db, PluginSupervisor: supervisor}

	form := url.Values{"enabled": {"false"}}
	request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.SetPathValue("plugin", pluginID)
	response := httptest.NewRecorder()
	ExtJsInstalledPluginHandler(app)(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("disable status = %d: %s", response.Code, response.Body.String())
	}
	installed, err := db.GetInstalledPlugin(ctx, pluginID)
	if err != nil || installed.Enabled {
		t.Fatalf("disabled plugin = %#v, %v", installed, err)
	}

	request = httptest.NewRequest(http.MethodPost, "/", nil)
	request.SetPathValue("plugin", pluginID)
	request.SetPathValue("version", "1.1.0")
	response = httptest.NewRecorder()
	ExtJsInstalledPluginVersionActivateHandler(app)(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("activate status = %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodDelete, "/", nil)
	request.SetPathValue("plugin", pluginID)
	request.SetPathValue("version", "1.0.0")
	response = httptest.NewRecorder()
	ExtJsInstalledPluginVersionHandler(app)(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("uninstall status = %d: %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(root, pluginID, "1.0.0")); !os.IsNotExist(err) {
		t.Fatalf("version directory still exists: %v", err)
	}
}

func TestRepositoryReleaseResponseShowsVerifiedMetadata(t *testing.T) {
	response := newRepositoryReleaseResponse(targetplugin.RepositoryRelease{
		PluginID: "example.plugin", Version: "1.0.0", Publisher: "Example", PublisherKeyFingerprint: "fingerprint",
		MinimumHostVersion: "0.1.0", MaximumHostVersion: "2.0.0", ProtocolVersion: 1,
		TargetTypes: []string{"archive"}, ManifestSHA256: strings.Repeat("ab", 32), Channel: "stable",
		Artifacts: []targetplugin.RepositoryArtifact{{OS: "linux", Arch: "amd64"}},
	})
	if !response.RepositorySignatureVerified || len(response.Platforms) != 1 || response.Platforms[0] != "linux/amd64" || response.ManifestSHA256 == "" {
		t.Fatalf("release response = %#v", response)
	}
}
