package coredb

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPluginRegistryLifecycle(t *testing.T) {
	ctx := context.Background()
	db, err := Initialize(ctx, filepath.Join(t.TempDir(), "plugin-registry.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	repository := PluginRepository{
		ID:        "org.pbs-plus.official",
		Name:      "PBS Plus",
		URL:       "https://plugins.example.test/index.toml",
		PublicKey: []byte("public-key-der"),
		Enabled:   true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	refreshTime := time.Unix(1_700_000_000, 0)
	updated, err := db.RecordPluginRepositoryRefresh(ctx, repository.ID, PluginRepositoryRefresh{
		ETag:         `"index-v1"`,
		LastModified: "Tue, 14 Nov 2023 22:13:20 GMT",
		RefreshedAt:  refreshTime,
	})
	if err != nil || !updated {
		t.Fatalf("RecordPluginRepositoryRefresh = %v, %v", updated, err)
	}
	repository.Name = "PBS Plus Stable"
	repository.URL = "https://plugins.example.test/stable.toml"
	updated, err = db.UpdatePluginRepository(ctx, repository.ID, repository.Name, repository.URL)
	if err != nil || !updated {
		t.Fatalf("UpdatePluginRepository = %v, %v", updated, err)
	}
	storedRepository, err := db.GetPluginRepository(ctx, repository.ID)
	if err != nil {
		t.Fatalf("GetPluginRepository: %v", err)
	}
	if storedRepository.ID != repository.ID || storedRepository.URL != repository.URL || !storedRepository.Enabled ||
		!bytes.Equal(storedRepository.PublicKey, repository.PublicKey) || !storedRepository.LastRefreshedAt.Equal(refreshTime) {
		t.Fatalf("repository = %#v", storedRepository)
	}

	installedAt := time.Unix(1_700_000_100, 0)
	checkedAt := time.Unix(1_700_000_101, 0)
	version := InstalledPluginVersion{
		PluginID:        "org.pbs-plus.filesystem",
		Version:         "1.0.0",
		Platform:        "linux/amd64",
		InstallPath:     "/var/lib/pbs-plus/plugins/org.pbs-plus.filesystem/1.0.0",
		Manifest:        []byte("manifest-v1"),
		ArtifactSHA256:  strings.Repeat("ab", 32),
		InstalledAt:     installedAt,
		HealthState:     PluginHealthHealthy,
		HealthCheckedAt: checkedAt,
	}
	if err := db.RegisterPluginVersion(ctx, repository.ID, version, true); err != nil {
		t.Fatalf("RegisterPluginVersion: %v", err)
	}
	plugin, err := db.GetInstalledPlugin(ctx, version.PluginID)
	if err != nil {
		t.Fatalf("GetInstalledPlugin: %v", err)
	}
	if plugin.ActiveVersion != version.Version || !plugin.Enabled || plugin.RepositoryID != repository.ID {
		t.Fatalf("plugin = %#v", plugin)
	}
	versions, err := db.ListInstalledPluginVersions(ctx, version.PluginID)
	if err != nil {
		t.Fatalf("ListInstalledPluginVersions: %v", err)
	}
	if len(versions) != 1 || !bytes.Equal(versions[0].Manifest, version.Manifest) || !versions[0].InstalledAt.Equal(installedAt) {
		t.Fatalf("versions = %#v", versions)
	}
	storedVersion, err := db.GetInstalledPluginVersion(ctx, version.PluginID, version.Version)
	if err != nil || storedVersion.InstallPath != version.InstallPath {
		t.Fatalf("GetInstalledPluginVersion = %#v, %v", storedVersion, err)
	}

	if deleted, err := db.DeleteInactivePluginVersion(ctx, version.PluginID, version.Version); err != nil || deleted {
		t.Fatalf("DeleteInactivePluginVersion active = %v, %v", deleted, err)
	}
	if activated, err := db.ActivatePluginVersion(ctx, version.PluginID, "missing"); err != nil || activated {
		t.Fatalf("ActivatePluginVersion missing = %v, %v", activated, err)
	}
	if changed, err := db.SetInstalledPluginEnabled(ctx, version.PluginID, false); err != nil || !changed {
		t.Fatalf("SetInstalledPluginEnabled = %v, %v", changed, err)
	}
	unhealthyAt := time.Unix(1_700_000_200, 0)
	if changed, err := db.UpdatePluginVersionHealth(ctx, version.PluginID, version.Version, PluginHealthUnhealthy, "probe failed", unhealthyAt); err != nil || !changed {
		t.Fatalf("UpdatePluginVersionHealth = %v, %v", changed, err)
	}
	plugin, err = db.GetInstalledPlugin(ctx, version.PluginID)
	if err != nil || plugin.Enabled {
		t.Fatalf("disabled plugin = %#v, %v", plugin, err)
	}
	storedVersion, err = db.GetInstalledPluginVersion(ctx, version.PluginID, version.Version)
	if err != nil || storedVersion.HealthState != PluginHealthUnhealthy || storedVersion.HealthMessage != "probe failed" || !storedVersion.HealthCheckedAt.Equal(unhealthyAt) {
		t.Fatalf("unhealthy version = %#v, %v", storedVersion, err)
	}

	if deleted, err := db.DeletePluginRepository(ctx, repository.ID); err == nil || deleted {
		t.Fatalf("DeletePluginRepository with installed plugin = %v, %v", deleted, err)
	}
	if changed, err := db.ClearPluginActivation(ctx, version.PluginID); err != nil || !changed {
		t.Fatalf("ClearPluginActivation = %v, %v", changed, err)
	}
	if deleted, err := db.DeleteInactivePluginVersion(ctx, version.PluginID, version.Version); err != nil || !deleted {
		t.Fatalf("DeleteInactivePluginVersion = %v, %v", deleted, err)
	}
	if deleted, err := db.DeleteEmptyPlugin(ctx, version.PluginID); err != nil || !deleted {
		t.Fatalf("DeleteEmptyPlugin = %v, %v", deleted, err)
	}
	if deleted, err := db.DeletePluginRepository(ctx, repository.ID); err != nil || !deleted {
		t.Fatalf("DeletePluginRepository = %v, %v", deleted, err)
	}
}

func TestRegisterPluginVersionRejectsRepositoryChange(t *testing.T) {
	ctx := context.Background()
	db, err := Initialize(ctx, filepath.Join(t.TempDir(), "plugin-repository-change.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	for _, id := range []string{"org.example.first", "org.example.second"} {
		if err := db.CreatePluginRepository(ctx, PluginRepository{
			ID:        id,
			Name:      id,
			URL:       "https://plugins.example.test/" + id,
			PublicKey: []byte(id),
			Enabled:   true,
		}); err != nil {
			t.Fatalf("CreatePluginRepository(%s): %v", id, err)
		}
	}
	version := InstalledPluginVersion{
		PluginID:       "org.example.plugin",
		Version:        "1.0.0",
		Platform:       "linux/amd64",
		InstallPath:    "/plugins/org.example.plugin/1.0.0",
		Manifest:       []byte("manifest-v1"),
		ArtifactSHA256: strings.Repeat("ab", 32),
		InstalledAt:    time.Unix(1_700_000_000, 0),
		HealthState:    PluginHealthUnknown,
	}
	if err := db.RegisterPluginVersion(ctx, "org.example.first", version, false); err != nil {
		t.Fatalf("RegisterPluginVersion first: %v", err)
	}
	version.Version = "2.0.0"
	version.InstallPath = "/plugins/org.example.plugin/2.0.0"
	version.Manifest = []byte("manifest-v2")
	if err := db.RegisterPluginVersion(ctx, "org.example.second", version, false); err == nil || !strings.Contains(err.Error(), "belongs to repository") {
		t.Fatalf("RegisterPluginVersion repository change error = %v", err)
	}
	versions, err := db.ListInstalledPluginVersions(ctx, version.PluginID)
	if err != nil {
		t.Fatalf("ListInstalledPluginVersions: %v", err)
	}
	if len(versions) != 1 || versions[0].Version != "1.0.0" {
		t.Fatalf("versions = %#v", versions)
	}
}
