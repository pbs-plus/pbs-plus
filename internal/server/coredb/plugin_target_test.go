package coredb

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestPluginTargetPersistence(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	crypto.SetSealKeyPath(filepath.Join(directory, "secrets.key"))
	t.Cleanup(func() { crypto.SetSealKeyPath(conf.SecretsKeyPath) })

	databasePath := filepath.Join(directory, "plugin-target.db")
	db, err := Initialize(ctx, databasePath)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	repository := PluginRepository{
		ID:        "org.pbs-plus.tests",
		Name:      "PBS Plus Tests",
		URL:       "https://plugins.example.test/index.toml",
		PublicKey: []byte("test-key"),
		Enabled:   true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	version := InstalledPluginVersion{
		PluginID:       "org.pbs-plus.external",
		Version:        "1.0.0",
		Platform:       "linux/amd64",
		InstallPath:    "/plugins/org.pbs-plus.external/1.0.0",
		Manifest:       []byte("manifest"),
		ArtifactSHA256: strings.Repeat("ab", 32),
		InstalledAt:    time.Unix(1_700_000_000, 0),
		HealthState:    PluginHealthUnknown,
	}
	if err := db.RegisterPluginVersion(ctx, repository.ID, version, true); err != nil {
		t.Fatalf("RegisterPluginVersion: %v", err)
	}

	config := pluginTargetConfig(t, "/srv/source")
	target := PluginTarget{
		Name:          "external-target",
		PluginID:      version.PluginID,
		PluginVersion: version.Version,
		TargetType:    "external-filesystem",
		SchemaVersion: 1,
		Config:        config,
		UpdatedAt:     time.Unix(1_700_000_100, 0),
	}
	if err := db.CreatePluginTarget(ctx, target, map[string][]byte{
		"password": []byte("first-secret"),
		"token":    []byte("remove-me"),
	}); err != nil {
		t.Fatalf("CreatePluginTarget: %v", err)
	}

	stored, err := db.GetPluginTarget(ctx, target.Name)
	if err != nil {
		t.Fatalf("GetPluginTarget: %v", err)
	}
	if stored.PluginID != target.PluginID || stored.PluginVersion != target.PluginVersion ||
		stored.SchemaVersion != target.SchemaVersion || !bytes.Equal(stored.Config, target.Config) ||
		len(stored.SecretFields) != 2 || stored.SecretFields[0] != "password" || stored.SecretFields[1] != "token" {
		t.Fatalf("stored target = %#v", stored)
	}
	if bytes.Contains(stored.Config, []byte("first-secret")) {
		t.Fatal("config contains plaintext secret")
	}
	secrets, err := db.ResolvePluginTargetSecrets(ctx, target.Name)
	if err != nil {
		t.Fatalf("ResolvePluginTargetSecrets: %v", err)
	}
	if string(secrets["password"]) != "first-secret" || string(secrets["token"]) != "remove-me" {
		t.Fatalf("resolved secrets = %#v", secrets)
	}
	var encrypted string
	if err := db.Reader().QueryRowContext(ctx,
		"SELECT encrypted_value FROM plugin_target_secrets WHERE target_name = ? AND field_key = ?",
		target.Name, "password").Scan(&encrypted); err != nil {
		t.Fatalf("read encrypted secret: %v", err)
	}
	if encrypted == "first-secret" || strings.Contains(encrypted, "first-secret") {
		t.Fatal("secret was stored as plaintext")
	}

	target.Config = pluginTargetConfig(t, "/srv/updated")
	target.UpdatedAt = time.Unix(1_700_000_200, 0)
	if err := db.UpdatePluginTarget(ctx, target, map[string][]byte{
		"password": []byte("rotated-secret"),
	}, []string{"token"}); err != nil {
		t.Fatalf("UpdatePluginTarget: %v", err)
	}
	secrets, err = db.ResolvePluginTargetSecrets(ctx, target.Name)
	if err != nil {
		t.Fatalf("ResolvePluginTargetSecrets after update: %v", err)
	}
	if len(secrets) != 1 || string(secrets["password"]) != "rotated-secret" {
		t.Fatalf("updated secrets = %#v", secrets)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err = Initialize(ctx, databasePath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	stored, err = db.GetPluginTarget(ctx, target.Name)
	if err != nil || !bytes.Equal(stored.Config, target.Config) || stored.SecretFields[0] != "password" {
		t.Fatalf("reopened target = %#v, %v", stored, err)
	}
	secrets, err = db.ResolvePluginTargetSecrets(ctx, target.Name)
	if err != nil || string(secrets["password"]) != "rotated-secret" {
		t.Fatalf("reopened secrets = %#v, %v", secrets, err)
	}
	if cleared, err := db.ClearPluginActivation(ctx, version.PluginID); err != nil || !cleared {
		t.Fatalf("ClearPluginActivation = %v, %v", cleared, err)
	}
	if deleted, err := db.DeleteInactivePluginVersion(ctx, version.PluginID, version.Version); err == nil || deleted {
		t.Fatalf("DeleteInactivePluginVersion with target = %v, %v", deleted, err)
	}
	if err := db.DeleteTarget(nil, target.Name); err != nil {
		t.Fatalf("DeleteTarget: %v", err)
	}
	if _, err := db.GetPluginTarget(ctx, target.Name); err != ErrTargetNotFound {
		t.Fatalf("GetPluginTarget deleted error = %v", err)
	}
}

func pluginTargetConfig(t *testing.T, path string) []byte {
	t.Helper()
	config, err := targetplugin.MarshalProtocol(targetplugin.Values{
		"path": targetplugin.NewStringScalar(path),
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	return config
}
