//go:build linux

package plugins

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestPluginJobOptionFormRoundTrip(t *testing.T) {
	schema := targetplugin.FormSchema{Version: 2, Fields: []targetplugin.FormField{
		{Key: "scope", Label: "Scope", Control: targetplugin.ControlText, Required: true},
		{Key: "workers", Label: "Workers", Control: targetplugin.ControlInteger},
		{Key: "enabled", Label: "Enabled", Control: targetplugin.ControlBoolean},
	}}
	form := pluginJobForm(map[string][]string{
		"target":                 {"archive"},
		"plugin-options.scope":   {"daily"},
		"plugin-options.workers": {"3"},
		"plugin-options.enabled": {"true"},
	})
	encoded, err := encodePluginJobForm(schema, form)
	if err != nil {
		t.Fatalf("encodePluginJobForm: %v", err)
	}
	data, err := PluginJobOptionFormData(&coredb.PluginJobOptions{Options: encoded})
	if err != nil {
		t.Fatalf("PluginJobOptionFormData: %v", err)
	}
	want := map[string]any{
		"plugin-options.scope":   "daily",
		"plugin-options.workers": int64(3),
		"plugin-options.enabled": true,
	}
	if !reflect.DeepEqual(data, want) {
		t.Fatalf("form data = %#v, want %#v", data, want)
	}
}

func TestPluginJobOptionFormRejectsInvalidFields(t *testing.T) {
	schema := targetplugin.FormSchema{Version: 1, Fields: []targetplugin.FormField{
		{Key: "scope", Label: "Scope", Control: targetplugin.ControlText, Required: true},
	}}
	if _, err := encodePluginJobForm(schema, map[string][]string{"unknown": {"value"}}); err == nil {
		t.Fatal("unknown job option was accepted")
	}
	secretSchema := targetplugin.FormSchema{Version: 1, Fields: []targetplugin.FormField{
		{Key: "token", Label: "Token", Control: targetplugin.ControlSecret, Required: true},
	}}
	if _, err := encodePluginJobForm(secretSchema, map[string][]string{"token": {"secret"}}); err == nil || !strings.Contains(err.Error(), "cannot contain secret") {
		t.Fatalf("secret job option error = %v", err)
	}
}

func TestParsePluginJobOptionsUsesActiveManifest(t *testing.T) {
	ctx := context.Background()
	db, err := coredb.Initialize(ctx, filepath.Join(t.TempDir(), "plugin-job-options.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	descriptor := targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        "org.pbs-plus.job-forms",
		Version:         "1.0.0",
		TargetTypes:     []string{"job-form"},
		TargetSchema:    targetplugin.FormSchema{Version: 1},
		BackupSchema: targetplugin.FormSchema{Version: 2, Fields: []targetplugin.FormField{
			{Key: "scope", Label: "Scope", Control: targetplugin.ControlText, Required: true},
		}},
		RestoreSchema: targetplugin.FormSchema{Version: 3},
	}
	schemaDigest, err := targetplugin.SchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("SchemaDigest: %v", err)
	}
	manifest, err := toml.Marshal(targetplugin.PluginManifest{
		FormatVersion:   targetplugin.ManifestFormatVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		PluginID:        descriptor.PluginID,
		Version:         descriptor.Version,
		TargetTypes:     descriptor.TargetTypes,
		SchemaSHA256:    schemaDigest,
		TargetSchema:    descriptor.TargetSchema,
		BackupSchema:    descriptor.BackupSchema,
		RestoreSchema:   descriptor.RestoreSchema,
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	repository := coredb.PluginRepository{
		ID: "org.pbs-plus.job-form-tests", Name: "Job Form Tests",
		URL: "https://plugins.example.test/index.toml", PublicKey: []byte("key"), Enabled: true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	if err := db.RegisterPluginVersion(ctx, repository.ID, coredb.InstalledPluginVersion{
		PluginID: descriptor.PluginID, Version: descriptor.Version, Platform: "linux/amd64",
		InstallPath: "/plugins/job-forms/1.0.0", Manifest: manifest,
		ArtifactSHA256: strings.Repeat("ab", 32), InstalledAt: time.Unix(1_700_000_000, 0),
		HealthState: coredb.PluginHealthUnknown,
	}, true); err != nil {
		t.Fatalf("RegisterPluginVersion: %v", err)
	}
	emptyConfig, err := targetplugin.MarshalProtocol(targetplugin.Values{})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	if err := db.CreatePluginTarget(ctx, coredb.PluginTarget{
		Name: "archive", PluginID: descriptor.PluginID, PluginVersion: descriptor.Version,
		TargetType: descriptor.TargetTypes[0], SchemaVersion: 1, Config: emptyConfig,
	}, nil); err != nil {
		t.Fatalf("CreatePluginTarget: %v", err)
	}

	options, err := ParseBackupJobOptions(ctx, db, "archive", map[string][]string{
		"store": {"backup"}, "plugin-options.scope": {"daily"},
	})
	if err != nil {
		t.Fatalf("ParseBackupJobOptions: %v", err)
	}
	if options.PluginID != descriptor.PluginID || options.PluginVersion != descriptor.Version || options.SchemaVersion != 2 {
		t.Fatalf("plugin options identity = %#v", options)
	}
	data, err := PluginJobOptionFormData(options)
	if err != nil || data["plugin-options.scope"] != "daily" {
		t.Fatalf("plugin option form data = %#v, %v", data, err)
	}

	legacy, err := ParseBackupJobOptions(ctx, db, "archive", map[string][]string{
		"store": {"backup"}, "database_scope": {"server"}, "schedule": {""},
	})
	if err != nil {
		t.Fatalf("ParseBackupJobOptions legacy form: %v", err)
	}
	legacyData, err := PluginJobOptionFormData(legacy)
	if err != nil || legacyData["plugin-options.scope"] != "server" {
		t.Fatalf("legacy plugin option form data = %#v, %v", legacyData, err)
	}
	if len(legacyData) != 1 {
		t.Fatalf("unrelated un-prefixed fields leaked into plugin options: %#v", legacyData)
	}
}
