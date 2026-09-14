//go:build linux

package plugins

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	pbscrypto "github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const (
	lifecyclePluginID     = "org.pbs-plus.lifecycle"
	lifecycleRepositoryID = "org.pbs-plus.tests"
)

func TestPluginLifecycle(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	pbscrypto.SetSealKeyPath(filepath.Join(directory, "secrets.key"))
	t.Cleanup(func() { pbscrypto.SetSealKeyPath(conf.SecretsKeyPath) })
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	versions := []string{"1.0.0", "1.1.0"}
	server, index := releaseServer(t, key, versions)
	defer server.Close()

	dbPath := filepath.Join(directory, "lifecycle.db")
	db, err := coredb.Initialize(ctx, dbPath)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	if err := db.CreatePluginRepository(ctx, coredb.PluginRepository{
		ID:        lifecycleRepositoryID,
		Name:      "PBS Plus Tests",
		URL:       server.URL + "/index.toml",
		PublicKey: der,
		Enabled:   true,
	}); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}

	fetcher := targetplugin.Fetcher{Client: server.Client()}
	refreshed, changed, err := Refresh(ctx, db, fetcher, lifecycleRepositoryID)
	if err != nil || !changed || len(refreshed.Releases) != len(versions) {
		t.Fatalf("Refresh = %v, %v, %v", len(refreshed.Releases), changed, err)
	}

	supervisor, err := targetplugin.NewSupervisor(2, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	root := t.TempDir()
	for _, version := range versions {
		resolved, err := resolveRelease(index, lifecyclePluginID, version)
		if err != nil {
			t.Fatalf("resolveRelease(%s): %v", version, err)
		}
		installed, err := installRelease(ctx, fetcher, root, server.URL+"/index.toml", resolved, &key.PublicKey)
		if err != nil {
			t.Fatalf("installRelease(%s): %v", version, err)
		}
		if err := registerVersion(ctx, db, supervisor, lifecycleRepositoryID, resolved, installed, true); err != nil {
			t.Fatalf("registerVersion(%s): %v", version, err)
		}
	}

	plugin, err := db.GetInstalledPlugin(ctx, lifecyclePluginID)
	if err != nil || plugin.ActiveVersion != "1.1.0" {
		t.Fatalf("upgraded plugin = %#v, %v", plugin, err)
	}

	active, err := db.GetInstalledPluginVersion(ctx, lifecyclePluginID, plugin.ActiveVersion)
	if err != nil {
		t.Fatalf("GetInstalledPluginVersion: %v", err)
	}
	var events []targetplugin.HostEvent
	err = supervisor.Run(ctx, lifecyclePluginID, active.InstallPath, func(runCtx context.Context, process *targetplugin.Process) error {
		process.SetEventSink(func(event targetplugin.HostEvent) error {
			events = append(events, event)
			return nil
		})
		request := targetplugin.TargetProbeRequest{
			Operation: targetplugin.Operation{
				ProtocolVersion:   targetplugin.CurrentProtocolVersion,
				ID:                "probe-1",
				IdempotencyKey:    "probe-1",
				DeadlineUnixMilli: time.Now().Add(time.Minute).UnixMilli(),
				PluginVersion:     active.Version,
				TargetType:        "lifecycle",
				SchemaVersion:     1,
				BrokerToken:       process.BrokerToken(),
			},
			Target: targetplugin.TargetInput{
				Config:  targetplugin.Values{"path": targetplugin.NewStringScalar("/data")},
				Secrets: targetplugin.Secrets{"credential": []byte("secret")},
			},
		}
		var response targetplugin.TargetProbeResponse
		if err := process.Invoke(runCtx, targetplugin.MethodTargetProbe, request, &response); err != nil {
			return err
		}
		if !response.Available {
			return fmt.Errorf("probe reported %q", response.Message)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("supervisor Run: %v", err)
	}
	if len(events) != 1 || events[0].Message != "probe complete" {
		t.Fatalf("events = %#v", events)
	}

	healthy, err := CheckHealth(ctx, db, supervisor, lifecyclePluginID, active.Version)
	if err != nil || !healthy {
		t.Fatalf("CheckHealth = %v, %v", healthy, err)
	}
	checked, err := db.GetInstalledPluginVersion(ctx, lifecyclePluginID, active.Version)
	if err != nil || checked.HealthState != coredb.PluginHealthHealthy || checked.HealthCheckedAt.IsZero() {
		t.Fatalf("checked version = %#v, %v", checked, err)
	}
	if _, err := CheckHealth(ctx, db, supervisor, lifecyclePluginID, "9.9.9"); err == nil {
		t.Fatal("CheckHealth succeeded for a missing version")
	}

	targetTypes, err := ListTargetTypes(ctx, db)
	if err != nil || len(targetTypes) != 1 || targetTypes[0].TargetType != "lifecycle" {
		t.Fatalf("ListTargetTypes = %#v, %v", targetTypes, err)
	}
	form := map[string][]string{"path": {"/data"}, "credential": {"secret"}}
	if err := CreateTarget(ctx, db, supervisor, "plugin-target", lifecyclePluginID, "lifecycle", form); err != nil {
		t.Fatalf("CreateTarget: %v", err)
	}
	pluginTarget, err := db.GetPluginTarget(ctx, "plugin-target")
	if err != nil || pluginTarget.PluginVersion != "1.1.0" || pluginTarget.TargetType != "lifecycle" || len(pluginTarget.SecretFields) != 1 || pluginTarget.SecretFields[0] != "credential" {
		t.Fatalf("plugin target = %#v, %v", pluginTarget, err)
	}
	optionValues := lifecycleValues(t, targetplugin.Values{"policy": targetplugin.NewStringScalar("full")})
	lease, err := OpenBackup(ctx, db, supervisor, "plugin-target", "backup-job", "execution-id", &coredb.PluginJobOptions{
		JobID: "backup-job", PluginID: lifecyclePluginID, PluginVersion: "1.1.0", SchemaVersion: 2, Options: optionValues,
	}, nil)
	if err != nil {
		t.Fatalf("OpenBackup: %v", err)
	}
	leasePath := lease.Path
	if data, err := os.ReadFile(filepath.Join(leasePath, "payload")); err != nil || string(data) != "plugin backup" {
		t.Fatalf("backup payload = %q, %v", data, err)
	}
	if !lease.Supports(targetplugin.FeatureExclusions) || lease.Supports(targetplugin.FeatureXattrs) {
		t.Fatalf("backup features = %#v", lease.HostFeatures)
	}
	metadataPath := lease.MetadataSourcePath
	encodedMetadata, err := os.ReadFile(filepath.Join(metadataPath, targetplugin.SnapshotMetadataFileName))
	if err != nil {
		t.Fatalf("read snapshot metadata: %v", err)
	}
	var metadata targetplugin.SnapshotMetadata
	if err := targetplugin.UnmarshalProtocol(encodedMetadata, &metadata); err != nil {
		t.Fatalf("decode snapshot metadata: %v", err)
	}
	if metadata.PluginID != lifecyclePluginID || metadata.PluginVersion != "1.1.0" || metadata.TargetType != "lifecycle" || metadata.TargetSchemaVersion != 2 || metadata.BackupSchemaVersion != 2 || metadata.Archive.Type != "lifecycle" {
		t.Fatalf("snapshot metadata = %#v", metadata)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close backup lease: %v", err)
	}
	if _, err := os.Stat(leasePath); !os.IsNotExist(err) {
		t.Fatalf("backup workspace survived close: %v", err)
	}
	if _, err := os.Stat(metadataPath); !os.IsNotExist(err) {
		t.Fatalf("backup metadata survived close: %v", err)
	}

	archive := targetplugin.Archive{Type: "lifecycle", FormatVersion: 1}
	newRestoreOptions := func(jobID, mode string) *coredb.PluginJobOptions {
		return &coredb.PluginJobOptions{
			JobID: jobID, PluginID: lifecyclePluginID, PluginVersion: "1.1.0", SchemaVersion: 2,
			Options: lifecycleValues(t, targetplugin.Values{"mode": targetplugin.NewStringScalar(mode)}),
		}
	}
	pathLease, err := OpenRestore(ctx, db, supervisor, "plugin-target", "restore-path", "execution-path", "attempt-path", archive, newRestoreOptions("restore-path", "path"), nil, nil)
	if err != nil {
		t.Fatalf("OpenRestore path: %v", err)
	}
	pathWorkspace := pathLease.StagingPath()
	if err := os.WriteFile(filepath.Join(pathLease.Path, "restored"), []byte("restored"), 0o600); err != nil {
		t.Fatalf("write path restore payload: %v", err)
	}
	if err := pathLease.Close(); err != nil {
		t.Fatalf("close path restore lease: %v", err)
	}
	if _, err := os.Stat(pathWorkspace); !os.IsNotExist(err) {
		t.Fatalf("path restore workspace survived close: %v", err)
	}

	structuredLease, err := OpenRestore(ctx, db, supervisor, "plugin-target", "restore-structured", "execution-structured", "attempt-structured", archive, newRestoreOptions("restore-structured", "structured"), nil, nil)
	if err != nil {
		t.Fatalf("OpenRestore structured: %v", err)
	}
	if err := os.WriteFile(filepath.Join(structuredLease.StagingPath(), "payload"), []byte("plugin restore"), 0o600); err != nil {
		t.Fatalf("stage structured restore payload: %v", err)
	}
	if err := structuredLease.Consume(ctx); err != nil {
		t.Fatalf("Consume structured restore: %v", err)
	}
	if err := structuredLease.Consume(ctx); err == nil || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("second Consume error = %v", err)
	}
	if err := structuredLease.Close(); err != nil {
		t.Fatalf("close structured restore lease: %v", err)
	}

	var agentRequest targetplugin.HostAgentRestoreRequest
	agentLease, err := OpenRestore(ctx, db, supervisor, "plugin-target", "restore-agent", "execution-agent", "attempt-agent", archive, newRestoreOptions("restore-agent", "agent"), nil, func(_ context.Context, request targetplugin.HostAgentRestoreRequest) error {
		agentRequest = request
		return nil
	})
	if err != nil {
		t.Fatalf("OpenRestore agent: %v", err)
	}
	if agentRequest.Hostname != "agent.example" || agentRequest.VolumeID != "disk-1" || agentRequest.DestinationPath != "/restore" {
		t.Fatalf("agent restore request = %#v", agentRequest)
	}
	if err := agentLease.Close(); err != nil {
		t.Fatalf("close agent restore lease: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close before restart: %v", err)
	}
	db, err = coredb.Initialize(ctx, dbPath)
	if err != nil {
		t.Fatalf("Initialize after restart: %v", err)
	}
	pluginTarget, err = db.GetPluginTarget(ctx, "plugin-target")
	if err != nil || len(pluginTarget.SecretFields) != 1 || pluginTarget.SecretFields[0] != "credential" {
		t.Fatalf("restarted plugin target = %#v, %v", pluginTarget, err)
	}
	probe, err := ProbeTarget(ctx, db, supervisor, "plugin-target")
	if err != nil || !probe.Available || probe.Size == nil || probe.Size.Total != 10 {
		t.Fatalf("ProbeTarget = %#v, %v", probe, err)
	}
	if err := UpdateTarget(ctx, db, supervisor, "plugin-target", map[string][]string{"path": {"/data"}}, nil); err != nil {
		t.Fatalf("UpdateTarget: %v", err)
	}
	if err := db.DeleteTarget(nil, "plugin-target"); err != nil {
		t.Fatalf("DeleteTarget: %v", err)
	}

	if err := ActivateVersion(ctx, db, supervisor, lifecyclePluginID, "1.0.0"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if disabled, err := db.SetInstalledPluginEnabled(ctx, lifecyclePluginID, false); err != nil || !disabled {
		t.Fatalf("disable = %v, %v", disabled, err)
	}
	if err := UninstallVersion(ctx, db, root, lifecyclePluginID, "1.1.0"); err != nil {
		t.Fatalf("UninstallVersion: %v", err)
	}
	if _, err := db.ClearPluginActivation(ctx, lifecyclePluginID); err != nil {
		t.Fatalf("ClearPluginActivation: %v", err)
	}
	if err := UninstallVersion(ctx, db, root, lifecyclePluginID, "1.0.0"); err != nil {
		t.Fatalf("UninstallVersion remaining: %v", err)
	}
	if _, err := db.GetInstalledPlugin(ctx, lifecyclePluginID); err == nil {
		t.Fatal("plugin row survived uninstall")
	}
	if err := RemoveRepository(ctx, db, lifecycleRepositoryID); err != nil {
		t.Fatalf("RemoveRepository: %v", err)
	}
}

func TestActivateVersionMigratesStoredRecords(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	pbscrypto.SetSealKeyPath(filepath.Join(directory, "secrets.key"))
	t.Cleanup(func() { pbscrypto.SetSealKeyPath(conf.SecretsKeyPath) })
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server, index := releaseServer(t, key, []string{"1.0.0", "1.1.0", "1.2.0"})
	defer server.Close()
	db, err := coredb.Initialize(ctx, filepath.Join(directory, "migration.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	if err := db.CreatePluginRepository(ctx, coredb.PluginRepository{
		ID: lifecycleRepositoryID, Name: "PBS Plus Tests", URL: server.URL + "/index.toml", PublicKey: der, Enabled: true,
	}); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	supervisor, err := targetplugin.NewSupervisor(2, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	fetcher := targetplugin.Fetcher{Client: server.Client()}
	root := t.TempDir()
	install := func(version string) error {
		resolved, err := resolveRelease(index, lifecyclePluginID, version)
		if err != nil {
			return err
		}
		installed, err := installRelease(ctx, fetcher, root, server.URL+"/index.toml", resolved, &key.PublicKey)
		if err != nil {
			return err
		}
		return registerVersion(ctx, db, supervisor, lifecycleRepositoryID, resolved, installed, true)
	}
	if err := install("1.0.0"); err != nil {
		t.Fatalf("install 1.0.0: %v", err)
	}
	config := lifecycleValues(t, targetplugin.Values{"path": targetplugin.NewStringScalar("/data")})
	if err := db.CreatePluginTarget(ctx, coredb.PluginTarget{
		Name: "migrated-target", PluginID: lifecyclePluginID, PluginVersion: "1.0.0", TargetType: "lifecycle", SchemaVersion: 1, Config: config,
	}, map[string][]byte{"token": []byte("secret")}); err != nil {
		t.Fatalf("CreatePluginTarget: %v", err)
	}
	if err := db.CreateBackup(nil, coredb.Backup{
		ID: "migrated-backup", Store: "store", Target: coredb.Target{Name: "migrated-target"},
		PluginOptions: &coredb.PluginJobOptions{PluginID: lifecyclePluginID, PluginVersion: "1.0.0", SchemaVersion: 1,
			Options: lifecycleValues(t, targetplugin.Values{"policy": targetplugin.NewStringScalar("daily")})},
	}); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := db.CreateRestore(nil, coredb.Restore{
		ID: "migrated-restore", Store: "store", Snapshot: "host/vm/100/2026-01-01T00:00:00Z", SrcPath: "/",
		DestTarget: coredb.Target{Name: "migrated-target"},
		PluginOptions: &coredb.PluginJobOptions{PluginID: lifecyclePluginID, PluginVersion: "1.0.0", SchemaVersion: 1,
			Options: lifecycleValues(t, targetplugin.Values{"mode": targetplugin.NewStringScalar("replace")})},
	}); err != nil {
		t.Fatalf("CreateRestore: %v", err)
	}

	if err := install("1.1.0"); err != nil {
		t.Fatalf("install 1.1.0: %v", err)
	}
	assertLifecycleMigrationState(t, ctx, db, "1.1.0", 2, "credential", 1)

	if err := install("1.2.0"); err == nil || !strings.Contains(err.Error(), "test migration failure") {
		t.Fatalf("install 1.2.0 error = %v", err)
	}
	assertLifecycleMigrationState(t, ctx, db, "1.1.0", 2, "credential", 1)

	if err := ActivateVersion(ctx, db, supervisor, lifecyclePluginID, "1.0.0"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	assertLifecycleMigrationState(t, ctx, db, "1.0.0", 1, "token", 2)
}

func assertLifecycleMigrationState(t *testing.T, ctx context.Context, db *coredb.Store, version string, schemaVersion uint32, secretField string, historyLength int) {
	t.Helper()
	plugin, err := db.GetInstalledPlugin(ctx, lifecyclePluginID)
	if err != nil || plugin.ActiveVersion != version {
		t.Fatalf("active plugin = %#v, %v", plugin, err)
	}
	target, err := db.GetPluginTarget(ctx, "migrated-target")
	if err != nil || target.PluginVersion != version || target.SchemaVersion != schemaVersion || !slices.Equal(target.SecretFields, []string{secretField}) {
		t.Fatalf("migrated target = %#v, %v", target, err)
	}
	secrets, err := db.ResolvePluginTargetSecrets(ctx, target.Name)
	if err != nil || string(secrets[secretField]) != "secret" || len(secrets) != 1 {
		t.Fatalf("migrated secrets = %#v, %v", secrets, err)
	}
	backup, err := db.GetBackupPluginOptions(ctx, "migrated-backup")
	if err != nil || backup.PluginVersion != version || backup.SchemaVersion != schemaVersion {
		t.Fatalf("migrated backup options = %#v, %v", backup, err)
	}
	restore, err := db.GetRestorePluginOptions(ctx, "migrated-restore")
	if err != nil || restore.PluginVersion != version || restore.SchemaVersion != schemaVersion {
		t.Fatalf("migrated restore options = %#v, %v", restore, err)
	}
	targetHistory, err := db.ListPluginTargetConfigHistory(ctx, target.Name)
	if err != nil || len(targetHistory) != historyLength {
		t.Fatalf("target history = %#v, %v", targetHistory, err)
	}
	backupHistory, err := db.ListBackupPluginOptionHistory(ctx, backup.JobID)
	if err != nil || len(backupHistory) != historyLength {
		t.Fatalf("backup history = %#v, %v", backupHistory, err)
	}
	restoreHistory, err := db.ListRestorePluginOptionHistory(ctx, restore.JobID)
	if err != nil || len(restoreHistory) != historyLength {
		t.Fatalf("restore history = %#v, %v", restoreHistory, err)
	}
}

func lifecycleValues(t *testing.T, values targetplugin.Values) []byte {
	t.Helper()
	encoded, err := targetplugin.MarshalProtocol(values)
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	return encoded
}

func releaseServer(t *testing.T, key *ecdsa.PrivateKey, versions []string) (*httptest.Server, targetplugin.RepositoryIndex) {
	t.Helper()
	testExecutable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	fingerprint, err := targetplugin.PublicKeyFingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("PublicKeyFingerprint: %v", err)
	}

	documents := map[string][]byte{}
	index := targetplugin.RepositoryIndex{
		FormatVersion: targetplugin.RepositoryFormatVersion,
		RepositoryID:  lifecycleRepositoryID,
	}
	for _, version := range versions {
		manifest := lifecycleManifest(t, version)
		manifestDigest := sha256.Sum256(manifest)
		artifact := []byte("#!/bin/sh\nPBS_PLUS_TEST_PLUGIN_VERSION=" + version +
			" exec '" + testExecutable + "' -test.run=^TestLifecyclePluginHelper$\n")
		artifactDigest := sha256.Sum256(artifact)
		documents["/"+version+"/manifest.toml"] = manifest
		documents["/"+version+"/plugin"] = artifact
		index.Releases = append(index.Releases, targetplugin.RepositoryRelease{
			PluginID:                lifecyclePluginID,
			Version:                 version,
			Publisher:               "PBS Plus Tests",
			PublisherKeyFingerprint: fingerprint,
			MinimumHostVersion:      "1.0.0",
			ProtocolVersion:         targetplugin.CurrentProtocolVersion,
			TargetTypes:             []string{"lifecycle"},
			ManifestURL:             version + "/manifest.toml",
			ManifestSHA256:          hex.EncodeToString(manifestDigest[:]),
			Channel:                 "stable",
			Artifacts: []targetplugin.RepositoryArtifact{{
				OS:        "linux",
				Arch:      "amd64",
				URL:       version + "/plugin",
				Size:      uint64(len(artifact)),
				SHA256:    hex.EncodeToString(artifactDigest[:]),
				Signature: signBase64(t, key, artifact),
			}},
		})
	}

	var encoded bytes.Buffer
	if err := toml.NewEncoder(&encoded).Encode(index); err != nil {
		t.Fatalf("encode index: %v", err)
	}
	documents["/index.toml"] = encoded.Bytes()
	documents["/index.toml.sig"] = []byte(signBase64(t, key, encoded.Bytes()))

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		document, ok := documents[request.URL.Path]
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("ETag", `"v1"`)
		_, _ = writer.Write(document)
	}))
	return server, index
}

func lifecycleManifest(t *testing.T, version string) []byte {
	t.Helper()
	descriptor := lifecycleDescriptorForVersion(version)
	digest, err := targetplugin.SchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("SchemaDigest: %v", err)
	}
	var encoded bytes.Buffer
	if err := toml.NewEncoder(&encoded).Encode(targetplugin.PluginManifest{
		FormatVersion:   targetplugin.ManifestFormatVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		PluginID:        descriptor.PluginID,
		Version:         descriptor.Version,
		TargetTypes:     descriptor.TargetTypes,
		SchemaSHA256:    digest,
		TargetSchema:    descriptor.TargetSchema,
		BackupSchema:    descriptor.BackupSchema,
		RestoreSchema:   descriptor.RestoreSchema,
	}); err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return encoded.Bytes()
}

func TestLifecyclePluginHelper(t *testing.T) {
	descriptor := lifecycleDescriptor()
	pipe := lifecyclePipe(t)
	if pipe == nil {
		return
	}
	defer pipe.Close()

	router := arpc.NewRouter()
	router.Handle(targetplugin.MethodDescribe, func(*arpc.Request) (arpc.Response, error) {
		return lifecycleResponse(descriptor)
	})
	router.Handle(targetplugin.MethodPluginHealth, func(*arpc.Request) (arpc.Response, error) {
		return lifecycleResponse(targetplugin.PluginHealthResponse{Healthy: true})
	})
	router.Handle(targetplugin.MethodTargetValidate, func(request *arpc.Request) (arpc.Response, error) {
		var validate targetplugin.TargetValidateRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &validate); err != nil {
			return arpc.Response{}, err
		}
		if err := validate.Validate(); err != nil {
			return arpc.Response{}, err
		}
		return lifecycleResponse(targetplugin.TargetValidateResponse{Config: validate.Target.Config})
	})
	router.Handle(targetplugin.MethodTargetMigrate, func(request *arpc.Request) (arpc.Response, error) {
		var migration targetplugin.TargetMigrateRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &migration); err != nil {
			return arpc.Response{}, err
		}
		if err := migration.Validate(); err != nil {
			return arpc.Response{}, err
		}
		if descriptor.Version == "1.2.0" {
			return arpc.Response{}, errors.New("test migration failure")
		}
		response := targetplugin.TargetMigrateResponse{Values: migration.Values}
		if migration.FromSchemaVersion == 1 && migration.ToSchemaVersion == 2 {
			response.RenameSecrets = map[string]string{"token": "credential"}
		}
		if migration.FromSchemaVersion == 2 && migration.ToSchemaVersion == 1 {
			response.RenameSecrets = map[string]string{"credential": "token"}
		}
		return lifecycleResponse(response)
	})
	router.Handle(targetplugin.MethodBackupOpen, func(request *arpc.Request) (arpc.Response, error) {
		var open targetplugin.BackupOpenRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &open); err != nil {
			return arpc.Response{}, err
		}
		if err := open.Validate(); err != nil {
			return arpc.Response{}, err
		}
		policy, ok := open.Job.Options["policy"]
		policyValue, valueOK := policy.StringValue()
		if !ok || !valueOK || policyValue != "full" || string(open.Job.Target.Secrets["credential"]) != "secret" {
			return arpc.Response{}, errors.New("backup received incomplete job values")
		}
		source := filepath.Join(open.Job.Workspace, "source")
		if err := os.Mkdir(source, 0o700); err != nil {
			return arpc.Response{}, err
		}
		if err := os.WriteFile(filepath.Join(source, "payload"), []byte("plugin backup"), 0o600); err != nil {
			return arpc.Response{}, err
		}
		return lifecycleResponse(targetplugin.BackupOpenResponse{
			Kind:         targetplugin.SourceDirectory,
			Path:         source,
			Archive:      targetplugin.Archive{Type: "lifecycle", FormatVersion: 1},
			HostFeatures: []targetplugin.HostFeature{targetplugin.FeatureSubpath, targetplugin.FeatureExclusions},
			CleanupToken: []byte("cleanup"),
		})
	})
	router.Handle(targetplugin.MethodRestoreOpen, func(request *arpc.Request) (arpc.Response, error) {
		var open targetplugin.RestoreOpenRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &open); err != nil {
			return arpc.Response{}, err
		}
		if err := open.Validate(); err != nil {
			return arpc.Response{}, err
		}
		mode, ok := open.Job.Options["mode"]
		modeValue, valueOK := mode.StringValue()
		if !ok || !valueOK || string(open.Job.Target.Secrets["credential"]) != "secret" {
			return arpc.Response{}, errors.New("restore received incomplete job values")
		}
		response := targetplugin.RestoreOpenResponse{CleanupToken: []byte("cleanup")}
		switch modeValue {
		case "path":
			response.Mode = targetplugin.RestoreModePath
			response.Path = filepath.Join(open.Job.Workspace, "destination")
			if err := os.Mkdir(response.Path, 0o700); err != nil {
				return arpc.Response{}, err
			}
		case "structured":
			response.Mode = targetplugin.RestoreModeStructured
		case "agent":
			brokerRequest := targetplugin.HostAgentRestoreRequest{
				Operation: open.Operation, Hostname: "agent.example", VolumeID: "disk-1", DestinationPath: "/restore",
			}
			encoded, err := targetplugin.MarshalProtocol(brokerRequest)
			if err != nil {
				return arpc.Response{}, err
			}
			var ignored []byte
			if err := pipe.Call(request.Context, targetplugin.MethodHostAgentRestore, encoded, &ignored); err != nil {
				return arpc.Response{}, err
			}
			response.Mode = targetplugin.RestoreModeAgent
		default:
			return arpc.Response{}, fmt.Errorf("unsupported test restore mode %q", modeValue)
		}
		return lifecycleResponse(response)
	})
	router.Handle(targetplugin.MethodRestoreConsume, func(request *arpc.Request) (arpc.Response, error) {
		var consume targetplugin.RestoreConsumeRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &consume); err != nil {
			return arpc.Response{}, err
		}
		if err := consume.Validate(); err != nil {
			return arpc.Response{}, err
		}
		payload, err := os.ReadFile(filepath.Join(consume.ArchivePath, "payload"))
		if err != nil {
			return arpc.Response{}, err
		}
		if string(payload) != "plugin restore" {
			return arpc.Response{}, errors.New("unexpected structured restore payload")
		}
		return lifecycleResponse(targetplugin.RestoreConsumeResponse{})
	})
	router.Handle(targetplugin.MethodBackupMigrateOptions, func(request *arpc.Request) (arpc.Response, error) {
		var migration targetplugin.BackupMigrateOptionsRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &migration); err != nil {
			return arpc.Response{}, err
		}
		if err := migration.Validate(); err != nil {
			return arpc.Response{}, err
		}
		return lifecycleResponse(targetplugin.BackupMigrateOptionsResponse{Values: migration.Values})
	})
	router.Handle(targetplugin.MethodRestoreMigrateOptions, func(request *arpc.Request) (arpc.Response, error) {
		var migration targetplugin.RestoreMigrateOptionsRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &migration); err != nil {
			return arpc.Response{}, err
		}
		if err := migration.Validate(); err != nil {
			return arpc.Response{}, err
		}
		return lifecycleResponse(targetplugin.RestoreMigrateOptionsResponse{Values: migration.Values})
	})
	router.Handle(targetplugin.MethodTargetProbe, func(request *arpc.Request) (arpc.Response, error) {
		var probe targetplugin.TargetProbeRequest
		if err := targetplugin.UnmarshalProtocol(request.Payload, &probe); err != nil {
			return arpc.Response{}, err
		}
		if err := probe.Validate(); err != nil {
			return arpc.Response{}, err
		}
		path, ok := probe.Target.Config["path"]
		pathValue, pathOK := path.StringValue()
		secretKey := "token"
		if descriptor.Version != "1.0.0" {
			secretKey = "credential"
		}
		if !ok || !pathOK || pathValue != "/data" || string(probe.Target.Secrets[secretKey]) != "secret" {
			return arpc.Response{}, fmt.Errorf("probe received incomplete target values")
		}
		event, err := targetplugin.MarshalProtocol(targetplugin.HostEvent{
			Operation: probe.Operation,
			Level:     targetplugin.EventInfo,
			Message:   "probe complete",
		})
		if err != nil {
			return arpc.Response{}, err
		}
		var ignored []byte
		if err := pipe.Call(t.Context(), targetplugin.MethodHostEvent, event, &ignored); err != nil {
			return arpc.Response{}, err
		}
		return lifecycleResponse(targetplugin.TargetProbeResponse{Available: true, Size: &targetplugin.Size{Total: 10, Used: 6, Free: 4}})
	})
	pipe.SetRouter(router)
	_ = pipe.Serve()
}

func lifecycleDescriptor() targetplugin.Descriptor {
	version := os.Getenv("PBS_PLUS_TEST_PLUGIN_VERSION")
	if version == "" {
		version = "1.0.0"
	}
	return lifecycleDescriptorForVersion(version)
}

func lifecycleDescriptorForVersion(version string) targetplugin.Descriptor {
	schemaVersion := uint32(1)
	secretKey := "token"
	if version == "1.1.0" {
		schemaVersion = 2
		secretKey = "credential"
	}
	if version == "1.2.0" {
		schemaVersion = 3
		secretKey = "credential"
	}
	return targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        lifecyclePluginID,
		Version:         version,
		TargetTypes:     []string{"lifecycle"},
		TargetSchema: targetplugin.FormSchema{Version: schemaVersion, Fields: []targetplugin.FormField{
			{Key: "path", Label: "Path", Control: targetplugin.ControlPath, Required: true},
			{Key: secretKey, Label: "Credential", Control: targetplugin.ControlSecret, Required: true},
		}},
		BackupSchema: targetplugin.FormSchema{Version: schemaVersion, Fields: []targetplugin.FormField{
			{Key: "policy", Label: "Policy", Control: targetplugin.ControlText},
		}},
		RestoreSchema: targetplugin.FormSchema{Version: schemaVersion, Fields: []targetplugin.FormField{
			{Key: "mode", Label: "Mode", Control: targetplugin.ControlText},
		}},
	}
}

func lifecyclePipe(t *testing.T) *arpc.StreamPipe {
	t.Helper()
	descriptorText, ok := os.LookupEnv(targetplugin.SocketFDEnv)
	if !ok {
		return nil
	}
	fd, err := strconv.Atoi(descriptorText)
	if err != nil {
		t.Fatalf("parse socket fd: %v", err)
	}
	file := os.NewFile(uintptr(fd), "pbs-plus-plugin")
	if file == nil {
		t.Fatal("open inherited socket")
	}
	conn, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		t.Fatalf("net.FileConn: %v", err)
	}
	pipe, err := arpc.NewServerPipe(t.Context(), conn)
	if err != nil {
		t.Fatalf("NewServerPipe: %v", err)
	}
	return pipe
}

func lifecycleResponse(value any) (arpc.Response, error) {
	data, err := targetplugin.MarshalProtocol(value)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: http.StatusOK, Data: data}, nil
}
