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
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
		if err := registerVersion(ctx, db, lifecycleRepositoryID, resolved, installed, true); err != nil {
			t.Fatalf("registerVersion(%s): %v", version, err)
		}
	}

	plugin, err := db.GetInstalledPlugin(ctx, lifecyclePluginID)
	if err != nil || plugin.ActiveVersion != "1.1.0" {
		t.Fatalf("upgraded plugin = %#v, %v", plugin, err)
	}

	supervisor, err := targetplugin.NewSupervisor(2, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
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
				Secrets: targetplugin.Secrets{"token": []byte("secret")},
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
	form := map[string][]string{"path": {"/data"}, "token": {"secret"}}
	if err := CreateTarget(ctx, db, supervisor, "plugin-target", lifecyclePluginID, "lifecycle", form); err != nil {
		t.Fatalf("CreateTarget: %v", err)
	}
	pluginTarget, err := db.GetPluginTarget(ctx, "plugin-target")
	if err != nil || pluginTarget.PluginVersion != "1.1.0" || pluginTarget.TargetType != "lifecycle" || len(pluginTarget.SecretFields) != 1 || pluginTarget.SecretFields[0] != "token" {
		t.Fatalf("plugin target = %#v, %v", pluginTarget, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close before restart: %v", err)
	}
	db, err = coredb.Initialize(ctx, dbPath)
	if err != nil {
		t.Fatalf("Initialize after restart: %v", err)
	}
	pluginTarget, err = db.GetPluginTarget(ctx, "plugin-target")
	if err != nil || len(pluginTarget.SecretFields) != 1 || pluginTarget.SecretFields[0] != "token" {
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

	if rolled, err := db.ActivatePluginVersion(ctx, lifecyclePluginID, "1.0.0"); err != nil || !rolled {
		t.Fatalf("rollback = %v, %v", rolled, err)
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
	descriptor := lifecycleDescriptor()
	descriptor.Version = version
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
		if !ok || !pathOK || pathValue != "/data" || string(probe.Target.Secrets["token"]) != "secret" {
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
	return targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        lifecyclePluginID,
		Version:         version,
		TargetTypes:     []string{"lifecycle"},
		TargetSchema: targetplugin.FormSchema{Version: 1, Fields: []targetplugin.FormField{
			{Key: "path", Label: "Path", Control: targetplugin.ControlPath, Required: true},
			{Key: "token", Label: "Token", Control: targetplugin.ControlSecret, Required: true},
		}},
		BackupSchema:  targetplugin.FormSchema{Version: 1},
		RestoreSchema: targetplugin.FormSchema{Version: 1},
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
