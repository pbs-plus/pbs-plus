package targetplugin

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestInstallVersion(t *testing.T) {
	request, artifactBytes := installVersionRequest(t, "org.pbs-plus.test", false)
	installed, err := InstallVersion(t.Context(), request)
	if err != nil {
		t.Fatalf("InstallVersion: %v", err)
	}

	wantDirectory := filepath.Join(request.Root, "org.pbs-plus.test", "1.0.0")
	if installed.Directory != wantDirectory {
		t.Fatalf("Directory = %q, want %q", installed.Directory, wantDirectory)
	}
	if installed.Descriptor.PluginID != "org.pbs-plus.test" {
		t.Fatalf("descriptor plugin ID = %q", installed.Descriptor.PluginID)
	}
	artifact, err := os.ReadFile(installed.Executable)
	if err != nil {
		t.Fatalf("read installed artifact: %v", err)
	}
	if !bytes.Equal(artifact, artifactBytes) {
		t.Fatal("installed artifact differs from verified bytes")
	}
	info, err := os.Stat(installed.Executable)
	if err != nil {
		t.Fatalf("stat installed artifact: %v", err)
	}
	if info.Mode().Perm() != 0o555 {
		t.Fatalf("artifact mode = %o, want 555", info.Mode().Perm())
	}
	manifest, err := os.ReadFile(filepath.Join(installed.Directory, "manifest.toml"))
	if err != nil {
		t.Fatalf("read installed manifest: %v", err)
	}
	if !bytes.Equal(manifest, request.ManifestBytes) {
		t.Fatal("installed manifest differs from authenticated bytes")
	}

	request.ArtifactReader = bytes.NewReader(artifactBytes)
	if _, err := InstallVersion(t.Context(), request); !errors.Is(err, ErrVersionInstalled) {
		t.Fatalf("second InstallVersion error = %v, want ErrVersionInstalled", err)
	}
}

func TestInstallVersionDoesNotReplaceConcurrentInstall(t *testing.T) {
	request, artifactBytes := installVersionRequest(t, "org.pbs-plus.test", false)
	results := make(chan error, 2)
	ctx := t.Context()
	for range 2 {
		installRequest := request
		installRequest.ArtifactReader = bytes.NewReader(artifactBytes)
		go func() {
			_, err := InstallVersion(ctx, installRequest)
			results <- err
		}()
	}

	var installed, exists int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			installed++
		case errors.Is(err, ErrVersionInstalled):
			exists++
		default:
			t.Fatalf("InstallVersion error = %v", err)
		}
	}
	if installed != 1 || exists != 1 {
		t.Fatalf("results = %d installed, %d existing", installed, exists)
	}
	entries, err := os.ReadDir(filepath.Join(request.Root, "org.pbs-plus.test"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "1.0.0" {
		t.Fatalf("plugin directory entries = %v", entries)
	}
}

func TestInstallVersionCleansFailedTransaction(t *testing.T) {
	tests := []struct {
		name      string
		pluginID  string
		unhealthy bool
		wantError string
	}{
		{name: "descriptor mismatch", pluginID: "org.pbs-plus.other", wantError: "plugin ID does not match"},
		{name: "unhealthy", pluginID: "org.pbs-plus.test", unhealthy: true, wantError: "test failure"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, _ := installVersionRequest(t, test.pluginID, test.unhealthy)
			_, err := InstallVersion(t.Context(), request)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("InstallVersion error = %v, want %q", err, test.wantError)
			}
			pluginRoot := filepath.Join(request.Root, test.pluginID)
			entries, readErr := os.ReadDir(pluginRoot)
			if readErr != nil {
				t.Fatalf("ReadDir: %v", readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("failed install left entries: %v", entries)
			}
		})
	}
}

func installVersionRequest(t *testing.T, pluginID string, unhealthy bool) (InstallVersionRequest, []byte) {
	t.Helper()
	testExecutable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	script := "#!/bin/sh\n"
	if unhealthy {
		script += "export PBS_PLUS_TEST_PLUGIN_UNHEALTHY=1\n"
	}
	script += "exec " + shellQuote(testExecutable) + " -test.run=^TestPluginProcessHelper$\n"
	artifactBytes := []byte(script)

	descriptor := Descriptor{
		ProtocolVersion: CurrentProtocolVersion,
		PluginID:        pluginID,
		Version:         "1.0.0",
		TargetTypes:     []string{"test"},
		TargetSchema:    FormSchema{Version: 1},
		BackupSchema:    FormSchema{Version: 1},
		RestoreSchema:   FormSchema{Version: 1},
	}
	schemaDigest, err := descriptorSchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("descriptorSchemaDigest: %v", err)
	}
	manifest := PluginManifest{
		FormatVersion:   ManifestFormatVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		PluginID:        descriptor.PluginID,
		Version:         descriptor.Version,
		TargetTypes:     descriptor.TargetTypes,
		SchemaSHA256:    schemaDigest,
		TargetSchema:    descriptor.TargetSchema,
		BackupSchema:    descriptor.BackupSchema,
		RestoreSchema:   descriptor.RestoreSchema,
	}
	manifestBytes, err := toml.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	fingerprint, err := p256Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("p256Fingerprint: %v", err)
	}
	artifact := signedArtifact(t, key, artifactBytes)
	artifact.OS = runtime.GOOS
	artifact.Arch = runtime.GOARCH
	release := RepositoryRelease{
		PluginID:                pluginID,
		Version:                 "1.0.0",
		Publisher:               "PBS Plus Tests",
		PublisherKeyFingerprint: fingerprint,
		MinimumHostVersion:      "1.0.0",
		ProtocolVersion:         CurrentProtocolVersion,
		TargetTypes:             []string{"test"},
		ManifestURL:             "plugins/test/manifest.toml",
		ManifestSHA256:          hex.EncodeToString(manifestDigest[:]),
		Channel:                 "stable",
		Artifacts:               []RepositoryArtifact{artifact},
	}
	return InstallVersionRequest{
		Root:             t.TempDir(),
		ManifestBytes:    manifestBytes,
		Release:          release,
		Artifact:         artifact,
		ArtifactReader:   bytes.NewReader(artifactBytes),
		PublisherKey:     &key.PublicKey,
		MaxArtifactBytes: uint64(len(artifactBytes)),
	}, artifactBytes
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func TestInstallVersionRejectsUnlistedArtifact(t *testing.T) {
	request, artifactBytes := installVersionRequest(t, "org.pbs-plus.test", false)
	request.Artifact.URL = "plugins/test/other"
	request.ArtifactReader = bytes.NewReader(artifactBytes)
	if _, err := InstallVersion(t.Context(), request); err == nil || !strings.Contains(err.Error(), "not in the release") {
		t.Fatalf("InstallVersion error = %v", err)
	}
}
