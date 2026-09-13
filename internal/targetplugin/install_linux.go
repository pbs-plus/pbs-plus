package targetplugin

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

const installCheckTimeout = 10 * time.Second

// ErrVersionInstalled reports an attempt to replace an installed immutable version.
var ErrVersionInstalled = errors.New("plugin version is already installed")

// InstallVersionRequest contains authenticated repository inputs and the streamed executable.
type InstallVersionRequest struct {
	Root             string
	ManifestBytes    []byte
	Release          RepositoryRelease
	Artifact         RepositoryArtifact
	ArtifactReader   io.Reader
	PublisherKey     *ecdsa.PublicKey
	MaxArtifactBytes uint64
}

// InstalledVersion identifies an atomically promoted plugin version.
type InstalledVersion struct {
	Directory  string
	Executable string
	Manifest   PluginManifest
	Descriptor Descriptor
}

// InstallVersion verifies and promotes one immutable plugin version.
func InstallVersion(ctx context.Context, request InstallVersionRequest) (InstalledVersion, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !filepath.IsAbs(request.Root) {
		return InstalledVersion{}, errors.New("plugin install root must be absolute")
	}
	if request.ArtifactReader == nil {
		return InstalledVersion{}, errors.New("plugin artifact reader is required")
	}

	manifest, err := ParsePluginManifest(request.ManifestBytes, request.Release.ManifestSHA256)
	if err != nil {
		return InstalledVersion{}, err
	}
	if err := manifest.VerifyRelease(request.Release); err != nil {
		return InstalledVersion{}, fmt.Errorf("verify plugin release: %w", err)
	}
	if !releaseContainsArtifact(request.Release, request.Artifact) {
		return InstalledVersion{}, errors.New("plugin artifact is not in the release")
	}
	if request.Artifact.OS != runtime.GOOS || request.Artifact.Arch != runtime.GOARCH {
		return InstalledVersion{}, fmt.Errorf("plugin artifact platform %s/%s does not match host %s/%s", request.Artifact.OS, request.Artifact.Arch, runtime.GOOS, runtime.GOARCH)
	}

	pluginRoot := filepath.Join(request.Root, manifest.PluginID)
	versionDirectory := filepath.Join(pluginRoot, manifest.Version)
	if _, err := os.Lstat(versionDirectory); err == nil {
		return InstalledVersion{}, ErrVersionInstalled
	} else if !errors.Is(err, os.ErrNotExist) {
		return InstalledVersion{}, fmt.Errorf("inspect plugin version: %w", err)
	}
	if err := os.MkdirAll(pluginRoot, 0o755); err != nil {
		return InstalledVersion{}, fmt.Errorf("create plugin directory: %w", err)
	}
	stagingDirectory, err := os.MkdirTemp(pluginRoot, ".install-")
	if err != nil {
		return InstalledVersion{}, fmt.Errorf("create install transaction: %w", err)
	}
	defer os.RemoveAll(stagingDirectory)

	executable := filepath.Join(stagingDirectory, "plugin")
	if err := stageArtifact(executable, request); err != nil {
		return InstalledVersion{}, err
	}
	if err := writeInstallFile(filepath.Join(stagingDirectory, "manifest.toml"), request.ManifestBytes, 0o444); err != nil {
		return InstalledVersion{}, fmt.Errorf("write plugin manifest: %w", err)
	}

	descriptor, err := checkStagedPlugin(ctx, executable, manifest)
	if err != nil {
		return InstalledVersion{}, err
	}
	if err := syncDirectory(stagingDirectory); err != nil {
		return InstalledVersion{}, fmt.Errorf("sync install transaction: %w", err)
	}
	if err := unix.Renameat2(unix.AT_FDCWD, stagingDirectory, unix.AT_FDCWD, versionDirectory, unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return InstalledVersion{}, ErrVersionInstalled
		}
		return InstalledVersion{}, fmt.Errorf("promote plugin version: %w", err)
	}
	if err := syncDirectory(pluginRoot); err != nil {
		return InstalledVersion{}, fmt.Errorf("sync plugin directory: %w", err)
	}

	return InstalledVersion{
		Directory:  versionDirectory,
		Executable: filepath.Join(versionDirectory, "plugin"),
		Manifest:   manifest,
		Descriptor: descriptor,
	}, nil
}

func releaseContainsArtifact(release RepositoryRelease, artifact RepositoryArtifact) bool {
	return slices.Contains(release.Artifacts, artifact)
}

func stageArtifact(path string, request InstallVersionRequest) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create staging artifact: %w", err)
	}
	if err := VerifyArtifact(file, request.ArtifactReader, request.Artifact, request.Release.PublisherKeyFingerprint, request.PublisherKey, request.MaxArtifactBytes); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0o555); err != nil {
		_ = file.Close()
		return fmt.Errorf("make staging artifact executable: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync staging artifact: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close staging artifact: %w", err)
	}
	return nil
}

func writeInstallFile(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func checkStagedPlugin(ctx context.Context, executable string, manifest PluginManifest) (Descriptor, error) {
	checkContext, cancel := context.WithTimeout(ctx, installCheckTimeout)
	defer cancel()

	process, err := Start(checkContext, executable)
	if err != nil {
		return Descriptor{}, fmt.Errorf("start staged plugin: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = process.Close()
		}
	}()

	descriptor, err := process.Describe(checkContext)
	if err != nil {
		return Descriptor{}, err
	}
	if err := manifest.VerifyDescriptor(descriptor); err != nil {
		return Descriptor{}, fmt.Errorf("verify staged plugin: %w", err)
	}
	deadline, _ := checkContext.Deadline()
	healthRequest := PluginHealthRequest{Operation: Operation{
		ProtocolVersion:   CurrentProtocolVersion,
		ID:                "install-health",
		IdempotencyKey:    "install-health:" + manifest.SchemaSHA256,
		DeadlineUnixMilli: deadline.UnixMilli(),
		PluginVersion:     manifest.Version,
	}}
	var health PluginHealthResponse
	if err := process.Invoke(checkContext, MethodPluginHealth, healthRequest, &health); err != nil {
		return Descriptor{}, fmt.Errorf("check staged plugin health: %w", err)
	}
	if !health.Healthy {
		return Descriptor{}, fmt.Errorf("staged plugin is unhealthy: %s", health.Message)
	}
	if err := process.Close(); err != nil {
		return Descriptor{}, fmt.Errorf("close staged plugin: %w", err)
	}
	closed = true
	return descriptor, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
