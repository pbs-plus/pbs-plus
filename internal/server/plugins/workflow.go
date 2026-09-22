//go:build linux

package plugins

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const (
	installAttempts    = 3
	installRetryDelay  = 15 * time.Second
	manifestFileName   = "manifest.toml"
	executableFileName = "plugin"
)

type resolvedRelease struct {
	Release  targetplugin.RepositoryRelease  `json:"release"`
	Artifact targetplugin.RepositoryArtifact `json:"artifact"`
}

type installedArtifact struct {
	Directory      string `json:"directory"`
	Platform       string `json:"platform"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}

// Register registers the durable plugin install workflow: resolve, install, register.
func Register(engine *jobs.Engine, app *application.Runtime) error {
	return engine.RegisterVersion(jobs.WorkflowPluginInstall, "1", func(w *jobs.WorkflowContext) error {
		var input jobs.PluginInstallInput
		if err := json.Unmarshal(w.Execution.Payload, &input); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding plugin install workflow input: %w", err))
		}
		return runInstall(w, app, input)
	})
}

// Submit queues one plugin installation, serialized against other work on the same plugin.
func Submit(ctx context.Context, engine *jobs.Engine, input jobs.PluginInstallInput) (jobdb.Execution, bool, error) {
	if input.RepositoryID == "" || input.PluginID == "" || input.Version == "" {
		return jobdb.Execution{}, false, errors.New("plugin repository, identity, and version are required")
	}
	request, err := jobs.NewWorkflowSubmit(
		jobs.WorkflowPluginInstall,
		input.PluginID,
		"manual",
		input.PluginID+"@"+input.Version,
		input,
		[]string{"plugin:" + input.PluginID},
		installAttempts,
		installRetryDelay,
	)
	if err != nil {
		return jobdb.Execution{}, false, err
	}
	return engine.Submit(ctx, request)
}

func runInstall(w *jobs.WorkflowContext, app *application.Runtime, input jobs.PluginInstallInput) error {
	repository, err := app.CoreDB.GetPluginRepository(w.Context, input.RepositoryID)
	if err != nil {
		return jobs.NonRetryable(fmt.Errorf("getting plugin repository: %w", err))
	}
	if !repository.Enabled {
		return jobs.NonRetryable(fmt.Errorf("plugin repository %q is disabled", repository.ID))
	}
	publisherKey, err := parsePublisherKey(repository.PublicKey)
	if err != nil {
		return jobs.NonRetryable(err)
	}

	fetcher := targetplugin.Fetcher{}
	resolvedRaw, err := w.Activity("resolve", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		document, err := fetcher.Index(ctx, repository.URL, targetplugin.PluginRepositoryCache{})
		if err != nil {
			return nil, err
		}
		index, err := targetplugin.ParseRepositoryIndex(document.Index, document.Signature, publisherKey)
		if err != nil {
			return nil, jobs.NonRetryable(err)
		}
		resolved, err := resolveRelease(index, input.PluginID, input.Version)
		if err != nil {
			return nil, jobs.NonRetryable(err)
		}
		return json.Marshal(resolved)
	})
	if err != nil {
		return err
	}
	var resolved resolvedRelease
	if err := json.Unmarshal(resolvedRaw, &resolved); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding resolved plugin release: %w", err))
	}

	installedRaw, err := w.Activity("install", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		installed, err := installRelease(ctx, fetcher, conf.PluginsBasePath, repository.URL, resolved, publisherKey)
		if err != nil {
			return nil, err
		}
		return json.Marshal(installed)
	})
	if err != nil {
		return err
	}
	var installed installedArtifact
	if err := json.Unmarshal(installedRaw, &installed); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding installed plugin artifact: %w", err))
	}

	return w.Step("register", func(ctx context.Context) error {
		return registerVersion(ctx, app.CoreDB, app.PluginSupervisor, repository.ID, resolved, installed, input.Activate)
	})
}

func installRelease(ctx context.Context, fetcher targetplugin.Fetcher, root, indexURL string, resolved resolvedRelease, publisherKey *ecdsa.PublicKey) (installedArtifact, error) {
	platform := resolved.Artifact.OS + "/" + resolved.Artifact.Arch
	result := installedArtifact{
		Directory:      filepath.Join(root, resolved.Release.PluginID, resolved.Release.Version),
		Platform:       platform,
		ArtifactSHA256: resolved.Artifact.SHA256,
	}
	manifestBytes, err := fetcher.Manifest(ctx, indexURL, resolved.Release.ManifestURL)
	if err != nil {
		return installedArtifact{}, err
	}
	stream, err := fetcher.Artifact(ctx, indexURL, resolved.Artifact.URL)
	if err != nil {
		return installedArtifact{}, err
	}
	defer stream.Close()

	installedVersion, err := targetplugin.InstallVersion(ctx, targetplugin.InstallVersionRequest{
		Root:             root,
		ManifestBytes:    manifestBytes,
		Release:          resolved.Release,
		Artifact:         resolved.Artifact,
		ArtifactReader:   stream,
		PublisherKey:     publisherKey,
		MaxArtifactBytes: resolved.Artifact.Size,
	})
	if errors.Is(err, targetplugin.ErrVersionInstalled) {
		return result, nil
	}
	if err != nil {
		return installedArtifact{}, err
	}
	result.Directory = installedVersion.Directory
	return result, nil
}

func registerVersion(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, repositoryID string, resolved resolvedRelease, installed installedArtifact, activate bool) error {
	if _, err := db.GetInstalledPluginVersion(ctx, resolved.Release.PluginID, resolved.Release.Version); err == nil {
		if !activate {
			return nil
		}
		return ActivateVersion(ctx, db, supervisor, resolved.Release.PluginID, resolved.Release.Version)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(installed.Directory, manifestFileName))
	if err != nil {
		return fmt.Errorf("reading installed plugin manifest: %w", err)
	}
	now := time.Now()
	if err := db.RegisterPluginVersion(ctx, repositoryID, coredb.InstalledPluginVersion{
		PluginID:        resolved.Release.PluginID,
		Version:         resolved.Release.Version,
		Platform:        installed.Platform,
		InstallPath:     filepath.Join(installed.Directory, executableFileName),
		Manifest:        manifestBytes,
		ArtifactSHA256:  installed.ArtifactSHA256,
		InstalledAt:     now,
		HealthState:     coredb.PluginHealthHealthy,
		HealthCheckedAt: now,
	}, false); err != nil {
		return err
	}
	if !activate {
		return nil
	}
	return ActivateVersion(ctx, db, supervisor, resolved.Release.PluginID, resolved.Release.Version)
}

func resolveRelease(index targetplugin.RepositoryIndex, pluginID, version string) (resolvedRelease, error) {
	for _, release := range index.Releases {
		if release.PluginID != pluginID || release.Version != version {
			continue
		}
		if release.Revoked {
			return resolvedRelease{}, fmt.Errorf("plugin %q version %q is revoked: %s", pluginID, version, release.RevocationReason)
		}
		if release.ProtocolVersion != targetplugin.CurrentProtocolVersion {
			return resolvedRelease{}, fmt.Errorf("plugin %q version %q needs protocol %d", pluginID, version, release.ProtocolVersion)
		}
		for _, artifact := range release.Artifacts {
			if artifact.OS == runtime.GOOS && artifact.Arch == runtime.GOARCH {
				return resolvedRelease{Release: release, Artifact: artifact}, nil
			}
		}
		return resolvedRelease{}, fmt.Errorf("plugin %q version %q has no %s/%s artifact", pluginID, version, runtime.GOOS, runtime.GOARCH)
	}
	return resolvedRelease{}, fmt.Errorf("plugin %q version %q is not in repository %q", pluginID, version, index.RepositoryID)
}

func parsePublisherKey(der []byte) (*ecdsa.PublicKey, error) {
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("parsing plugin repository key: %w", err)
	}
	publicKey, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("plugin repository key is not an ECDSA key")
	}
	return publicKey, nil
}
