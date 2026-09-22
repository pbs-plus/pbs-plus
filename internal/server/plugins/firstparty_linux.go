//go:build linux

package plugins

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

// Publisher key pinned per release; rotating it ships with a server update.
const (
	FirstPartyRepositoryID  = "org.pbs-plus.plugins"
	FirstPartyRepositoryURL = "https://raw.githubusercontent.com/pbs-plus/plugins/main/index.toml"
)

const firstPartyPublisherKeyPEM = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAELyf4NWiWAmx3dc8iedxNz7Jkq4Ov
1D8NhnPCbFHM8FPrVDpc1UXPF91fwvKNUtvedBTj6MmcHur8BW+Jab5UHA==
-----END PUBLIC KEY-----`

// FirstPartyPublisherKey returns the pinned key for the default repository.
func FirstPartyPublisherKey() (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(firstPartyPublisherKeyPEM))
	if block == nil {
		return nil, errors.New("first-party publisher key is not PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse first-party publisher key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("first-party publisher key is not ECDSA")
	}
	return key, nil
}

// Injection points for tests: the pinned defaults swap for a local server + key.
var (
	firstPartyRepositoryURL = FirstPartyRepositoryURL
	firstPartyPublisherKey  = FirstPartyPublisherKey
	newFirstPartyFetcher    = func() targetplugin.Fetcher { return targetplugin.Fetcher{} }
)

// EnsureFirstPartyPlugins installs every compatible first-party release; failures
// are returned so boot continues offline.
func EnsureFirstPartyPlugins(ctx context.Context, db *coredb.Store) (int, error) {
	if err := ensureFirstPartyRepository(ctx, db); err != nil {
		return 0, err
	}

	publisherKey, err := firstPartyPublisherKey()
	if err != nil {
		return 0, err
	}
	fetcher := newFirstPartyFetcher()
	document, err := fetcher.Index(ctx, firstPartyRepositoryURL, targetplugin.PluginRepositoryCache{})
	if err != nil {
		return 0, recordRepositoryError(ctx, db, err)
	}
	index, err := targetplugin.ParseRepositoryIndex(document.Index, document.Signature, publisherKey)
	if err != nil {
		return 0, recordRepositoryError(ctx, db, err)
	}
	if err := index.Validate(); err != nil {
		return 0, recordRepositoryError(ctx, db, err)
	}

	installed := 0
	var failures error
	for _, release := range newestCompatibleReleases(index.Releases) {
		added, err := installFirstPartyRelease(ctx, db, fetcher, publisherKey, release)
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("install %s: %w", release.PluginID, err))
			continue
		}
		if added {
			installed++
		}
	}
	return installed, failures
}

// newestCompatibleReleases picks the highest semver release per plugin that
// speaks the current protocol and carries an artifact for this platform.
func newestCompatibleReleases(releases []targetplugin.RepositoryRelease) []targetplugin.RepositoryRelease {
	newest := map[string]targetplugin.RepositoryRelease{}
	for _, release := range releases {
		if release.Revoked || release.ProtocolVersion != targetplugin.CurrentProtocolVersion {
			continue
		}
		if artifactForPlatform(release).URL == "" {
			continue
		}
		current, ok := newest[release.PluginID]
		if !ok {
			newest[release.PluginID] = release
			continue
		}
		currentVersion, currentErr := semver.NewVersion(current.Version)
		releaseVersion, releaseErr := semver.NewVersion(release.Version)
		if currentErr != nil || releaseErr != nil {
			continue
		}
		if releaseVersion.GreaterThan(currentVersion) {
			newest[release.PluginID] = release
		}
	}
	selected := make([]targetplugin.RepositoryRelease, 0, len(newest))
	for _, release := range newest {
		selected = append(selected, release)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].PluginID < selected[j].PluginID })
	return selected
}

func artifactForPlatform(release targetplugin.RepositoryRelease) targetplugin.RepositoryArtifact {
	for _, candidate := range release.Artifacts {
		if candidate.OS == runtime.GOOS && candidate.Arch == runtime.GOARCH {
			return candidate
		}
	}
	return targetplugin.RepositoryArtifact{}
}

func installFirstPartyRelease(ctx context.Context, db *coredb.Store, fetcher targetplugin.Fetcher, publisherKey *ecdsa.PublicKey, release targetplugin.RepositoryRelease) (bool, error) {
	if release.Revoked {
		return false, nil
	}
	if release.ProtocolVersion != targetplugin.CurrentProtocolVersion {
		return false, nil
	}
	plugin, err := db.GetInstalledPlugin(ctx, release.PluginID)
	alreadyActive := err == nil && plugin.ActiveVersion == release.Version
	if alreadyActive {
		return false, nil
	}
	activate := err != nil || plugin.ActiveVersion == ""

	artifact := artifactForPlatform(release)
	if artifact.URL == "" {
		return false, nil
	}

	resolved := resolvedRelease{Release: release, Artifact: artifact}
	installed, err := installRelease(ctx, fetcher, firstPartyPluginsRoot(), firstPartyRepositoryURL, resolved, publisherKey)
	if err != nil {
		return false, err
	}
	if err := registerVersion(ctx, db, nil, FirstPartyRepositoryID, resolved, installed, activate); err != nil {
		return false, err
	}
	return true, nil
}

func firstPartyPluginsRoot() string {
	return conf.PluginsBasePath
}

func ensureFirstPartyRepository(ctx context.Context, db *coredb.Store) error {
	publicKey, err := FirstPartyPublisherKey()
	if err != nil {
		return err
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return err
	}
	_, getErr := db.GetPluginRepository(ctx, FirstPartyRepositoryID)
	if getErr == nil {
		if _, err := db.UpdatePluginRepository(ctx, FirstPartyRepositoryID, "PBS Plus first-party plugins", FirstPartyRepositoryURL); err != nil {
			return err
		}
		return nil
	}
	if !errors.Is(getErr, sql.ErrNoRows) {
		return getErr
	}
	return db.CreatePluginRepository(ctx, coredb.PluginRepository{
		ID:        FirstPartyRepositoryID,
		Name:      "PBS Plus first-party plugins",
		URL:       FirstPartyRepositoryURL,
		PublicKey: der,
		Enabled:   true,
	})
}

func recordRepositoryError(ctx context.Context, db *coredb.Store, err error) error {
	_, recordErr := db.RecordPluginRepositoryRefresh(ctx, FirstPartyRepositoryID, coredb.PluginRepositoryRefresh{
		RefreshedAt: time.Now().UTC(),
		LastError:   err.Error(),
	})
	if recordErr != nil {
		log.Error(recordErr, "recording first-party repository error")
	}
	return err
}
