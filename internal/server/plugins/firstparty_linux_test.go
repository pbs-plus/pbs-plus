//go:build linux

package plugins

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestEnsureFirstPartyPluginsInstallsAll(t *testing.T) {
	restore := stubFirstPartyDeps(t)
	defer restore()

	ctx := context.Background()
	directory := t.TempDir()
	crypto.SetSealKeyPath(filepath.Join(directory, "secrets.key"))
	t.Cleanup(func() { crypto.SetSealKeyPath(conf.SecretsKeyPath) })
	db, err := coredb.Initialize(ctx, filepath.Join(directory, "firstparty.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	installed, err := EnsureFirstPartyPlugins(ctx, db)
	if err != nil || installed != 1 {
		t.Fatalf("EnsureFirstPartyPlugins = %d, %v; want 1, nil", installed, err)
	}

	plugin, err := db.GetInstalledPlugin(ctx, lifecyclePluginID)
	if err != nil || plugin.ActiveVersion != "1.1.0" {
		t.Fatalf("installed plugin = %#v, %v; want active 1.1.0", plugin, err)
	}
	repository, err := db.GetPluginRepository(ctx, FirstPartyRepositoryID)
	if err != nil || !repository.Enabled {
		t.Fatalf("repository = %#v, %v; want enabled default row", repository, err)
	}

	installed, err = EnsureFirstPartyPlugins(ctx, db)
	if err != nil || installed != 0 {
		t.Fatalf("second EnsureFirstPartyPlugins = %d, %v; want 0, nil", installed, err)
	}
}

func TestEnsureFirstPartyPluginsRecordsRepositoryError(t *testing.T) {
	restore := stubFirstPartyDeps(t)
	defer restore()
	firstPartyRepositoryURL = "https://firstparty.invalid/index.toml"
	newFirstPartyFetcher = func() targetplugin.Fetcher { return targetplugin.Fetcher{} }

	ctx := context.Background()
	directory := t.TempDir()
	crypto.SetSealKeyPath(filepath.Join(directory, "secrets.key"))
	t.Cleanup(func() { crypto.SetSealKeyPath(conf.SecretsKeyPath) })
	db, err := coredb.Initialize(ctx, filepath.Join(directory, "firstparty.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := EnsureFirstPartyPlugins(ctx, db); err == nil {
		t.Fatal("EnsureFirstPartyPlugins with an unreachable repository = nil error")
	}
	repository, err := db.GetPluginRepository(ctx, FirstPartyRepositoryID)
	if err != nil || repository.LastError == "" {
		t.Fatalf("repository = %#v, %v; want recorded LastError", repository, err)
	}
}

// stubFirstPartyDeps points the default-repository machinery at the lifecycle test server's key.
func stubFirstPartyDeps(t *testing.T) func() {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server, _ := releaseServer(t, key, []string{"1.0.0", "1.1.0"})
	t.Cleanup(server.Close)

	savedURL, savedKey, savedFetcher := firstPartyRepositoryURL, firstPartyPublisherKey, newFirstPartyFetcher
	firstPartyRepositoryURL = server.URL + "/index.toml"
	firstPartyPublisherKey = func() (*ecdsa.PublicKey, error) { return &key.PublicKey, nil }
	newFirstPartyFetcher = func() targetplugin.Fetcher { return targetplugin.Fetcher{Client: server.Client()} }

	savedRoot := conf.PluginsBasePath
	conf.PluginsBasePath = filepath.Join(t.TempDir(), "plugins")

	return func() {
		firstPartyRepositoryURL = savedURL
		firstPartyPublisherKey = savedKey
		newFirstPartyFetcher = savedFetcher
		conf.PluginsBasePath = savedRoot
	}
}
