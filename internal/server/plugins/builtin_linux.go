//go:build linux

package plugins

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/agentfs"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/dovecot"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/ldap"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/mysql"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/postgresql"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/s3"
)

// BuiltinRepositoryID owns first-party plugins that ship inside the server package instead of a network repository.
const BuiltinRepositoryID = "org.pbs-plus.builtin"

type builtinPlugin struct {
	descriptor targetplugin.Descriptor
	artifact   string
}

func builtinPlugins() []builtinPlugin {
	return []builtinPlugin{
		{descriptor: filesystem.Descriptor(), artifact: "pbs-plus-plugin-filesystem"},
		{descriptor: agentfs.Descriptor(), artifact: "pbs-plus-plugin-agentfs"},
		{descriptor: s3.Descriptor(), artifact: "pbs-plus-plugin-s3"},
		{descriptor: postgresql.Descriptor(), artifact: "pbs-plus-plugin-postgresql"},
		{descriptor: mysql.Descriptor(), artifact: "pbs-plus-plugin-mysql"},
		{descriptor: ldap.Descriptor(), artifact: "pbs-plus-plugin-ldap"},
		{descriptor: dovecot.Descriptor(), artifact: "pbs-plus-plugin-dovecot"},
	}
}

// InstallBuiltins registers every first-party plugin artifact found in artifactDir, skipping versions already installed.
func InstallBuiltins(ctx context.Context, db *coredb.Store, artifactDir string) (int, error) {
	installed := 0
	var failures error
	for _, builtin := range builtinPlugins() {
		artifact := filepath.Join(artifactDir, builtin.artifact)
		if _, err := os.Stat(artifact); err != nil {
			continue
		}
		added, err := installBuiltin(ctx, db, builtin, artifact)
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("install %s: %w", builtin.descriptor.PluginID, err))
			continue
		}
		if added {
			installed++
		}
	}
	return installed, failures
}

func installBuiltin(ctx context.Context, db *coredb.Store, builtin builtinPlugin, artifact string) (bool, error) {
	descriptor := builtin.descriptor
	if _, err := db.GetInstalledPluginVersion(ctx, descriptor.PluginID, descriptor.Version); err == nil {
		return false, nil
	}
	manifestBytes, err := builtinManifest(descriptor)
	if err != nil {
		return false, err
	}
	version, err := targetplugin.InstallBundledVersion(ctx, conf.PluginsBasePath, manifestBytes, artifact)
	if err != nil && !errors.Is(err, targetplugin.ErrVersionInstalled) {
		return false, err
	}
	if errors.Is(err, targetplugin.ErrVersionInstalled) {
		version.Directory = filepath.Join(conf.PluginsBasePath, descriptor.PluginID, descriptor.Version)
	}
	if err := ensureBuiltinRepository(ctx, db); err != nil {
		return false, err
	}
	digest, err := fileDigest(artifact)
	if err != nil {
		return false, err
	}
	plugin, err := db.GetInstalledPlugin(ctx, descriptor.PluginID)
	activate := err != nil || plugin.ActiveVersion == ""
	if err := db.RegisterPluginVersion(ctx, BuiltinRepositoryID, coredb.InstalledPluginVersion{
		PluginID:       descriptor.PluginID,
		Version:        descriptor.Version,
		Platform:       runtime.GOOS + "/" + runtime.GOARCH,
		InstallPath:    version.Directory,
		Manifest:       manifestBytes,
		ArtifactSHA256: digest,
		InstalledAt:    time.Now().UTC(),
		HealthState:    coredb.PluginHealthUnknown,
	}, activate); err != nil {
		return false, err
	}
	return true, nil
}

func builtinManifest(descriptor targetplugin.Descriptor) ([]byte, error) {
	schemaDigest, err := targetplugin.SchemaDigest(descriptor)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("encode builtin manifest: %w", err)
	}
	return manifest, nil
}

func ensureBuiltinRepository(ctx context.Context, db *coredb.Store) error {
	if _, err := db.GetPluginRepository(ctx, BuiltinRepositoryID); err == nil {
		return nil
	}
	return db.CreatePluginRepository(ctx, coredb.PluginRepository{
		ID:        BuiltinRepositoryID,
		Name:      "PBS Plus built-in plugins",
		URL:       "https://github.com/pbs-plus/pbs-plus",
		PublicKey: []byte(BuiltinRepositoryID),
		Enabled:   true,
	})
}

func fileDigest(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
