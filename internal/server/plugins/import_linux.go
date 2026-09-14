//go:build linux

package plugins

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/postgresql"
)

type importConfig func(coredb.Target) (targetplugin.Values, map[string][]byte, error)

// ImportLocalTargets attaches plugin configs to every legacy local filesystem target.
func ImportLocalTargets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, filesystem.PluginID, filesystem.TargetTypeLocal,
		func(target coredb.Target) bool { return target.IsLocal() },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			return targetplugin.Values{"path": targetplugin.NewStringScalar(target.Path)}, nil, nil
		})
}

// ImportPostgreSQLTargets attaches plugin configs to every legacy PostgreSQL target,
// copying the stored password into the plugin secret store so both paths stay usable.
func ImportPostgreSQLTargets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, postgresql.PluginID, postgresql.TargetType,
		func(target coredb.Target) bool { return target.Type == coredb.TargetTypePostgreSQL },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			password, err := db.GetDatabasePassword(target.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("get database password for %q: %w", target.Name, err)
			}
			config := targetplugin.Values{
				"host":     targetplugin.NewStringScalar(target.DatabaseHost),
				"port":     targetplugin.NewIntegerScalar(int64(target.DatabasePort)),
				"username": targetplugin.NewStringScalar(target.DatabaseUsername),
			}
			if target.DatabaseTLSMode != "" {
				config["tls_mode"] = targetplugin.NewStringScalar(target.DatabaseTLSMode)
			}
			if target.DatabaseCACertificate != "" {
				config["ca_certificate"] = targetplugin.NewStringScalar(target.DatabaseCACertificate)
			}
			if target.DatabaseDefaultClientDir != "" {
				config["default_client_dir"] = targetplugin.NewStringScalar(target.DatabaseDefaultClientDir)
			}
			return config, map[string][]byte{"password": []byte(password)}, nil
		})
}

func importTargets(
	ctx context.Context,
	db *coredb.Store,
	pluginID string,
	targetType string,
	matches func(coredb.Target) bool,
	build importConfig,
) (int, error) {
	plugin, err := db.GetInstalledPlugin(ctx, pluginID)
	if err != nil {
		return 0, fmt.Errorf("plugin %s is not installed: %w", pluginID, err)
	}
	if !plugin.Enabled || plugin.ActiveVersion == "" {
		return 0, fmt.Errorf("plugin %s is not enabled", pluginID)
	}
	manifest, _, err := loadActiveManifest(ctx, db, pluginID)
	if err != nil {
		return 0, err
	}

	targets, err := db.GetAllTargets()
	if err != nil {
		return 0, err
	}
	imported := 0
	for _, target := range targets {
		if !matches(target) {
			continue
		}
		if _, err := db.GetPluginTarget(ctx, target.Name); err == nil {
			continue
		} else if !errors.Is(err, coredb.ErrTargetNotFound) {
			return imported, err
		}
		values, secrets, err := build(target)
		if err != nil {
			return imported, err
		}
		config, err := targetplugin.MarshalProtocol(values)
		if err != nil {
			return imported, err
		}
		secretFields := make([]string, 0, len(secrets))
		for field := range secrets {
			secretFields = append(secretFields, field)
		}
		slices.Sort(secretFields)
		if err := db.AttachPluginTarget(ctx, coredb.PluginTarget{
			Name:          target.Name,
			PluginID:      pluginID,
			PluginVersion: plugin.ActiveVersion,
			TargetType:    targetType,
			SchemaVersion: manifest.TargetSchema.Version,
			Config:        config,
			SecretFields:  secretFields,
		}, secrets); err != nil {
			return imported, err
		}
		imported++
	}
	return imported, nil
}
