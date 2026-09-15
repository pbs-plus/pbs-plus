//go:build linux

package plugins

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

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

type importConfig func(coredb.Target) (targetplugin.Values, map[string][]byte, error)

func optionalString(config targetplugin.Values, key, value string) {
	if value != "" {
		config[key] = targetplugin.NewStringScalar(value)
	}
}

// ImportLocalTargets attaches plugin configs to every legacy local filesystem target.
func ImportLocalTargets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, filesystem.PluginID, filesystem.TargetTypeLocal,
		func(target coredb.Target) bool { return target.IsLocal() },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			return targetplugin.Values{"path": targetplugin.NewStringScalar(target.Path)}, nil, nil
		})
}

// ImportAgentTargets attaches plugin configs to every legacy agent filesystem target.
func ImportAgentTargets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, agentfs.PluginID, agentfs.TargetType,
		func(target coredb.Target) bool { return target.IsAgent() },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			operatingSystem := target.AgentHost.OperatingSystem
			if operatingSystem == "" {
				operatingSystem = "linux"
			}
			return targetplugin.Values{
				"hostname":         targetplugin.NewStringScalar(target.GetHostname()),
				"volume_id":        targetplugin.NewStringScalar(target.VolumeID),
				"operating_system": targetplugin.NewStringScalar(operatingSystem),
			}, nil, nil
		})
}

// ImportS3Targets copies legacy S3 settings into the encrypted plugin store while preserving the public target row.
func ImportS3Targets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, s3.PluginID, s3.TargetType,
		func(target coredb.Target) bool { return target.IsS3() },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			if target.S3Info == nil {
				return nil, nil, fmt.Errorf("S3 target %q has no parsed configuration", target.Name)
			}
			secret, err := db.GetS3Secret(target.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("get S3 secret for %q: %w", target.Name, err)
			}
			config := targetplugin.Values{
				"endpoint":   targetplugin.NewStringScalar(target.S3Info.Endpoint),
				"bucket":     targetplugin.NewStringScalar(target.S3Info.Bucket),
				"access_key": targetplugin.NewStringScalar(target.S3Info.AccessKey),
				"use_ssl":    targetplugin.NewBooleanScalar(target.S3Info.UseSSL),
				"path_style": targetplugin.NewBooleanScalar(target.S3Info.IsPathStyle),
			}
			optionalString(config, "region", target.S3Info.Region)
			optionalString(config, "prefix", target.S3Info.Key)
			return config, map[string][]byte{"secret_key": []byte(secret)}, nil
		})
}

// ImportPostgreSQLTargets preserves the legacy target while copying its password into the plugin secret store.
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
			optionalString(config, "tls_mode", target.DatabaseTLSMode)
			optionalString(config, "ca_certificate", target.DatabaseCACertificate)
			optionalString(config, "default_client_dir", target.DatabaseDefaultClientDir)
			return config, map[string][]byte{"password": []byte(password)}, nil
		})
}

func ImportMySQLTargets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, mysql.PluginID, mysql.TargetType,
		func(target coredb.Target) bool { return target.Type == coredb.TargetTypeMySQL },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			password, err := db.GetDatabasePassword(target.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("get database password for %q: %w", target.Name, err)
			}
			config := targetplugin.Values{
				"host":          targetplugin.NewStringScalar(target.DatabaseHost),
				"port":          targetplugin.NewIntegerScalar(int64(target.DatabasePort)),
				"username":      targetplugin.NewStringScalar(target.DatabaseUsername),
				"variant":       targetplugin.NewStringScalar(target.DatabaseVariant),
				"client_family": targetplugin.NewStringScalar(target.DatabaseClientFamily),
			}
			optionalString(config, "tls_mode", target.DatabaseTLSMode)
			optionalString(config, "ca_certificate", target.DatabaseCACertificate)
			optionalString(config, "default_client_dir", target.DatabaseDefaultClientDir)
			return config, map[string][]byte{"password": []byte(password)}, nil
		})
}

func ImportLDAPTargets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, ldap.PluginID, ldap.TargetType,
		func(target coredb.Target) bool { return target.Type == coredb.TargetTypeLDAP },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			password, err := db.GetDatabasePassword(target.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("get database password for %q: %w", target.Name, err)
			}
			config := targetplugin.Values{
				"host":     targetplugin.NewStringScalar(target.DatabaseHost),
				"port":     targetplugin.NewIntegerScalar(int64(target.DatabasePort)),
				"username": targetplugin.NewStringScalar(target.DatabaseUsername),
				"base_dn":  targetplugin.NewStringScalar(target.LdapBaseDN),
			}
			optionalString(config, "tls_mode", target.DatabaseTLSMode)
			optionalString(config, "ca_certificate", target.DatabaseCACertificate)
			optionalString(config, "default_client_dir", target.DatabaseDefaultClientDir)
			return config, map[string][]byte{"password": []byte(password)}, nil
		})
}

func ImportDovecotTargets(ctx context.Context, db *coredb.Store) (int, error) {
	return importTargets(ctx, db, dovecot.PluginID, dovecot.TargetType,
		func(target coredb.Target) bool { return target.Type == coredb.TargetTypeDovecot },
		func(target coredb.Target) (targetplugin.Values, map[string][]byte, error) {
			password, err := db.GetDatabasePassword(target.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("get Dovecot password for %q: %w", target.Name, err)
			}
			config := targetplugin.Values{
				"host": targetplugin.NewStringScalar(target.DatabaseHost),
				"port": targetplugin.NewIntegerScalar(int64(target.DatabasePort)),
			}
			optionalString(config, "ca_certificate", target.DatabaseCACertificate)
			optionalString(config, "default_client_dir", target.DatabaseDefaultClientDir)
			return config, map[string][]byte{"password": []byte(password)}, nil
		})
}

func ImportFirstPartyTargets(ctx context.Context, db *coredb.Store) (int, error) {
	importers := []func(context.Context, *coredb.Store) (int, error){
		ImportLocalTargets,
		ImportAgentTargets,
		ImportS3Targets,
		ImportPostgreSQLTargets,
		ImportMySQLTargets,
		ImportLDAPTargets,
		ImportDovecotTargets,
	}
	total := 0
	for _, importer := range importers {
		imported, err := importer(ctx, db)
		if err != nil {
			return total, err
		}
		total += imported
	}
	return total, nil
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
		desired := coredb.PluginTarget{
			Name:          target.Name,
			PluginID:      pluginID,
			PluginVersion: plugin.ActiveVersion,
			TargetType:    targetType,
			SchemaVersion: manifest.TargetSchema.Version,
			Config:        config,
			SecretFields:  secretFields,
		}
		existing, err := db.GetPluginTarget(ctx, target.Name)
		if errors.Is(err, coredb.ErrTargetNotFound) {
			if err := db.AttachPluginTarget(ctx, desired, secrets); err != nil {
				return imported, err
			}
			imported++
			continue
		}
		if err != nil {
			return imported, err
		}
		if existing.PluginID != pluginID || existing.TargetType != targetType {
			return imported, fmt.Errorf("target %q is attached to plugin %s type %s", target.Name, existing.PluginID, existing.TargetType)
		}
		storedSecrets, err := db.ResolvePluginTargetSecrets(ctx, target.Name)
		if err != nil {
			return imported, err
		}
		if existing.PluginVersion == desired.PluginVersion && existing.SchemaVersion == desired.SchemaVersion && bytes.Equal(existing.Config, desired.Config) && equalSecrets(storedSecrets, secrets) {
			continue
		}
		deleteSecrets := make([]string, 0)
		for field := range storedSecrets {
			if _, keep := secrets[field]; !keep {
				deleteSecrets = append(deleteSecrets, field)
			}
		}
		if err := db.SyncAttachedPluginTarget(ctx, desired, secrets, deleteSecrets); err != nil {
			return imported, err
		}
		imported++
	}
	return imported, nil
}

func equalSecrets(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for field, value := range left {
		rightValue, ok := right[field]
		if !ok || !bytes.Equal(value, rightValue) {
			return false
		}
	}
	return true
}
