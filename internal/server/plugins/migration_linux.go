//go:build linux

package plugins

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func ActivateVersion(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, pluginID, version string) error {
	plugin, err := db.GetInstalledPlugin(ctx, pluginID)
	if err != nil {
		return err
	}
	if plugin.ActiveVersion == version {
		return nil
	}
	manifest, installed, err := loadInstalledManifest(ctx, db, pluginID, version)
	if err != nil {
		return err
	}
	targets, targetByName, err := prepareTargetMigrations(ctx, db, supervisor, manifest, installed, plugin.ActiveVersion)
	if err != nil {
		return err
	}
	backups, err := prepareBackupOptionMigrations(ctx, db, supervisor, manifest, installed, plugin.ActiveVersion, targetByName)
	if err != nil {
		return err
	}
	restores, err := prepareRestoreOptionMigrations(ctx, db, supervisor, manifest, installed, plugin.ActiveVersion, targetByName)
	if err != nil {
		return err
	}
	return db.CommitPluginActivation(ctx, coredb.PluginActivationMigration{
		PluginID:       pluginID,
		FromVersion:    plugin.ActiveVersion,
		ToVersion:      version,
		Targets:        targets,
		BackupOptions:  backups,
		RestoreOptions: restores,
	})
}

func prepareTargetMigrations(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, fromVersion string) ([]coredb.PluginTargetMigration, map[string]coredb.PluginTarget, error) {
	rows, err := db.ListPluginTargets(ctx, installed.PluginID)
	if err != nil {
		return nil, nil, err
	}
	migrations := make([]coredb.PluginTargetMigration, 0, len(rows))
	targets := make(map[string]coredb.PluginTarget, len(rows))
	for _, row := range rows {
		current, err := db.GetPluginTarget(ctx, row.Name)
		if err != nil {
			return nil, nil, err
		}
		if current.PluginVersion != fromVersion {
			return nil, nil, fmt.Errorf("plugin target %q uses version %q instead of active version %q", current.Name, current.PluginVersion, fromVersion)
		}
		if !slices.Contains(manifest.TargetTypes, current.TargetType) {
			return nil, nil, fmt.Errorf("plugin version %q does not provide target type %q", installed.Version, current.TargetType)
		}
		migration, err := prepareTargetMigration(ctx, supervisor, manifest, installed, current)
		if err != nil {
			return nil, nil, fmt.Errorf("migrate plugin target %q: %w", current.Name, err)
		}
		migrations = append(migrations, migration)
		targets[current.Name] = current
	}
	return migrations, targets, nil
}

func prepareTargetMigration(ctx context.Context, supervisor *targetplugin.Supervisor, manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, current coredb.PluginTarget) (coredb.PluginTargetMigration, error) {
	var values targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(current.Config, &values); err != nil {
		return coredb.PluginTargetMigration{}, fmt.Errorf("decode config: %w", err)
	}
	response := targetplugin.MigrateResponse{Values: values}
	if current.SchemaVersion != manifest.TargetSchema.Version {
		migrated, err := invokeTargetMigration(ctx, supervisor, installed, current, manifest.TargetSchema.Version, values)
		if err != nil {
			return coredb.PluginTargetMigration{}, err
		}
		response = targetplugin.MigrateResponse(migrated)
	}
	secretPresent, secretFields, err := applySecretEdits(current.SecretFields, response)
	if err != nil {
		return coredb.PluginTargetMigration{}, err
	}
	if err := targetplugin.ValidateFormConfig(manifest.TargetSchema, response.Values, secretPresent); err != nil {
		return coredb.PluginTargetMigration{}, fmt.Errorf("validate migrated config: %w", err)
	}
	encoded, err := targetplugin.MarshalProtocol(response.Values)
	if err != nil {
		return coredb.PluginTargetMigration{}, fmt.Errorf("encode migrated config: %w", err)
	}
	migrated := current
	migrated.PluginVersion = installed.Version
	migrated.SchemaVersion = manifest.TargetSchema.Version
	migrated.Config = encoded
	migrated.SecretFields = secretFields
	return coredb.PluginTargetMigration{
		Current:       current,
		Migrated:      migrated,
		RenameSecrets: response.RenameSecrets,
		DeleteSecrets: response.DeleteSecrets,
	}, nil
}

func prepareBackupOptionMigrations(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, fromVersion string, targets map[string]coredb.PluginTarget) ([]coredb.PluginJobOptionsMigration, error) {
	rows, err := db.ListBackupPluginOptionsByPlugin(ctx, installed.PluginID)
	if err != nil {
		return nil, err
	}
	migrations := make([]coredb.PluginJobOptionsMigration, 0, len(rows))
	for _, current := range rows {
		if current.PluginVersion != fromVersion {
			return nil, fmt.Errorf("backup %q uses plugin version %q instead of active version %q", current.JobID, current.PluginVersion, fromVersion)
		}
		backup, err := db.GetBackup(current.JobID)
		if err != nil {
			return nil, err
		}
		target, ok := targets[backup.Target.Name]
		if !ok {
			return nil, fmt.Errorf("backup %q does not reference a target owned by plugin %q", current.JobID, installed.PluginID)
		}
		migration, err := prepareBackupOptionMigration(ctx, supervisor, manifest, installed, target.TargetType, current)
		if err != nil {
			return nil, fmt.Errorf("migrate backup %q options: %w", current.JobID, err)
		}
		migrations = append(migrations, migration)
	}
	return migrations, nil
}

func prepareRestoreOptionMigrations(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, fromVersion string, targets map[string]coredb.PluginTarget) ([]coredb.PluginJobOptionsMigration, error) {
	rows, err := db.ListRestorePluginOptionsByPlugin(ctx, installed.PluginID)
	if err != nil {
		return nil, err
	}
	migrations := make([]coredb.PluginJobOptionsMigration, 0, len(rows))
	for _, current := range rows {
		if current.PluginVersion != fromVersion {
			return nil, fmt.Errorf("restore %q uses plugin version %q instead of active version %q", current.JobID, current.PluginVersion, fromVersion)
		}
		restore, err := db.GetRestore(current.JobID)
		if err != nil {
			return nil, err
		}
		target, ok := targets[restore.DestTarget.Name]
		if !ok {
			return nil, fmt.Errorf("restore %q does not reference a target owned by plugin %q", current.JobID, installed.PluginID)
		}
		migration, err := prepareRestoreOptionMigration(ctx, supervisor, manifest, installed, target.TargetType, current)
		if err != nil {
			return nil, fmt.Errorf("migrate restore %q options: %w", current.JobID, err)
		}
		migrations = append(migrations, migration)
	}
	return migrations, nil
}

func prepareBackupOptionMigration(ctx context.Context, supervisor *targetplugin.Supervisor, manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, targetType string, current coredb.PluginJobOptions) (coredb.PluginJobOptionsMigration, error) {
	values, err := decodePluginJobOptions(current)
	if err != nil {
		return coredb.PluginJobOptionsMigration{}, err
	}
	if current.SchemaVersion != manifest.BackupSchema.Version {
		response, err := invokeBackupOptionMigration(ctx, supervisor, installed, targetType, current.SchemaVersion, manifest.BackupSchema.Version, values)
		if err != nil {
			return coredb.PluginJobOptionsMigration{}, err
		}
		if len(response.RenameSecrets) != 0 || len(response.DeleteSecrets) != 0 {
			return coredb.PluginJobOptionsMigration{}, errors.New("backup option migration returned secret edits")
		}
		values = response.Values
	}
	return migratedPluginJobOptions(current, installed.Version, manifest.BackupSchema, values)
}

func prepareRestoreOptionMigration(ctx context.Context, supervisor *targetplugin.Supervisor, manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, targetType string, current coredb.PluginJobOptions) (coredb.PluginJobOptionsMigration, error) {
	values, err := decodePluginJobOptions(current)
	if err != nil {
		return coredb.PluginJobOptionsMigration{}, err
	}
	if current.SchemaVersion != manifest.RestoreSchema.Version {
		response, err := invokeRestoreOptionMigration(ctx, supervisor, installed, targetType, current.SchemaVersion, manifest.RestoreSchema.Version, values)
		if err != nil {
			return coredb.PluginJobOptionsMigration{}, err
		}
		if len(response.RenameSecrets) != 0 || len(response.DeleteSecrets) != 0 {
			return coredb.PluginJobOptionsMigration{}, errors.New("restore option migration returned secret edits")
		}
		values = response.Values
	}
	return migratedPluginJobOptions(current, installed.Version, manifest.RestoreSchema, values)
}

func decodePluginJobOptions(options coredb.PluginJobOptions) (targetplugin.Values, error) {
	var values targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(options.Options, &values); err != nil {
		return nil, fmt.Errorf("decode options: %w", err)
	}
	return values, nil
}

func migratedPluginJobOptions(current coredb.PluginJobOptions, version string, schema targetplugin.FormSchema, values targetplugin.Values) (coredb.PluginJobOptionsMigration, error) {
	if err := targetplugin.ValidateFormConfig(schema, values, nil); err != nil {
		return coredb.PluginJobOptionsMigration{}, fmt.Errorf("validate migrated options: %w", err)
	}
	encoded, err := targetplugin.MarshalProtocol(values)
	if err != nil {
		return coredb.PluginJobOptionsMigration{}, fmt.Errorf("encode migrated options: %w", err)
	}
	migrated := current
	migrated.PluginVersion = version
	migrated.SchemaVersion = schema.Version
	migrated.Options = encoded
	return coredb.PluginJobOptionsMigration{Current: current, Migrated: migrated}, nil
}

func invokeTargetMigration(ctx context.Context, supervisor *targetplugin.Supervisor, installed coredb.InstalledPluginVersion, target coredb.PluginTarget, toVersion uint32, values targetplugin.Values) (targetplugin.TargetMigrateResponse, error) {
	ctx, cancel := pluginMigrationContext(ctx)
	defer cancel()
	request := targetplugin.TargetMigrateRequest(targetplugin.MigrateRequest{
		Operation:         pluginMigrationOperation(ctx, installed.Version, target.TargetType, target.SchemaVersion, "target-migrate"),
		FromSchemaVersion: target.SchemaVersion,
		ToSchemaVersion:   toVersion,
		Values:            values,
		SecretFields:      target.SecretFields,
	})
	var response targetplugin.TargetMigrateResponse
	err := supervisor.Run(ctx, installed.PluginID, installed.InstallPath, func(runCtx context.Context, process *targetplugin.Process) error {
		request.Operation.BrokerToken = process.BrokerToken()
		return process.Invoke(runCtx, targetplugin.MethodTargetMigrate, request, &response)
	})
	return response, err
}

func invokeBackupOptionMigration(ctx context.Context, supervisor *targetplugin.Supervisor, installed coredb.InstalledPluginVersion, targetType string, fromVersion, toVersion uint32, values targetplugin.Values) (targetplugin.BackupMigrateOptionsResponse, error) {
	ctx, cancel := pluginMigrationContext(ctx)
	defer cancel()
	request := targetplugin.BackupMigrateOptionsRequest(targetplugin.MigrateRequest{
		Operation:         pluginMigrationOperation(ctx, installed.Version, targetType, fromVersion, "backup-migrate"),
		FromSchemaVersion: fromVersion,
		ToSchemaVersion:   toVersion,
		Values:            values,
	})
	var response targetplugin.BackupMigrateOptionsResponse
	err := supervisor.Run(ctx, installed.PluginID, installed.InstallPath, func(runCtx context.Context, process *targetplugin.Process) error {
		request.Operation.BrokerToken = process.BrokerToken()
		return process.Invoke(runCtx, targetplugin.MethodBackupMigrateOptions, request, &response)
	})
	return response, err
}

func invokeRestoreOptionMigration(ctx context.Context, supervisor *targetplugin.Supervisor, installed coredb.InstalledPluginVersion, targetType string, fromVersion, toVersion uint32, values targetplugin.Values) (targetplugin.RestoreMigrateOptionsResponse, error) {
	ctx, cancel := pluginMigrationContext(ctx)
	defer cancel()
	request := targetplugin.RestoreMigrateOptionsRequest(targetplugin.MigrateRequest{
		Operation:         pluginMigrationOperation(ctx, installed.Version, targetType, fromVersion, "restore-migrate"),
		FromSchemaVersion: fromVersion,
		ToSchemaVersion:   toVersion,
		Values:            values,
	})
	var response targetplugin.RestoreMigrateOptionsResponse
	err := supervisor.Run(ctx, installed.PluginID, installed.InstallPath, func(runCtx context.Context, process *targetplugin.Process) error {
		request.Operation.BrokerToken = process.BrokerToken()
		return process.Invoke(runCtx, targetplugin.MethodRestoreMigrateOptions, request, &response)
	})
	return response, err
}

func pluginMigrationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, targetOperationTimeout)
}

func pluginMigrationOperation(ctx context.Context, version, targetType string, schemaVersion uint32, prefix string) targetplugin.Operation {
	deadline, _ := ctx.Deadline()
	operationID := uuid.NewString()
	return targetplugin.Operation{
		ProtocolVersion:   targetplugin.CurrentProtocolVersion,
		ID:                operationID,
		IdempotencyKey:    prefix + ":" + operationID,
		DeadlineUnixMilli: deadline.UnixMilli(),
		PluginVersion:     version,
		TargetType:        targetType,
		SchemaVersion:     schemaVersion,
	}
}

func applySecretEdits(current []string, migration targetplugin.MigrateResponse) (map[string]bool, []string, error) {
	fields := make(map[string]bool, len(current))
	for _, field := range current {
		fields[field] = true
	}
	remove := make(map[string]struct{}, len(migration.RenameSecrets)+len(migration.DeleteSecrets))
	for from := range migration.RenameSecrets {
		if !fields[from] {
			return nil, nil, fmt.Errorf("secret %q does not exist", from)
		}
		remove[from] = struct{}{}
	}
	for _, field := range migration.DeleteSecrets {
		if !fields[field] {
			return nil, nil, fmt.Errorf("secret %q does not exist", field)
		}
		remove[field] = struct{}{}
	}
	for field := range remove {
		delete(fields, field)
	}
	for _, to := range migration.RenameSecrets {
		if fields[to] {
			return nil, nil, fmt.Errorf("secret migration collides at %q", to)
		}
		fields[to] = true
	}
	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	slices.Sort(result)
	return fields, result, nil
}
