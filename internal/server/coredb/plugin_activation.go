package coredb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

type PluginTargetMigration struct {
	Current       PluginTarget
	Migrated      PluginTarget
	RenameSecrets map[string]string
	DeleteSecrets []string
}

type PluginJobOptionsMigration struct {
	Current  PluginJobOptions
	Migrated PluginJobOptions
}

type PluginActivationMigration struct {
	PluginID       string
	FromVersion    string
	ToVersion      string
	Targets        []PluginTargetMigration
	BackupOptions  []PluginJobOptionsMigration
	RestoreOptions []PluginJobOptionsMigration
	MigratedAt     time.Time
}

func (db *Store) CommitPluginActivation(ctx context.Context, migration PluginActivationMigration) error {
	if err := validatePluginActivationMigration(migration); err != nil {
		return err
	}
	ctx = db.pluginContext(ctx)
	migratedAt := migration.MigratedAt
	if migratedAt.IsZero() {
		migratedAt = time.Now()
	}
	return db.RunInTransaction(ctx, func(_ *Transaction, queries *corequery.Queries) error {
		for _, target := range migration.Targets {
			if err := migratePluginTarget(ctx, queries, target, migratedAt); err != nil {
				return err
			}
		}
		for _, options := range migration.BackupOptions {
			if err := migrateBackupPluginOptions(ctx, queries, options, migratedAt); err != nil {
				return err
			}
		}
		for _, options := range migration.RestoreOptions {
			if err := migrateRestorePluginOptions(ctx, queries, options, migratedAt); err != nil {
				return err
			}
		}
		rows, err := queries.ActivateTargetPluginVersionFrom(ctx, corequery.ActivateTargetPluginVersionFromParams{
			ToVersion:   migration.ToVersion,
			PluginID:    migration.PluginID,
			FromVersion: migration.FromVersion,
		})
		if err != nil {
			return fmt.Errorf("activate migrated plugin version: %w", err)
		}
		if rows != 1 {
			return errors.New("plugin activation changed during migration")
		}
		return nil
	})
}

func validatePluginActivationMigration(migration PluginActivationMigration) error {
	if migration.PluginID == "" || migration.ToVersion == "" || migration.FromVersion == migration.ToVersion {
		return errors.New("plugin activation identity is invalid")
	}
	for _, target := range migration.Targets {
		if err := validatePluginTargetMigration(migration, target); err != nil {
			return err
		}
	}
	for _, options := range migration.BackupOptions {
		if err := validatePluginJobOptionsMigration(migration, options); err != nil {
			return fmt.Errorf("backup %w", err)
		}
	}
	for _, options := range migration.RestoreOptions {
		if err := validatePluginJobOptionsMigration(migration, options); err != nil {
			return fmt.Errorf("restore %w", err)
		}
	}
	return nil
}

func validatePluginTargetMigration(activation PluginActivationMigration, migration PluginTargetMigration) error {
	if err := validatePluginTarget(migration.Current); err != nil {
		return fmt.Errorf("current plugin target %q: %w", migration.Current.Name, err)
	}
	if err := validatePluginTarget(migration.Migrated); err != nil {
		return fmt.Errorf("migrated plugin target %q: %w", migration.Migrated.Name, err)
	}
	if migration.Current.Name != migration.Migrated.Name || migration.Current.PluginID != activation.PluginID ||
		migration.Migrated.PluginID != activation.PluginID || migration.Current.PluginVersion != activation.FromVersion ||
		migration.Migrated.PluginVersion != activation.ToVersion || migration.Current.TargetType != migration.Migrated.TargetType {
		return fmt.Errorf("plugin target %q migration identity changed", migration.Current.Name)
	}
	return nil
}

func validatePluginJobOptionsMigration(activation PluginActivationMigration, migration PluginJobOptionsMigration) error {
	if err := validatePluginJobOptions(migration.Current); err != nil {
		return fmt.Errorf("plugin options %q current value: %w", migration.Current.JobID, err)
	}
	if err := validatePluginJobOptions(migration.Migrated); err != nil {
		return fmt.Errorf("plugin options %q migrated value: %w", migration.Migrated.JobID, err)
	}
	if migration.Current.JobID != migration.Migrated.JobID || migration.Current.PluginID != activation.PluginID ||
		migration.Migrated.PluginID != activation.PluginID || migration.Current.PluginVersion != activation.FromVersion ||
		migration.Migrated.PluginVersion != activation.ToVersion {
		return fmt.Errorf("plugin options %q migration identity changed", migration.Current.JobID)
	}
	return nil
}

func migratePluginTarget(ctx context.Context, queries *corequery.Queries, migration PluginTargetMigration, migratedAt time.Time) error {
	if err := queries.ArchivePluginTargetConfig(ctx, corequery.ArchivePluginTargetConfigParams{
		MigratedAt: migratedAt.Unix(),
		TargetName: migration.Current.Name,
	}); err != nil {
		return fmt.Errorf("archive plugin target %q: %w", migration.Current.Name, err)
	}
	rows, err := queries.MigratePluginTargetConfig(ctx, corequery.MigratePluginTargetConfigParams{
		ToPluginVersion:   migration.Migrated.PluginVersion,
		ToSchemaVersion:   int64(migration.Migrated.SchemaVersion),
		ToConfig:          migration.Migrated.Config,
		ToUpdatedAt:       migratedAt.Unix(),
		TargetName:        migration.Current.Name,
		PluginID:          migration.Current.PluginID,
		FromPluginVersion: migration.Current.PluginVersion,
		FromSchemaVersion: int64(migration.Current.SchemaVersion),
		FromConfig:        migration.Current.Config,
		FromUpdatedAt:     migration.Current.UpdatedAt.Unix(),
	})
	if err != nil {
		return fmt.Errorf("migrate plugin target %q: %w", migration.Current.Name, err)
	}
	if rows != 1 {
		return fmt.Errorf("plugin target %q changed during migration", migration.Current.Name)
	}
	return migratePluginTargetSecrets(ctx, queries, migration)
}

func migratePluginTargetSecrets(ctx context.Context, queries *corequery.Queries, migration PluginTargetMigration) error {
	rows, err := queries.ListPluginTargetSecrets(ctx, migration.Current.Name)
	if err != nil {
		return fmt.Errorf("list plugin target %q secrets: %w", migration.Current.Name, err)
	}
	stored := make(map[string]string, len(rows))
	fields := make([]string, len(rows))
	for index, row := range rows {
		stored[row.FieldKey] = row.EncryptedValue
		fields[index] = row.FieldKey
	}
	expected := slices.Clone(migration.Current.SecretFields)
	sort.Strings(expected)
	if !slices.Equal(fields, expected) {
		return fmt.Errorf("plugin target %q secrets changed during migration", migration.Current.Name)
	}
	remove := make(map[string]struct{}, len(migration.RenameSecrets)+len(migration.DeleteSecrets))
	moved := make(map[string]string, len(migration.RenameSecrets))
	for from, to := range migration.RenameSecrets {
		value, ok := stored[from]
		if !ok {
			return fmt.Errorf("plugin target %q secret %q does not exist", migration.Current.Name, from)
		}
		remove[from] = struct{}{}
		moved[to] = value
	}
	for _, field := range migration.DeleteSecrets {
		if _, ok := stored[field]; !ok {
			return fmt.Errorf("plugin target %q secret %q does not exist", migration.Current.Name, field)
		}
		remove[field] = struct{}{}
	}
	for field := range stored {
		if _, removed := remove[field]; removed {
			continue
		}
		if _, collision := moved[field]; collision {
			return fmt.Errorf("plugin target %q secret migration collides at %q", migration.Current.Name, field)
		}
	}
	removedFields := make([]string, 0, len(remove))
	for field := range remove {
		removedFields = append(removedFields, field)
	}
	sort.Strings(removedFields)
	for _, field := range removedFields {
		if _, err := queries.DeletePluginTargetSecret(ctx, corequery.DeletePluginTargetSecretParams{
			TargetName: migration.Current.Name,
			FieldKey:   field,
		}); err != nil {
			return fmt.Errorf("delete plugin target %q secret %q: %w", migration.Current.Name, field, err)
		}
	}
	movedFields := make([]string, 0, len(moved))
	for field := range moved {
		movedFields = append(movedFields, field)
	}
	sort.Strings(movedFields)
	for _, field := range movedFields {
		if err := queries.UpsertPluginTargetSecret(ctx, corequery.UpsertPluginTargetSecretParams{
			TargetName:     migration.Current.Name,
			FieldKey:       field,
			EncryptedValue: moved[field],
		}); err != nil {
			return fmt.Errorf("rename plugin target %q secret to %q: %w", migration.Current.Name, field, err)
		}
	}
	return nil
}

func migrateBackupPluginOptions(ctx context.Context, queries *corequery.Queries, migration PluginJobOptionsMigration, migratedAt time.Time) error {
	if err := queries.ArchiveBackupPluginOptions(ctx, corequery.ArchiveBackupPluginOptionsParams{
		MigratedAt: migratedAt.Unix(),
		BackupID:   migration.Current.JobID,
	}); err != nil {
		return fmt.Errorf("archive backup plugin options %q: %w", migration.Current.JobID, err)
	}
	rows, err := queries.MigrateBackupPluginOptions(ctx, corequery.MigrateBackupPluginOptionsParams{
		ToPluginVersion:   migration.Migrated.PluginVersion,
		ToSchemaVersion:   int64(migration.Migrated.SchemaVersion),
		ToOptions:         migration.Migrated.Options,
		ToUpdatedAt:       migratedAt.Unix(),
		BackupID:          migration.Current.JobID,
		PluginID:          migration.Current.PluginID,
		FromPluginVersion: migration.Current.PluginVersion,
		FromSchemaVersion: int64(migration.Current.SchemaVersion),
		FromOptions:       migration.Current.Options,
		FromUpdatedAt:     migration.Current.UpdatedAt.Unix(),
	})
	if err != nil {
		return fmt.Errorf("migrate backup plugin options %q: %w", migration.Current.JobID, err)
	}
	if rows != 1 {
		return fmt.Errorf("backup plugin options %q changed during migration", migration.Current.JobID)
	}
	return nil
}

func migrateRestorePluginOptions(ctx context.Context, queries *corequery.Queries, migration PluginJobOptionsMigration, migratedAt time.Time) error {
	if err := queries.ArchiveRestorePluginOptions(ctx, corequery.ArchiveRestorePluginOptionsParams{
		MigratedAt: migratedAt.Unix(),
		RestoreID:  migration.Current.JobID,
	}); err != nil {
		return fmt.Errorf("archive restore plugin options %q: %w", migration.Current.JobID, err)
	}
	rows, err := queries.MigrateRestorePluginOptions(ctx, corequery.MigrateRestorePluginOptionsParams{
		ToPluginVersion:   migration.Migrated.PluginVersion,
		ToSchemaVersion:   int64(migration.Migrated.SchemaVersion),
		ToOptions:         migration.Migrated.Options,
		ToUpdatedAt:       migratedAt.Unix(),
		RestoreID:         migration.Current.JobID,
		PluginID:          migration.Current.PluginID,
		FromPluginVersion: migration.Current.PluginVersion,
		FromSchemaVersion: int64(migration.Current.SchemaVersion),
		FromOptions:       migration.Current.Options,
		FromUpdatedAt:     migration.Current.UpdatedAt.Unix(),
	})
	if err != nil {
		return fmt.Errorf("migrate restore plugin options %q: %w", migration.Current.JobID, err)
	}
	if rows != 1 {
		return fmt.Errorf("restore plugin options %q changed during migration", migration.Current.JobID)
	}
	return nil
}
