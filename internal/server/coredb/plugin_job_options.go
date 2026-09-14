package coredb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

const maxPluginJobOptionsBytes = 4 << 20

type PluginJobOptions struct {
	JobID         string
	PluginID      string
	PluginVersion string
	SchemaVersion uint32
	Options       []byte
	UpdatedAt     time.Time
}

type PluginJobOptionsHistory struct {
	ID            int64
	JobID         string
	PluginID      string
	PluginVersion string
	SchemaVersion uint32
	Options       []byte
	MigratedAt    time.Time
}

func (db *Store) UpsertBackupPluginOptions(ctx context.Context, options PluginJobOptions) error {
	if err := validatePluginJobOptions(options); err != nil {
		return err
	}
	return db.queries.UpsertBackupPluginOptions(db.pluginContext(ctx), corequery.UpsertBackupPluginOptionsParams{
		BackupID:      options.JobID,
		PluginID:      options.PluginID,
		PluginVersion: options.PluginVersion,
		SchemaVersion: int64(options.SchemaVersion),
		Options:       options.Options,
		UpdatedAt:     pluginJobOptionsUpdatedAt(options).Unix(),
	})
}

func (db *Store) GetBackupPluginOptions(ctx context.Context, backupID string) (PluginJobOptions, error) {
	row, err := db.readQueries.GetBackupPluginOptions(db.pluginContext(ctx), backupID)
	if err != nil {
		return PluginJobOptions{}, fmt.Errorf("get backup plugin options: %w", err)
	}
	return backupPluginOptionsFromRow(row), nil
}

func (db *Store) storeBackupPluginOptions(q *corequery.Queries, backup Backup) error {
	if backup.PluginOptions == nil {
		return q.DeleteBackupPluginOptions(db.ctx, backup.ID)
	}
	options := *backup.PluginOptions
	options.JobID = backup.ID
	if err := validatePluginJobOptions(options); err != nil {
		return err
	}
	return q.UpsertBackupPluginOptions(db.ctx, corequery.UpsertBackupPluginOptionsParams{
		BackupID:      options.JobID,
		PluginID:      options.PluginID,
		PluginVersion: options.PluginVersion,
		SchemaVersion: int64(options.SchemaVersion),
		Options:       options.Options,
		UpdatedAt:     pluginJobOptionsUpdatedAt(options).Unix(),
	})
}

func (db *Store) loadBackupPluginOptions(backupID string) (*PluginJobOptions, error) {
	row, err := db.readQueries.GetBackupPluginOptions(db.ctx, backupID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	options := backupPluginOptionsFromRow(row)
	return &options, nil
}

func (db *Store) ListBackupPluginOptions(ctx context.Context, pluginID, pluginVersion string) ([]PluginJobOptions, error) {
	rows, err := db.readQueries.ListBackupPluginOptionsByPluginVersion(db.pluginContext(ctx), corequery.ListBackupPluginOptionsByPluginVersionParams{
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("list backup plugin options: %w", err)
	}
	options := make([]PluginJobOptions, len(rows))
	for index, row := range rows {
		options[index] = backupPluginOptionsFromRow(row)
	}
	return options, nil
}

func (db *Store) ListBackupPluginOptionHistory(ctx context.Context, backupID string) ([]PluginJobOptionsHistory, error) {
	rows, err := db.readQueries.ListBackupPluginOptionHistory(db.pluginContext(ctx), backupID)
	if err != nil {
		return nil, fmt.Errorf("list backup plugin option history: %w", err)
	}
	history := make([]PluginJobOptionsHistory, len(rows))
	for index, row := range rows {
		history[index] = PluginJobOptionsHistory{
			ID:            row.ID,
			JobID:         row.BackupID,
			PluginID:      row.PluginID,
			PluginVersion: row.PluginVersion,
			SchemaVersion: uint32(row.SchemaVersion),
			Options:       row.Options,
			MigratedAt:    time.Unix(row.MigratedAt, 0),
		}
	}
	return history, nil
}

func (db *Store) UpsertRestorePluginOptions(ctx context.Context, options PluginJobOptions) error {
	if err := validatePluginJobOptions(options); err != nil {
		return err
	}
	return db.queries.UpsertRestorePluginOptions(db.pluginContext(ctx), corequery.UpsertRestorePluginOptionsParams{
		RestoreID:     options.JobID,
		PluginID:      options.PluginID,
		PluginVersion: options.PluginVersion,
		SchemaVersion: int64(options.SchemaVersion),
		Options:       options.Options,
		UpdatedAt:     pluginJobOptionsUpdatedAt(options).Unix(),
	})
}

func (db *Store) GetRestorePluginOptions(ctx context.Context, restoreID string) (PluginJobOptions, error) {
	row, err := db.readQueries.GetRestorePluginOptions(db.pluginContext(ctx), restoreID)
	if err != nil {
		return PluginJobOptions{}, fmt.Errorf("get restore plugin options: %w", err)
	}
	return restorePluginOptionsFromRow(row), nil
}

func (db *Store) storeRestorePluginOptions(q *corequery.Queries, restore Restore) error {
	if restore.PluginOptions == nil {
		return q.DeleteRestorePluginOptions(db.ctx, restore.ID)
	}
	options := *restore.PluginOptions
	options.JobID = restore.ID
	if err := validatePluginJobOptions(options); err != nil {
		return err
	}
	return q.UpsertRestorePluginOptions(db.ctx, corequery.UpsertRestorePluginOptionsParams{
		RestoreID:     options.JobID,
		PluginID:      options.PluginID,
		PluginVersion: options.PluginVersion,
		SchemaVersion: int64(options.SchemaVersion),
		Options:       options.Options,
		UpdatedAt:     pluginJobOptionsUpdatedAt(options).Unix(),
	})
}

func (db *Store) loadRestorePluginOptions(restoreID string) (*PluginJobOptions, error) {
	row, err := db.readQueries.GetRestorePluginOptions(db.ctx, restoreID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	options := restorePluginOptionsFromRow(row)
	return &options, nil
}

func (db *Store) ListRestorePluginOptions(ctx context.Context, pluginID, pluginVersion string) ([]PluginJobOptions, error) {
	rows, err := db.readQueries.ListRestorePluginOptionsByPluginVersion(db.pluginContext(ctx), corequery.ListRestorePluginOptionsByPluginVersionParams{
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("list restore plugin options: %w", err)
	}
	options := make([]PluginJobOptions, len(rows))
	for index, row := range rows {
		options[index] = restorePluginOptionsFromRow(row)
	}
	return options, nil
}

func (db *Store) ListRestorePluginOptionHistory(ctx context.Context, restoreID string) ([]PluginJobOptionsHistory, error) {
	rows, err := db.readQueries.ListRestorePluginOptionHistory(db.pluginContext(ctx), restoreID)
	if err != nil {
		return nil, fmt.Errorf("list restore plugin option history: %w", err)
	}
	history := make([]PluginJobOptionsHistory, len(rows))
	for index, row := range rows {
		history[index] = PluginJobOptionsHistory{
			ID:            row.ID,
			JobID:         row.RestoreID,
			PluginID:      row.PluginID,
			PluginVersion: row.PluginVersion,
			SchemaVersion: uint32(row.SchemaVersion),
			Options:       row.Options,
			MigratedAt:    time.Unix(row.MigratedAt, 0),
		}
	}
	return history, nil
}

func validatePluginJobOptions(options PluginJobOptions) error {
	if options.JobID == "" || options.PluginID == "" || options.PluginVersion == "" {
		return fmt.Errorf("plugin job option identity is incomplete")
	}
	if options.SchemaVersion == 0 {
		return fmt.Errorf("plugin job option schema version is zero")
	}
	if len(options.Options) == 0 || len(options.Options) > maxPluginJobOptionsBytes {
		return fmt.Errorf("plugin job option size is invalid")
	}
	return nil
}

func pluginJobOptionsUpdatedAt(options PluginJobOptions) time.Time {
	if options.UpdatedAt.IsZero() {
		return time.Now()
	}
	return options.UpdatedAt
}

func backupPluginOptionsFromRow(row corequery.BackupPluginOption) PluginJobOptions {
	return PluginJobOptions{
		JobID:         row.BackupID,
		PluginID:      row.PluginID,
		PluginVersion: row.PluginVersion,
		SchemaVersion: uint32(row.SchemaVersion),
		Options:       row.Options,
		UpdatedAt:     time.Unix(row.UpdatedAt, 0),
	}
}

func restorePluginOptionsFromRow(row corequery.RestorePluginOption) PluginJobOptions {
	return PluginJobOptions{
		JobID:         row.RestoreID,
		PluginID:      row.PluginID,
		PluginVersion: row.PluginVersion,
		SchemaVersion: uint32(row.SchemaVersion),
		Options:       row.Options,
		UpdatedAt:     time.Unix(row.UpdatedAt, 0),
	}
}
