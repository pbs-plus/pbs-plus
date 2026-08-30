//go:build linux

package coredb

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

func (db *Store) BackupGroupMigrationCompleted(backupID string) (bool, error) {
	completed, err := db.readQueries.BackupGroupMigrationCompleted(db.ctx, backupID)
	if err != nil {
		return false, fmt.Errorf("check backup group migration %q: %w", backupID, err)
	}
	return completed != 0, nil
}

func (db *Store) CompleteBackupGroupMigration(backupID string) error {
	if err := db.RunInTransaction(db.ctx, func(_ *Transaction, q *corequery.Queries) error {
		return q.CompleteBackupGroupMigration(db.ctx, backupID)
	}); err != nil {
		return fmt.Errorf("complete backup group migration %q: %w", backupID, err)
	}
	return nil
}
