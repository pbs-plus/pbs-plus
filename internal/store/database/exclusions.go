//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/pattern"
	_ "modernc.org/sqlite"
)

// CreateExclusion inserts a new exclusion into the database.
func (database *Database) CreateExclusion(tx *sql.Tx, exclusion types.Exclusion) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateExclusion: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if exclusion.Path == "" {
		return errors.New("path is empty")
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	_, err = tx.Exec(`
        INSERT INTO exclusions (job_id, path, comment)
        VALUES (?, ?, ?)
    `, exclusion.JobId, exclusion.Path, exclusion.Comment)
	if err != nil {
		// Consider checking for specific constraint errors if needed
		return fmt.Errorf("CreateExclusion: error inserting exclusion: %w", err)
	}

	commitNeeded = true
	return nil
}

// GetAllBackupExclusions returns all exclusions associated with a backup.
// Assumes an index exists on exclusions.job_id.
func (database *Database) GetAllBackupExclusions(backupId string) ([]types.Exclusion, error) {
	rows, err := database.readDb.Query(`
        SELECT job_id, path, comment FROM exclusions
        WHERE job_id = ?
    `, backupId)
	if err != nil {
		return nil, fmt.Errorf("GetAllBackupExclusions: error querying exclusions: %w", err)
	}
	defer rows.Close()

	var exclusions []types.Exclusion
	// Using a map for deduplication based on path as per original logic.
	// If (job_id, path) is unique in DB, this map is unnecessary.
	seenPaths := make(map[string]bool)

	for rows.Next() {
		var excl types.Exclusion
		if err := rows.Scan(&excl.JobId, &excl.Path, &excl.Comment); err != nil {
			syslog.L.Error(fmt.Errorf("GetAllBackupExclusions: error scanning row: %w", err)).Write()
			continue
		}
		// Deduplicate based on path within this backup's exclusions
		if seenPaths[excl.Path] {
			continue
		}
		seenPaths[excl.Path] = true
		exclusions = append(exclusions, excl)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllBackupExclusions: error iterating exclusion rows: %w", err)
	}

	return exclusions, nil
}

// GetAllGlobalExclusions returns all exclusions that are not tied to any backup.
// Assumes an index exists on exclusions.job_id or handles NULL checks efficiently.
func (database *Database) GetAllGlobalExclusions() ([]types.Exclusion, error) {
	rows, err := database.readDb.Query(`
        SELECT job_id, path, comment FROM exclusions
        WHERE job_id IS NULL OR job_id = ''
    `)
	if err != nil {
		return nil, fmt.Errorf("GetAllGlobalExclusions: error querying exclusions: %w", err)
	}
	defer rows.Close()

	var exclusions []types.Exclusion
	// Using a map for deduplication based on path as per original logic.
	// If path is unique for global exclusions, this map is unnecessary.
	seenPaths := make(map[string]bool)

	for rows.Next() {
		var excl types.Exclusion
		// Scan into NullString for job_id if it can truly be NULL
		var backupID sql.NullString
		if err := rows.Scan(&backupID, &excl.Path, &excl.Comment); err != nil {
			syslog.L.Error(fmt.Errorf("GetAllGlobalExclusions: error scanning row: %w", err)).Write()
			continue
		}
		excl.JobId = backupID.String // Assign empty string if NULL

		// Deduplicate based on path within global exclusions
		if seenPaths[excl.Path] {
			continue
		}
		seenPaths[excl.Path] = true
		exclusions = append(exclusions, excl)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllGlobalExclusions: error iterating exclusion rows: %w", err)
	}

	return exclusions, nil
}

// GetExclusion retrieves a single exclusion by its path.
// Assumes an index exists on exclusions.path.
func (database *Database) GetExclusion(path string) (*types.Exclusion, error) {
	row := database.readDb.QueryRow(`
        SELECT job_id, path, comment FROM exclusions WHERE path = ? LIMIT 1
    `, path) // Added LIMIT 1 as path is expected to be unique for this operation

	var excl types.Exclusion
	var backupID sql.NullString // Handle potentially NULL job_id
	err := row.Scan(&backupID, &excl.Path, &excl.Comment)
	if err != nil {
		// Return sql.ErrNoRows directly if not found
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("GetExclusion: error fetching exclusion for path %s: %w", path, err)
	}
	excl.JobId = backupID.String // Assign empty string if NULL
	return &excl, nil
}

// UpdateExclusion updates an existing exclusion identified by its path.
// Assumes an index exists on exclusions.path.
func (database *Database) UpdateExclusion(tx *sql.Tx, exclusion types.Exclusion) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateExclusion: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if exclusion.Path == "" {
		return errors.New("path is empty")
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	// Handle NULL job_id correctly
	var backupIDArg any
	if exclusion.JobId == "" {
		backupIDArg = nil
	} else {
		backupIDArg = exclusion.JobId
	}

	res, err := tx.Exec(`
        UPDATE exclusions SET job_id = ?, comment = ? WHERE path = ?
    `, backupIDArg, exclusion.Comment, exclusion.Path)
	if err != nil {
		return fmt.Errorf("UpdateExclusion: error updating exclusion: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		// Log error getting rows affected but proceed to check count
		syslog.L.Error(fmt.Errorf("UpdateExclusion: error getting rows affected: %w", err)).Write()
	}
	if affected == 0 {
		// Return sql.ErrNoRows if the exclusion wasn't found to update
		return sql.ErrNoRows
	}

	commitNeeded = true
	return nil
}

// DeleteExclusion removes an exclusion from the database by its path.
// Assumes an index exists on exclusions.path.
func (database *Database) DeleteExclusion(tx *sql.Tx, path string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("DeleteExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteExclusion: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	path = strings.ReplaceAll(path, "\\", "/")
	res, err := tx.Exec(`
        DELETE FROM exclusions WHERE path = ?
    `, path)
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error deleting exclusion: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		syslog.L.Error(fmt.Errorf("DeleteExclusion: error getting rows affected: %w", err)).Write()
	}
	if affected == 0 {
		// Return sql.ErrNoRows if the exclusion wasn't found to delete
		return sql.ErrNoRows
	}

	commitNeeded = true
	return nil
}
