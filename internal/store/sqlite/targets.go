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
	"github.com/pbs-plus/pbs-plus/internal/utils"
	_ "modernc.org/sqlite"
)

// CreateTarget inserts a new target or updates if it exists.
func (database *Database) CreateTarget(tx *sql.Tx, target types.Target) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateTarget: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if target.Path == "" {
		return fmt.Errorf("target path empty")
	}
	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("invalid target path: %s", target.Path)
	}

	_, err = tx.Exec(`
        INSERT INTO targets (name, path, auth, token_used, drive_type, drive_name, drive_fs, drive_total_bytes,
					drive_used_bytes, drive_free_bytes, drive_total, drive_used, drive_free)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
		target.Name, target.Path, target.Auth, target.TokenUsed,
		target.DriveType, target.DriveName, target.DriveFS,
		target.DriveTotalBytes, target.DriveUsedBytes, target.DriveFreeBytes,
		target.DriveTotal, target.DriveUsed, target.DriveFree,
	)
	if err != nil {
		// Use specific error check if possible, otherwise string contains is fallback
		// For modernc.org/sqlite, check for sqlite3.ErrConstraintUnique or similar
		if strings.Contains(err.Error(), "UNIQUE constraint failed") { // Consider more robust error checking
			err = database.UpdateTarget(tx, target) // Use the existing transaction
			if err == nil {
				commitNeeded = true // Mark for commit if update succeeds
			}
			return err // Return the result of UpdateTarget
		}
		return fmt.Errorf("CreateTarget: error inserting target: %w", err)
	}

	commitNeeded = true
	return nil
}

// UpdateTarget updates an existing target.
func (database *Database) UpdateTarget(tx *sql.Tx, target types.Target) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateTarget: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if target.Path == "" {
		return fmt.Errorf("target path empty")
	}
	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("invalid target path: %s", target.Path)
	}

	_, err = tx.Exec(`
        UPDATE targets SET
					path = ?, auth = ?, token_used = ?, drive_type = ?,
					drive_name = ?, drive_fs = ?, drive_total_bytes = ?,
					drive_used_bytes = ?, drive_free_bytes = ?, drive_total = ?,
					drive_used = ?, drive_free = ?
        WHERE name = ?
    `,
		target.Path, target.Auth, target.TokenUsed,
		target.DriveType, target.DriveName, target.DriveFS,
		target.DriveTotalBytes, target.DriveUsedBytes, target.DriveFreeBytes,
		target.DriveTotal, target.DriveUsed, target.DriveFree, target.Name,
	)
	if err != nil {
		return fmt.Errorf("UpdateTarget: error updating target: %w", err)
	}

	commitNeeded = true
	return nil
}

// DeleteTarget removes a target.
func (database *Database) DeleteTarget(tx *sql.Tx, name string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("DeleteTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteTarget: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	res, err := tx.Exec("DELETE FROM targets WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("DeleteTarget: error deleting target: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		// Return sql.ErrNoRows if the target wasn't found
		return sql.ErrNoRows
	}

	commitNeeded = true
	return nil
}

// GetTarget retrieves a target by name along with its associated job count.
func (database *Database) GetTarget(name string) (types.Target, error) {
	row := database.readDb.QueryRow(`
        SELECT
            t.name, t.path, t.auth, t.token_used, t.drive_type, t.drive_name,
            t.drive_fs, t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
            t.drive_total, t.drive_used, t.drive_free,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN jobs j ON t.name = j.target
        WHERE t.name = ?
        GROUP BY t.name
    `, name) // Grouping by primary key (or unique key) is sufficient

	var target types.Target
	var jobCount int
	err := row.Scan(
		&target.Name, &target.Path, &target.Auth, &target.TokenUsed,
		&target.DriveType, &target.DriveName, &target.DriveFS,
		&target.DriveTotalBytes, &target.DriveUsedBytes, &target.DriveFreeBytes,
		&target.DriveTotal, &target.DriveUsed, &target.DriveFree,
		&jobCount,
	)
	if err != nil {
		// Don't wrap sql.ErrNoRows, return it directly
		if errors.Is(err, sql.ErrNoRows) {
			return types.Target{}, sql.ErrNoRows
		}
		return types.Target{}, fmt.Errorf("GetTarget: error fetching target: %w", err)
	}

	target.JobCount = jobCount
	target.IsAgent = strings.HasPrefix(target.Path, "agent://")

	return target, nil
}

// GetAllTargets returns all targets along with their associated job counts.
func (database *Database) GetAllTargets() ([]types.Target, error) {
	rows, err := database.readDb.Query(`
        SELECT
            t.name, t.path, t.auth, t.token_used, t.drive_type, t.drive_name,
            t.drive_fs, t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
            t.drive_total, t.drive_used, t.drive_free,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN jobs j ON t.name = j.target
        GROUP BY t.name, t.path, t.auth, t.token_used, t.drive_type, t.drive_name,
                 t.drive_fs, t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
                 t.drive_total, t.drive_used, t.drive_free
        ORDER BY t.name
    `) // Group by all non-aggregated columns
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
	}
	defer rows.Close()

	var targets []types.Target
	for rows.Next() {
		var target types.Target
		var jobCount int
		err := rows.Scan(
			&target.Name, &target.Path, &target.Auth, &target.TokenUsed,
			&target.DriveType, &target.DriveName, &target.DriveFS,
			&target.DriveTotalBytes, &target.DriveUsedBytes, &target.DriveFreeBytes,
			&target.DriveTotal, &target.DriveUsed, &target.DriveFree,
			&jobCount,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllTargets: error scanning target row: %w", err)).Write()
			continue
		}

		target.JobCount = jobCount
		target.IsAgent = strings.HasPrefix(target.Path, "agent://")

		targets = append(targets, target)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllTargets: error iterating target rows: %w", err)
	}

	return targets, nil
}

// GetAllTargetsByIP returns all agent targets matching the given client IP.
func (database *Database) GetAllTargetsByIP(clientIP string) ([]types.Target, error) {
	// This query doesn't need job count, so no JOIN needed here.
	rows, err := database.readDb.Query(`
		SELECT name, path, auth, token_used, drive_type, drive_name, drive_fs, drive_total_bytes,
			drive_used_bytes, drive_free_bytes, drive_total, drive_used, drive_free FROM targets
		WHERE path LIKE ?
		ORDER BY name
		`, fmt.Sprintf("agent://%s%%", clientIP)) // Ensure clientIP is properly escaped if needed
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error querying targets: %w", err)
	}
	defer rows.Close()

	var targets []types.Target
	for rows.Next() {
		var target types.Target
		err := rows.Scan(
			&target.Name, &target.Path, &target.Auth, &target.TokenUsed,
			&target.DriveType, &target.DriveName, &target.DriveFS,
			&target.DriveTotalBytes, &target.DriveUsedBytes, &target.DriveFreeBytes,
			&target.DriveTotal, &target.DriveUsed, &target.DriveFree,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllTargetsByIP: error scanning target row: %w", err)).Write()
			continue
		}

		// JobCount is not fetched here, default is 0
		target.IsAgent = true // We are querying specifically for agent paths

		targets = append(targets, target)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error iterating target rows: %w", err)
	}

	return targets, nil
}
