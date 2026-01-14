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
	_ "modernc.org/sqlite"
)

// CreateScript inserts a new script or updates if it exists.
func (database *Database) CreateScript(tx *sql.Tx, script types.Script) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateScript: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if script.Path == "" {
		return fmt.Errorf("script path empty")
	}

	_, err = tx.Exec(`
        INSERT INTO scripts (path, description)
        VALUES (?, ?)
    `,
		script.Path, script.Description,
	)
	if err != nil {
		// Use specific error check if possible, otherwise string contains is fallback
		// For modernc.org/sqlite, check for sqlite3.ErrConstraintUnique or similar
		if strings.Contains(err.Error(), "UNIQUE constraint failed") { // Consider more robust error checking
			err = database.UpdateScript(tx, script) // Use the existing transaction
			if err == nil {
				commitNeeded = true // Mark for commit if update succeeds
			}
			return err // Return the result of UpdateScript
		}
		return fmt.Errorf("CreateScript: error inserting script: %w", err)
	}

	commitNeeded = true
	return nil
}

// UpdateScript updates an existing script.
func (database *Database) UpdateScript(tx *sql.Tx, script types.Script) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateScript: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if script.Path == "" {
		return fmt.Errorf("script path empty")
	}

	_, err = tx.Exec(`
        UPDATE scripts SET
					description = ?
        WHERE path = ?
    `,
		script.Description, script.Path,
	)
	if err != nil {
		return fmt.Errorf("UpdateScript: error updating script: %w", err)
	}

	commitNeeded = true
	return nil
}

// DeleteScript removes a script.
func (database *Database) DeleteScript(tx *sql.Tx, name string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("DeleteScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteScript: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	res, err := tx.Exec("DELETE FROM scripts WHERE path = ?", name)
	if err != nil {
		return fmt.Errorf("DeleteScript: error deleting script: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		// Return sql.ErrNoRows if the script wasn't found
		return sql.ErrNoRows
	}

	commitNeeded = true
	return nil
}

// GetScript retrieves a script by name along with its associated job count.
func (database *Database) GetScript(path string) (types.Script, error) {
	row := database.readDb.QueryRow(`
        SELECT
            s.path, s.description,
            COUNT(j.id) as job_count,
            COUNT(t.name) as target_count
        FROM scripts s
        LEFT JOIN backups j ON
					s.path = j.pre_script
					OR s.path = j.post_script
        LEFT JOIN targets t ON
					s.path = t.mount_script
        WHERE s.path = ?
        GROUP BY s.path
    `, path) // Grouping by primary key (or unique key) is sufficient

	var script types.Script
	var jobCount int
	var targetCount int
	err := row.Scan(
		&script.Path, &script.Description,
		&jobCount, &targetCount,
	)
	if err != nil {
		// Don't wrap sql.ErrNoRows, return it directly
		if errors.Is(err, sql.ErrNoRows) {
			return types.Script{}, sql.ErrNoRows
		}
		return types.Script{}, fmt.Errorf("GetScript: error fetching script: %w", err)
	}

	script.JobCount = jobCount
	script.TargetCount = targetCount

	return script, nil
}

// GetAllScripts returns all scripts along with their associated job counts.
func (database *Database) GetAllScripts() ([]types.Script, error) {
	rows, err := database.readDb.Query(`
        SELECT
            s.path, s.description,
            COUNT(j.id) as job_count,
            COUNT(t.name) as target_count
        FROM scripts s
        LEFT JOIN backups j ON
					s.path = j.pre_script
					OR s.path = j.post_script
        LEFT JOIN targets t ON
					s.path = t.mount_script
        GROUP BY s.path, s.description
				ORDER BY s.path
    `) // Group by all non-aggregated columns
	if err != nil {
		return nil, fmt.Errorf("GetAllScripts: error querying scripts: %w", err)
	}
	defer rows.Close()

	var scripts []types.Script
	for rows.Next() {
		var script types.Script
		var jobCount int
		var targetCount int
		err := rows.Scan(
			&script.Path, &script.Description,
			&jobCount, &targetCount,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllScripts: error scanning script row: %w", err)).Write()
			continue
		}

		script.JobCount = jobCount
		script.TargetCount = targetCount

		scripts = append(scripts, script)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllScripts: error iterating script rows: %w", err)
	}

	return scripts, nil
}
