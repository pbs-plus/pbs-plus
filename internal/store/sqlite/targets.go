//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	_ "modernc.org/sqlite"
)

// CreateTarget inserts a new target.
func (database *Database) CreateTarget(tx *sql.Tx, target types.Target) error {
	if tx == nil {
		database.writeMu.Lock()
		defer database.writeMu.Unlock()

		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
	}

	if target.Path == "" {
		return fmt.Errorf("target path empty")
	}
	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("invalid target path: %s", target.Path)
	}

	_, err := tx.Exec(`
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
		// If the target already exists, update it.
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return database.UpdateTarget(tx, target)
		}
		return fmt.Errorf("CreateTarget: error inserting target: %w", err)
	}
	return nil
}

// UpdateTarget updates an existing target.
func (database *Database) UpdateTarget(tx *sql.Tx, target types.Target) error {
	if tx == nil {
		database.writeMu.Lock()
		defer database.writeMu.Unlock()

		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
	}

	if target.Path == "" {
		return fmt.Errorf("target path empty")
	}
	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("invalid target path: %s", target.Path)
	}

	_, err := tx.Exec(`
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
	return nil
}

// DeleteTarget removes a target.
func (database *Database) DeleteTarget(tx *sql.Tx, name string) error {
	if tx == nil {
		database.writeMu.Lock()
		defer database.writeMu.Unlock()

		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
	}

	_, err := tx.Exec("DELETE FROM targets WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("DeleteTarget: error deleting target: %w", err)
	}
	return nil
}

// GetTarget retrieves a target by name.
func (database *Database) GetTarget(name string) (types.Target, error) {
	row := database.readDb.QueryRow(`
        SELECT name, path, auth, token_used, drive_type, drive_name, drive_fs, drive_total_bytes,
					drive_used_bytes, drive_free_bytes, drive_total, drive_used, drive_free FROM targets
        WHERE name = ?
    `, name)
	var target types.Target
	err := row.Scan(
		&target.Name, &target.Path, &target.Auth, &target.TokenUsed,
		&target.DriveType, &target.DriveName, &target.DriveFS,
		&target.DriveTotalBytes, &target.DriveUsedBytes, &target.DriveFreeBytes,
		&target.DriveTotal, &target.DriveUsed, &target.DriveFree,
	)
	if err != nil {
		return types.Target{}, fmt.Errorf("GetTarget: error fetching target: %w", err)
	}

	// Adjust fields based on target.Path.
	if strings.HasPrefix(target.Path, "agent://") {
		target.IsAgent = true
	} else {
		target.ConnectionStatus = utils.IsValid(target.Path)
		target.IsAgent = false
	}
	return target, nil
}

// GetAllTargets returns all targets.
func (database *Database) GetAllTargets() ([]types.Target, error) {
	rows, err := database.readDb.Query(`
		SELECT name, path, auth, token_used, drive_type, drive_name, drive_fs, drive_total_bytes,
			drive_used_bytes, drive_free_bytes, drive_total, drive_used, drive_free FROM targets
	`)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
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
			continue
		}

		if strings.HasPrefix(target.Path, "agent://") {
			target.IsAgent = true
		} else {
			target.ConnectionStatus = utils.IsValid(target.Path)
			target.IsAgent = false
		}

		targets = append(targets, target)
	}
	return targets, nil
}

// GetAllTargetsByIP returns all agent targets matching the given client IP.
func (database *Database) GetAllTargetsByIP(clientIP string) ([]types.Target, error) {
	rows, err := database.readDb.Query(`
		SELECT name, path, auth, token_used, drive_type, drive_name, drive_fs, drive_total_bytes,
			drive_used_bytes, drive_free_bytes, drive_total, drive_used, drive_free FROM targets
		WHERE path LIKE ?
		`, fmt.Sprintf("agent://%s%%", clientIP))
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
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
			continue
		}

		if strings.HasPrefix(target.Path, "agent://") {
			target.IsAgent = true
		} else {
			target.ConnectionStatus = utils.IsValid(target.Path)
			target.IsAgent = false
		}

		targets = append(targets, target)
	}
	return targets, nil
}
