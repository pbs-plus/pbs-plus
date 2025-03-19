//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	_ "modernc.org/sqlite"
)

// CreateTarget inserts a new target.
func (database *Database) CreateTarget(tx *sql.Tx, target types.Target) error {
	if tx == nil {
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
        INSERT INTO targets (name, path, drive_used_bytes, is_agent, connection_status)
        VALUES (?, ?, ?, ?, ?)
    `, target.Name, target.Path, target.DriveUsedBytes, target.IsAgent, target.ConnectionStatus)
	if err != nil {
		// If the target already exists, update it.
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return database.UpdateTarget(tx, target)
		}
		return fmt.Errorf("CreateTarget: error inserting target: %w", err)
	}
	return nil
}

// GetTarget retrieves a target by name.
func (database *Database) GetTarget(name string) (types.Target, error) {
	row := database.readDb.QueryRow(`
        SELECT name, path, drive_used_bytes, is_agent, connection_status FROM targets
        WHERE name = ?
    `, name)
	var target types.Target
	err := row.Scan(&target.Name, &target.Path, &target.DriveUsedBytes, &target.IsAgent,
		&target.ConnectionStatus)
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

// UpdateTarget updates an existing target.
func (database *Database) UpdateTarget(tx *sql.Tx, target types.Target) error {
	if tx == nil {
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
        UPDATE targets SET path = ?, drive_used_bytes = ?, is_agent = ?, connection_status = ?
        WHERE name = ?
    `, target.Path, target.DriveUsedBytes, target.IsAgent, target.ConnectionStatus, target.Name)
	if err != nil {
		return fmt.Errorf("UpdateTarget: error updating target: %w", err)
	}
	return nil
}

// DeleteTarget removes a target.
func (database *Database) DeleteTarget(tx *sql.Tx, name string) error {
	if tx == nil {
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

// GetAllTargets returns all targets.
func (database *Database) GetAllTargets() ([]types.Target, error) {
	rows, err := database.readDb.Query("SELECT name FROM targets")
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
	}
	defer rows.Close()

	var targets []types.Target
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		target, err := database.GetTarget(name)
		if err != nil {
			syslog.L.Error(err).WithField("id", name).Write()
			continue
		}
		targets = append(targets, target)
	}
	return targets, nil
}

// GetAllTargetsByIP returns all agent targets matching the given client IP.
func (database *Database) GetAllTargetsByIP(clientIP string) ([]types.Target, error) {
	rows, err := database.readDb.Query("SELECT name FROM targets")
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error querying targets: %w", err)
	}
	defer rows.Close()

	var targets []types.Target
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		target, err := database.GetTarget(name)
		if err != nil {
			syslog.L.Error(err).WithField("id", name).Write()
			continue
		}
		if target.IsAgent {
			parts := strings.Split(target.Path, "/")
			if len(parts) >= 3 && parts[2] == clientIP {
				targets = append(targets, target)
			}
		}
	}
	return targets, nil
}
