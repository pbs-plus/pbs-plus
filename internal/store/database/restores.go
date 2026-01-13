//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	_ "modernc.org/sqlite"
)

// generateUniqueRestoreID produces a unique restore id based on the restore’s target.
func (database *Database) generateUniqueRestoreID(restore types.Restore) (string, error) {
	baseID := utils.Slugify(restore.DestTarget.String())
	if baseID == "" {
		return "", fmt.Errorf("invalid target: slugified value is empty")
	}

	for idx := 0; idx < maxAttempts; idx++ {
		var newID string
		if idx == 0 {
			newID = baseID
		} else {
			newID = fmt.Sprintf("%s-%d", baseID, idx)
		}
		var exists int
		err := database.readDb.
			QueryRow("SELECT 1 FROM restores WHERE id = ? LIMIT 1", newID).
			Scan(&exists)

		if errors.Is(err, sql.ErrNoRows) {
			return newID, nil
		}
		if err != nil {
			return "", fmt.Errorf(
				"generateUniqueRestoreID: error checking restore existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique restore ID after %d attempts",
		maxAttempts)
}

// CreateRestore creates a new restore record and adds any associated exclusions.
func (database *Database) CreateRestore(tx *sql.Tx, restore types.Restore) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateRestore: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback() // Rollback on panic
				panic(p)          // Re-panic after rollback
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateRestore: failed to commit transaction: %w", cErr) // Assign commit error back
					syslog.L.Error(err).Write()
				}
			} else {
				// Rollback if commit isn't explicitly needed (e.g., early return without error)
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if restore.ID == "" {
		id, err := database.generateUniqueRestoreID(restore)
		if err != nil {
			return fmt.Errorf("CreateRestore: failed to generate unique id -> %w", err)
		}
		restore.ID = id
	}

	if restore.DestTarget.String() == "" {
		return errors.New("dest target is empty")
	}
	if restore.Snapshot == "" {
		return errors.New("snapshot is empty")
	}
	if restore.Store == "" {
		return errors.New("datastore is empty")
	}
	if !utils.IsValidID(restore.ID) && restore.ID != "" {
		return fmt.Errorf("CreateRestore: invalid id string -> %s", restore.ID)
	}
	if !utils.IsValidPathString(restore.SrcPath) {
		return fmt.Errorf("invalid source path string: %s", restore.SrcPath)
	}
	if !utils.IsValidPathString(restore.DestPath) {
		return fmt.Errorf("invalid dest path string: %s", restore.DestPath)
	}
	if restore.RetryInterval <= 0 {
		restore.RetryInterval = 1
	}
	if restore.Retry < 0 {
		restore.Retry = 0
	}

	_, err = tx.Exec(`
        INSERT INTO restores (
            id, store, namespace, snapshot, src_path, dest_target, dest_path, comment,
            current_pid, last_run_upid, last_successful_upid, retry,
            retry_interval
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, restore.ID, restore.Store, restore.Namespace, restore.Snapshot, restore.SrcPath, restore.DestTarget, restore.DestPath,
		restore.Comment, restore.CurrentPID, restore.LastRunUpid, restore.LastSuccessfulUpid, restore.Retry,
		restore.RetryInterval)
	if err != nil {
		return fmt.Errorf("CreateRestore: error inserting restore: %w", err)
	}

	commitNeeded = true
	return nil
}

// GetRestore retrieves a restore by id and assembles its exclusions.
func (database *Database) GetRestore(id string) (types.Restore, error) {
	query := `
        SELECT
            j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target, j.dest_path, j.comment,
            j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval
        FROM restores j
        WHERE j.id = ?
    `
	rows, err := database.readDb.Query(query, id)
	if err != nil {
		return types.Restore{}, fmt.Errorf("GetRestore: error querying restore data: %w", err)
	}
	defer rows.Close()

	var restore types.Restore
	var found bool = false

	for rows.Next() {
		found = true
		err := rows.Scan(
			&restore.ID, &restore.Store, &restore.Namespace, &restore.Snapshot, &restore.SrcPath, &restore.DestTarget,
			&restore.DestPath, &restore.Comment, &restore.CurrentPID, &restore.LastRunUpid,
			&restore.LastSuccessfulUpid, &restore.Retry, &restore.RetryInterval)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetRestore: error scanning restore data: %w", err)).
				WithField("id", id).
				Write()
			return types.Restore{}, fmt.Errorf("GetRestore: error scanning restore data for id %s: %w", id, err)
		}
	}

	if err = rows.Err(); err != nil {
		return types.Restore{}, fmt.Errorf("GetRestore: error iterating restore results: %w", err)
	}

	if !found {
		return types.Restore{}, ErrRestoreNotFound
	}

	database.populateRestoreExtras(&restore)

	return restore, nil
}

// populateRestoreExtras fills in details not directly from the database tables.
func (database *Database) populateRestoreExtras(restore *types.Restore) {
	if restore.LastRunUpid != "" {
		task, err := proxmox.GetTaskByUPID(restore.LastRunUpid)
		if err == nil {
			restore.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				restore.LastRunState = task.ExitStatus
				restore.Duration = task.EndTime - task.StartTime
			} else if task.StartTime > 0 {
				restore.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if restore.LastSuccessfulUpid != "" {
		if successTask, err := proxmox.GetTaskByUPID(restore.LastSuccessfulUpid); err == nil {
			restore.LastSuccessfulEndtime = successTask.EndTime
		}
	}
}

// UpdateRestore updates an existing restore and its exclusions.
func (database *Database) UpdateRestore(tx *sql.Tx, restore types.Restore) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateRestore: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateRestore: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if !utils.IsValidID(restore.ID) && restore.ID != "" {
		return fmt.Errorf("UpdateRestore: invalid id string -> %s", restore.ID)
	}
	if restore.DestTarget.String() == "" {
		return errors.New("dest target is empty")
	}
	if restore.Snapshot == "" {
		return errors.New("snapshot is empty")
	}
	if restore.Store == "" {
		return errors.New("datastore is empty")
	}
	if !utils.IsValidID(restore.ID) && restore.ID != "" {
		return fmt.Errorf("CreateRestore: invalid id string -> %s", restore.ID)
	}
	if !utils.IsValidPathString(restore.SrcPath) {
		return fmt.Errorf("invalid source path string: %s", restore.SrcPath)
	}
	if !utils.IsValidPathString(restore.DestPath) {
		return fmt.Errorf("invalid dest path string: %s", restore.DestPath)
	}
	if restore.RetryInterval <= 0 {
		restore.RetryInterval = 1
	}
	if restore.Retry < 0 {
		restore.Retry = 0
	}

	_, err = tx.Exec(`
        UPDATE restores SET store = ?, namespace = ?, snapshot = ?, src_path = ?, dest_target = ?, dest_path = ?,
            comment = ?, current_pid = ?, last_run_upid = ?, retry = ?,
            retry_interval = ?, last_successful_upid = ?
        WHERE id = ?
    `, restore.Store, restore.Namespace, restore.Snapshot, restore.SrcPath, restore.DestTarget, restore.DestPath,
		restore.Comment, restore.CurrentPID, restore.LastRunUpid, restore.Retry, restore.RetryInterval,
		restore.LastSuccessfulUpid, restore.ID)
	if err != nil {
		return fmt.Errorf("UpdateRestore: error updating restore: %w", err)
	}

	if restore.LastRunUpid != "" {
		go database.linkRestoreLog(restore.ID, restore.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

// linkRestoreLog handles the asynchronous log linking.
func (database *Database) linkRestoreLog(restoreID, upid string) {
	restoreLogsPath := filepath.Join(constants.RestoreLogsBasePath, restoreID)
	if err := os.MkdirAll(restoreLogsPath, 0755); err != nil {
		syslog.L.Error(fmt.Errorf("linkRestoreLog: failed to create log dir: %w", err)).
			WithField("id", restoreID).
			Write()
		return
	}

	restoreLogPath := filepath.Join(restoreLogsPath, upid)
	if _, err := os.Lstat(restoreLogPath); err != nil && !os.IsNotExist(err) {
		syslog.L.Error(fmt.Errorf("linkRestoreLog: failed to stat potential symlink: %w", err)).
			WithField("path", restoreLogPath).
			Write()
		return
	}

	origLogPath, err := proxmox.GetLogPath(upid)
	if err != nil {
		syslog.L.Error(fmt.Errorf("linkRestoreLog: failed to get original log path: %w", err)).
			WithField("id", restoreID).
			WithField("upid", upid).
			Write()
		return
	}

	if _, err := os.Stat(origLogPath); err != nil {
		syslog.L.Error(fmt.Errorf("linkRestoreLog: original log path does not exist or error stating: %w", err)).
			WithField("orig_path", origLogPath).
			WithField("id", restoreID).
			Write()
		return
	}

	_ = os.Remove(restoreLogPath)

	err = os.Symlink(origLogPath, restoreLogPath)
	if err != nil {
		syslog.L.Error(fmt.Errorf("linkRestoreLog: failed to create symlink: %w", err)).
			WithField("id", restoreID).
			WithField("source", origLogPath).
			WithField("link", restoreLogPath).
			Write()
	}
}

// GetAllRestores returns all restore records.
func (database *Database) GetAllRestores() ([]types.Restore, error) {
	query := `
        SELECT
            j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target, t.path, j.dest_path, j.comment,
            j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval
        FROM restores j
        LEFT JOIN targets t ON j.dest_target = t.name
        ORDER BY j.id
    `
	rows, err := database.readDb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllRestores: error querying restores: %w", err)
	}
	defer rows.Close()

	restoresMap := make(map[string]*types.Restore)
	var restoreOrder []string

	for rows.Next() {
		var restoreID, store, snapshot, srcPath, destTarget, destPath, comment, lastRunUpid, lastSuccessfulUpid string
		var retry int
		var retryInterval int
		var targetPath, namespace sql.NullString
		var currentPID int

		err := rows.Scan(
			&restoreID, &store, &namespace, &snapshot, &srcPath, &destTarget, &targetPath, &destPath, &comment,
			&currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllRestores: error scanning row: %w", err)).Write()
			continue
		}

		restore, exists := restoresMap[restoreID]
		if !exists {
			restore = &types.Restore{
				ID:                 restoreID,
				Store:              store,
				Snapshot:           snapshot,
				SrcPath:            srcPath,
				DestTarget:         types.WrapTargetName(destTarget),
				DestPath:           destPath,
				Comment:            comment,
				CurrentPID:         currentPID,
				LastRunUpid:        lastRunUpid,
				LastSuccessfulUpid: lastSuccessfulUpid,
				Retry:              retry,
				RetryInterval:      retryInterval,
			}
			restoresMap[restoreID] = restore
			restoreOrder = append(restoreOrder, restoreID)
			database.populateRestoreExtras(restore) // Populate non-SQL extras once per restore
		}

		if targetPath.Valid {
			restore.DestTargetPath = types.WrapTargetPath(targetPath.String)
		}
		if namespace.Valid {
			restore.Namespace = namespace.String
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllRestores: error iterating restore results: %w", err)
	}

	restores := make([]types.Restore, len(restoreOrder))
	for i, restoreID := range restoreOrder {
		restore := restoresMap[restoreID]
		restores[i] = *restore
	}

	return restores, nil
}

func (database *Database) GetAllQueuedRestores() ([]types.Restore, error) {
	query := `
        SELECT
            j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target, t.path, j.dest_path, j.comment,
            j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval
        FROM restores j
				WHERE j.last_run_upid LIKE "%pbsplusgen-queue%"
        ORDER BY j.id
    `
	rows, err := database.readDb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllQueuedRestores: error querying restores: %w", err)
	}
	defer rows.Close()

	restoresMap := make(map[string]*types.Restore)
	var restoreOrder []string

	for rows.Next() {
		var restoreID, store, snapshot, srcPath, destTarget, destPath, comment, lastRunUpid, lastSuccessfulUpid string
		var retry int
		var retryInterval int
		var currentPID int
		var targetPath, namespace sql.NullString

		err := rows.Scan(
			&restoreID, &store, &namespace, &snapshot, &srcPath, &destTarget, &targetPath, &destPath, &comment,
			&currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllRestores: error scanning row: %w", err)).Write()
			continue
		}

		restore, exists := restoresMap[restoreID]
		if !exists {
			restore = &types.Restore{
				ID:                 restoreID,
				Store:              store,
				Snapshot:           snapshot,
				SrcPath:            srcPath,
				DestTarget:         types.WrapTargetName(destTarget),
				DestPath:           destPath,
				Comment:            comment,
				CurrentPID:         currentPID,
				LastRunUpid:        lastRunUpid,
				LastSuccessfulUpid: lastSuccessfulUpid,
				Retry:              retry,
				RetryInterval:      retryInterval,
			}
			restoresMap[restoreID] = restore
			restoreOrder = append(restoreOrder, restoreID)
			database.populateRestoreExtras(restore) // Populate non-SQL extras once per restore
		}

		if targetPath.Valid {
			restore.DestTargetPath = types.WrapTargetPath(targetPath.String)
		}
		if namespace.Valid {
			restore.Namespace = namespace.String
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllQueuedRestores: error iterating restore results: %w", err)
	}

	restores := make([]types.Restore, len(restoreOrder))
	for i, restoreID := range restoreOrder {
		restore := restoresMap[restoreID]
		restores[i] = *restore
	}

	return restores, nil
}

// DeleteRestore deletes a restore and any related exclusions.
func (database *Database) DeleteRestore(tx *sql.Tx, id string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("DeleteRestore: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteRestore: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	res, err := tx.Exec("DELETE FROM restores WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("DeleteRestore: error deleting restore %s: %w", id, err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrRestoreNotFound
	}

	restoreLogsPath := filepath.Join(constants.RestoreLogsBasePath, id)
	if err := os.RemoveAll(restoreLogsPath); err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Error(fmt.Errorf("DeleteRestore: failed removing restore logs: %w", err)).
				WithField("id", id).
				Write()
		}
	}

	commitNeeded = true
	return nil
}
