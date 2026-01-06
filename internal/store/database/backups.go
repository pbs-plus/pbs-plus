//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	_ "modernc.org/sqlite"
)

// generateUniqueJobId produces a unique backup id based on the backup’s target.
func (database *Database) generateUniqueJobId(backup types.Backup) (string, error) {
	baseID := utils.Slugify(backup.Target)
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
			QueryRow("SELECT 1 FROM backups WHERE id = ? LIMIT 1", newID).
			Scan(&exists)

		if errors.Is(err, sql.ErrNoRows) {
			return newID, nil
		}
		if err != nil {
			return "", fmt.Errorf(
				"generateUniqueJobId: error checking backup existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique backup ID after %d attempts",
		maxAttempts)
}

// CreateBackup creates a new backup record and adds any associated exclusions.
func (database *Database) CreateBackup(tx *sql.Tx, backup types.Backup) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateBackup: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback() // Rollback on panic
				panic(p)          // Re-panic after rollback
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateBackup: failed to commit transaction: %w", cErr) // Assign commit error back
					syslog.L.Error(err).Write()
				}
			} else {
				// Rollback if commit isn't explicitly needed (e.g., early return without error)
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if backup.ID == "" {
		id, err := database.generateUniqueJobId(backup)
		if err != nil {
			return fmt.Errorf("CreateBackup: failed to generate unique id -> %w", err)
		}
		backup.ID = id
	}

	if backup.Target == "" {
		return errors.New("target is empty")
	}
	if backup.Store == "" {
		return errors.New("datastore is empty")
	}
	if !utils.IsValidID(backup.ID) && backup.ID != "" {
		return fmt.Errorf("CreateBackup: invalid id string -> %s", backup.ID)
	}
	if !utils.IsValidNamespace(backup.Namespace) && backup.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", backup.Namespace)
	}
	if err := utils.ValidateOnCalendar(backup.Schedule); err != nil && backup.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", backup.Schedule)
	}
	if !utils.IsValidPathString(backup.Subpath) {
		return fmt.Errorf("invalid subpath string: %s", backup.Subpath)
	}
	if backup.RetryInterval <= 0 {
		backup.RetryInterval = 1
	}
	if backup.Retry < 0 {
		backup.Retry = 0
	}
	if backup.MaxDirEntries <= 0 {
		backup.MaxDirEntries = 1048576
	}
	if strings.TrimSpace(backup.ReadMode) == "" {
		backup.ReadMode = "standard"
	}
	if strings.TrimSpace(backup.SourceMode) == "" {
		backup.SourceMode = "snapshot"
	}

	_, err = tx.Exec(`
        INSERT INTO backups (
            id, store, mode, source_mode, read_mode, target, subpath, schedule, comment,
            notification_mode, namespace, current_pid, last_run_upid, last_successful_upid, retry,
            retry_interval, max_dir_entries, pre_script, post_script, include_xattr
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, backup.ID, backup.Store, backup.Mode, backup.SourceMode, backup.ReadMode, backup.Target, backup.Subpath,
		backup.Schedule, backup.Comment, backup.NotificationMode, backup.Namespace, backup.CurrentPID,
		backup.LastRunUpid, backup.LastSuccessfulUpid, backup.Retry, backup.RetryInterval,
		backup.MaxDirEntries, backup.PreScript, backup.PostScript, backup.IncludeXattr)
	if err != nil {
		return fmt.Errorf("CreateBackup: error inserting backup: %w", err)
	}

	for _, exclusion := range backup.Exclusions {
		if exclusion.JobId == "" {
			exclusion.JobId = backup.ID
		}
		// Assuming CreateExclusion uses the provided tx
		if err = database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(fmt.Errorf("CreateBackup: failed to create exclusion: %w", err)).
				WithField("backup_id", backup.ID).
				WithField("path", exclusion.Path).
				Write()
			// Return the first error encountered during exclusion creation
			return fmt.Errorf("CreateBackup: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = system.SetSchedule(backup); err != nil {
		syslog.L.Error(fmt.Errorf("CreateBackup: failed to set schedule: %w", err)).
			WithField("id", backup.ID).
			Write()
		// Decide if schedule setting failure should rollback the DB transaction
		// return fmt.Errorf("CreateBackup: failed to set schedule: %w", err) // Uncomment to rollback
	}

	commitNeeded = true
	return nil
}

// GetBackup retrieves a backup by id and assembles its exclusions.
func (database *Database) GetBackup(id string) (types.Backup, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            t.drive_used_bytes, t.mount_script, j.include_xattr
        FROM backups j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
        WHERE j.id = ?
    `
	rows, err := database.readDb.Query(query, id)
	if err != nil {
		return types.Backup{}, fmt.Errorf("GetBackup: error querying backup data: %w", err)
	}
	defer rows.Close()

	var backup types.Backup
	var exclusionPaths []string
	var found bool = false

	for rows.Next() {
		found = true
		var exclusionPath sql.NullString
		var driveUsedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&backup.ID, &backup.Store, &backup.Mode, &backup.SourceMode, &backup.ReadMode,
			&backup.Target, &backup.Subpath, &backup.Schedule, &backup.Comment,
			&backup.NotificationMode, &backup.Namespace, &backup.CurrentPID, &backup.LastRunUpid,
			&backup.LastSuccessfulUpid, &backup.Retry, &backup.RetryInterval, &backup.MaxDirEntries,
			&backup.PreScript, &backup.PostScript, &exclusionPath, &driveUsedBytes,
			&mountScript, &backup.IncludeXattr,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetBackup: error scanning backup data: %w", err)).
				WithField("id", id).
				Write()
			return types.Backup{}, fmt.Errorf("GetBackup: error scanning backup data for id %s: %w", id, err)
		}

		if mountScript.Valid {
			backup.TargetMountScript = mountScript.String
		} else {
			// Optionally, handle the NULL case, e.g., set to an empty string
			backup.TargetMountScript = ""
		}

		if exclusionPath.Valid && exclusionPath.String != "" {
			exclusionPaths = append(exclusionPaths, exclusionPath.String)
		}
		// Only set ExpectedSize once from the first row (or any row) where it's valid
		if driveUsedBytes.Valid && backup.ExpectedSize == 0 {
			backup.ExpectedSize = int(driveUsedBytes.Int64)
		}
	}

	if err = rows.Err(); err != nil {
		return types.Backup{}, fmt.Errorf("GetBackup: error iterating backup results: %w", err)
	}

	if !found {
		return types.Backup{}, ErrBackupNotFound
	}

	backup.Exclusions = make([]types.Exclusion, 0, len(exclusionPaths))
	for _, path := range exclusionPaths {
		backup.Exclusions = append(backup.Exclusions, types.Exclusion{
			JobId: backup.ID,
			Path:  path,
		})
	}
	backup.RawExclusions = strings.Join(exclusionPaths, "\n")

	database.populateBackupExtras(&backup)

	return backup, nil
}

// populateBackupExtras fills in details not directly from the database tables.
func (database *Database) populateBackupExtras(backup *types.Backup) {
	if backup.LastRunUpid != "" {
		task, err := proxmox.GetTaskByUPID(backup.LastRunUpid)
		if err == nil {
			backup.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				backup.LastRunState = task.ExitStatus
				backup.Duration = task.EndTime - task.StartTime
			} else if task.StartTime > 0 {
				backup.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if backup.LastSuccessfulUpid != "" {
		if successTask, err := proxmox.GetTaskByUPID(backup.LastSuccessfulUpid); err == nil {
			backup.LastSuccessfulEndtime = successTask.EndTime
		}
	}

	if nextSchedule, err := system.GetNextSchedule(*backup); err == nil && nextSchedule != nil {
		backup.NextRun = nextSchedule.Unix()
	}
}

// UpdateBackup updates an existing backup and its exclusions.
func (database *Database) UpdateBackup(tx *sql.Tx, backup types.Backup) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateBackup: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateBackup: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if !utils.IsValidID(backup.ID) && backup.ID != "" {
		return fmt.Errorf("UpdateBackup: invalid id string -> %s", backup.ID)
	}
	if backup.Target == "" {
		return errors.New("target is empty")
	}
	if backup.Store == "" {
		return errors.New("datastore is empty")
	}
	if backup.RetryInterval <= 0 {
		backup.RetryInterval = 1
	}
	if backup.Retry < 0 {
		backup.Retry = 0
	}
	if !utils.IsValidNamespace(backup.Namespace) && backup.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", backup.Namespace)
	}
	if err := utils.ValidateOnCalendar(backup.Schedule); err != nil && backup.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", backup.Schedule)
	}
	if !utils.IsValidPathString(backup.Subpath) {
		return fmt.Errorf("invalid subpath string: %s", backup.Subpath)
	}
	if strings.TrimSpace(backup.ReadMode) == "" {
		backup.ReadMode = "standard"
	}
	if strings.TrimSpace(backup.SourceMode) == "" {
		backup.SourceMode = "snapshot"
	}

	_, err = tx.Exec(`
        UPDATE backups SET store = ?, mode = ?, source_mode = ?, read_mode = ?, target = ?,
            subpath = ?, schedule = ?, comment = ?, notification_mode = ?,
            namespace = ?, current_pid = ?, last_run_upid = ?, retry = ?,
            retry_interval = ?, last_successful_upid = ?, pre_script = ?, post_script = ?,
            max_dir_entries = ?, include_xattr = ?
        WHERE id = ?
    `, backup.Store, backup.Mode, backup.SourceMode, backup.ReadMode, backup.Target, backup.Subpath,
		backup.Schedule, backup.Comment, backup.NotificationMode, backup.Namespace,
		backup.CurrentPID, backup.LastRunUpid, backup.Retry, backup.RetryInterval,
		backup.LastSuccessfulUpid, backup.PreScript, backup.PostScript, backup.MaxDirEntries,
		backup.IncludeXattr, backup.ID)
	if err != nil {
		return fmt.Errorf("UpdateBackup: error updating backup: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM exclusions WHERE job_id = ?`, backup.ID)
	if err != nil {
		return fmt.Errorf("UpdateBackup: error removing old exclusions for backup %s: %w", backup.ID, err)
	}

	for _, exclusion := range backup.Exclusions {
		exclusion.JobId = backup.ID // Ensure correct JobId
		if err = database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(fmt.Errorf("UpdateBackup: failed to create exclusion: %w", err)).
				WithField("backup_id", backup.ID).
				WithField("path", exclusion.Path).
				Write()
			return fmt.Errorf("UpdateBackup: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = system.SetSchedule(backup); err != nil {
		syslog.L.Error(fmt.Errorf("UpdateBackup: failed to set schedule: %w", err)).
			WithField("id", backup.ID).
			Write()
		// Decide if schedule setting failure should rollback the DB transaction
		// return fmt.Errorf("UpdateBackup: failed to set schedule: %w", err) // Uncomment to rollback
	}

	if backup.LastRunUpid != "" {
		go database.linkBackupLog(backup.ID, backup.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

// linkBackupLog handles the asynchronous log linking.
func (database *Database) linkBackupLog(backupID, upid string) {
	backupLogsPath := filepath.Join(constants.BackupLogsBasePath, backupID)
	if err := os.MkdirAll(backupLogsPath, 0755); err != nil {
		syslog.L.Error(fmt.Errorf("linkBackupLog: failed to create log dir: %w", err)).
			WithField("id", backupID).
			Write()
		return
	}

	backupLogPath := filepath.Join(backupLogsPath, upid)
	if _, err := os.Lstat(backupLogPath); err != nil && !os.IsNotExist(err) {
		syslog.L.Error(fmt.Errorf("linkBackupLog: failed to stat potential symlink: %w", err)).
			WithField("path", backupLogPath).
			Write()
		return
	}

	origLogPath, err := proxmox.GetLogPath(upid)
	if err != nil {
		syslog.L.Error(fmt.Errorf("linkBackupLog: failed to get original log path: %w", err)).
			WithField("id", backupID).
			WithField("upid", upid).
			Write()
		return
	}

	if _, err := os.Stat(origLogPath); err != nil {
		syslog.L.Error(fmt.Errorf("linkBackupLog: original log path does not exist or error stating: %w", err)).
			WithField("orig_path", origLogPath).
			WithField("id", backupID).
			Write()
		return
	}

	_ = os.Remove(backupLogPath)

	err = os.Symlink(origLogPath, backupLogPath)
	if err != nil {
		syslog.L.Error(fmt.Errorf("linkBackupLog: failed to create symlink: %w", err)).
			WithField("id", backupID).
			WithField("source", origLogPath).
			WithField("link", backupLogPath).
			Write()
	}
}

// GetAllBackups returns all backup records.
func (database *Database) GetAllBackups() ([]types.Backup, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            t.drive_used_bytes, t.mount_script, t.path, j.include_xattr
        FROM backups j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
        ORDER BY j.id
    `
	rows, err := database.readDb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllBackups: error querying backups: %w", err)
	}
	defer rows.Close()

	backupsMap := make(map[string]*types.Backup)
	var backupOrder []string

	for rows.Next() {
		var backupID, store, mode, sourceMode, readMode, target, subpath, schedule, comment, notificationMode, namespace, lastRunUpid, lastSuccessfulUpid string
		var preScript, postScript string
		var retry, maxDirEntries int
		var retryInterval int
		var currentPID int
		var includeXAttr bool
		var targetPath, exclusionPath sql.NullString
		var driveUsedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&backupID, &store, &mode, &sourceMode, &readMode, &target, &subpath, &schedule, &comment,
			&notificationMode, &namespace, &currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval, &maxDirEntries, &preScript, &postScript,
			&exclusionPath, &driveUsedBytes, &mountScript, &targetPath, &includeXAttr,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllBackups: error scanning row: %w", err)).Write()
			continue
		}

		backup, exists := backupsMap[backupID]
		if !exists {
			backup = &types.Backup{
				ID:                 backupID,
				Store:              store,
				Mode:               mode,
				SourceMode:         sourceMode,
				ReadMode:           readMode,
				Target:             target,
				Subpath:            subpath,
				Schedule:           schedule,
				Comment:            comment,
				NotificationMode:   notificationMode,
				Namespace:          namespace,
				CurrentPID:         currentPID,
				LastRunUpid:        lastRunUpid,
				LastSuccessfulUpid: lastSuccessfulUpid,
				Retry:              retry,
				RetryInterval:      retryInterval,
				MaxDirEntries:      maxDirEntries,
				Exclusions:         make([]types.Exclusion, 0),
				PreScript:          preScript,
				PostScript:         postScript,
				IncludeXattr:       includeXAttr,
			}
			if driveUsedBytes.Valid {
				backup.ExpectedSize = int(driveUsedBytes.Int64)
			}
			backupsMap[backupID] = backup
			backupOrder = append(backupOrder, backupID)
			database.populateBackupExtras(backup) // Populate non-SQL extras once per backup
		}

		if targetPath.Valid {
			backup.TargetPath = targetPath.String
		}

		if mountScript.Valid {
			backup.TargetMountScript = mountScript.String
		} else {
			backup.TargetMountScript = ""
		}

		if exclusionPath.Valid && exclusionPath.String != "" {
			backup.Exclusions = append(backup.Exclusions, types.Exclusion{
				JobId: backupID,
				Path:  exclusionPath.String,
			})
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllBackups: error iterating backup results: %w", err)
	}

	backups := make([]types.Backup, len(backupOrder))
	for i, backupID := range backupOrder {
		backup := backupsMap[backupID]
		pathSlice := make([]string, len(backup.Exclusions))
		for k, exclusion := range backup.Exclusions {
			pathSlice[k] = exclusion.Path
		}
		backup.RawExclusions = strings.Join(pathSlice, "\n")
		backups[i] = *backup
	}

	return backups, nil
}

func (database *Database) GetAllQueuedBackups() ([]types.Backup, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            t.drive_used_bytes, t.mount_script, j.include_xattr
        FROM backups j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
				WHERE j.last_run_upid LIKE "%pbsplusgen-queue%"
        ORDER BY j.id
    `
	rows, err := database.readDb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllQueuedBackups: error querying backups: %w", err)
	}
	defer rows.Close()

	backupsMap := make(map[string]*types.Backup)
	var backupOrder []string

	for rows.Next() {
		var backupID, store, mode, sourceMode, readMode, target, subpath, schedule, comment, notificationMode, namespace, lastRunUpid, lastSuccessfulUpid string
		var preScript, postScript string
		var retry, maxDirEntries int
		var retryInterval int
		var currentPID int
		var includeXAttr bool
		var exclusionPath sql.NullString
		var driveUsedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&backupID, &store, &mode, &sourceMode, &readMode, &target, &subpath, &schedule, &comment,
			&notificationMode, &namespace, &currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval, &maxDirEntries, &preScript, &postScript,
			&exclusionPath, &driveUsedBytes, &mountScript, &includeXAttr,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllQueuedBackups: error scanning row: %w", err)).Write()
			continue
		}

		backup, exists := backupsMap[backupID]
		if !exists {
			backup = &types.Backup{
				ID:                 backupID,
				Store:              store,
				Mode:               mode,
				SourceMode:         sourceMode,
				ReadMode:           readMode,
				Target:             target,
				Subpath:            subpath,
				Schedule:           schedule,
				Comment:            comment,
				NotificationMode:   notificationMode,
				Namespace:          namespace,
				CurrentPID:         currentPID,
				LastRunUpid:        lastRunUpid,
				LastSuccessfulUpid: lastSuccessfulUpid,
				Retry:              retry,
				RetryInterval:      retryInterval,
				MaxDirEntries:      maxDirEntries,
				Exclusions:         make([]types.Exclusion, 0),
				PreScript:          preScript,
				PostScript:         postScript,
				IncludeXattr:       includeXAttr,
			}
			if driveUsedBytes.Valid {
				backup.ExpectedSize = int(driveUsedBytes.Int64)
			}
			backupsMap[backupID] = backup
			backupOrder = append(backupOrder, backupID)
			database.populateBackupExtras(backup) // Populate non-SQL extras once per backup
		}

		if mountScript.Valid {
			backup.TargetMountScript = mountScript.String
		} else {
			backup.TargetMountScript = ""
		}

		if exclusionPath.Valid && exclusionPath.String != "" {
			backup.Exclusions = append(backup.Exclusions, types.Exclusion{
				JobId: backupID,
				Path:  exclusionPath.String,
			})
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllQueuedBackups: error iterating backup results: %w", err)
	}

	backups := make([]types.Backup, len(backupOrder))
	for i, backupID := range backupOrder {
		backup := backupsMap[backupID]
		pathSlice := make([]string, len(backup.Exclusions))
		for k, exclusion := range backup.Exclusions {
			pathSlice[k] = exclusion.Path
		}
		backup.RawExclusions = strings.Join(pathSlice, "\n")
		backups[i] = *backup
	}

	return backups, nil
}

// DeleteBackup deletes a backup and any related exclusions.
func (database *Database) DeleteBackup(tx *sql.Tx, id string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("DeleteBackup: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteBackup: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	// Delete associated exclusions first (or rely on ON DELETE CASCADE)
	_, err = tx.Exec("DELETE FROM exclusions WHERE job_id = ?", id)
	if err != nil {
		syslog.L.Error(fmt.Errorf("DeleteBackup: error deleting exclusions: %w", err)).
			WithField("id", id).
			Write()
		// Return error if exclusions deletion fails
		return fmt.Errorf("DeleteBackup: error deleting exclusions for backup %s: %w", id, err)
	}

	// Delete the backup itself
	res, err := tx.Exec("DELETE FROM backups WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("DeleteBackup: error deleting backup %s: %w", id, err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrBackupNotFound
	}

	// Filesystem and system operations outside transaction
	backupLogsPath := filepath.Join(constants.BackupLogsBasePath, id)
	if err := os.RemoveAll(backupLogsPath); err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Error(fmt.Errorf("DeleteBackup: failed removing backup logs: %w", err)).
				WithField("id", id).
				Write()
			// Decide if this failure should prevent commit (if tx was passed in)
			// or just be logged. Currently just logged.
		}
	}

	if err := system.DeleteSchedule(id); err != nil {
		syslog.L.Error(fmt.Errorf("DeleteBackup: failed deleting schedule: %w", err)).
			WithField("id", id).
			Write()
		// Decide if this failure should prevent commit or just be logged.
	}

	commitNeeded = true
	return nil
}
