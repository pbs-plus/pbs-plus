//go:build linux

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func (database *Database) generateUniqueRestoreID(restore Restore) (string, error) {
	baseID := utils.Slugify(restore.DestTarget.Name)
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

		_, err := database.readQueries.RestoreExists(database.ctx, newID)
		if errors.Is(err, sql.ErrNoRows) {
			return newID, nil
		}
		if err != nil {
			return "", fmt.Errorf("generateUniqueRestoreID: error checking restore existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique restore ID after %d attempts", maxAttempts)
}

func (database *Database) CreateRestore(tx *Transaction, restore Restore) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateRestore: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateRestore: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateRestore: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
		q = database.queries.WithTx(tx.Tx)
	}

	if restore.ID == "" {
		id, err := database.generateUniqueRestoreID(restore)
		if err != nil {
			return fmt.Errorf("CreateRestore: failed to generate unique id -> %w", err)
		}
		restore.ID = id
	}

	// Validation
	if restore.DestTarget.Name == "" {
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
	if !utils.IsValidPathString(restore.DestSubpath) {
		return fmt.Errorf("invalid dest path string: %s", restore.DestSubpath)
	}
	if restore.RetryInterval <= 0 {
		restore.RetryInterval = 1
	}
	if restore.Retry < 0 {
		restore.Retry = 0
	}

	err = q.CreateRestore(database.ctx, sqlc.CreateRestoreParams{
		ID:                 restore.ID,
		Store:              restore.Store,
		Namespace:          sql.NullString{String: restore.Namespace, Valid: restore.Namespace != ""},
		Snapshot:           restore.Snapshot,
		SrcPath:            restore.SrcPath,
		DestTarget:         restore.DestTarget.Name,
		DestSubpath:        toNullString(restore.DestSubpath),
		Comment:            toNullString(restore.Comment),
		CurrentPid:         intToNullString(restore.CurrentPID),
		LastRunUpid:        toNullString(restore.History.LastRunUpid),
		LastSuccessfulUpid: toNullString(restore.History.LastSuccessfulUpid),
		Retry:              toNullInt64(restore.Retry),
		RetryInterval:      toNullInt64(restore.RetryInterval),
		PreScript:          restore.PreScript,
		PostScript:         restore.PostScript,
	})
	if err != nil {
		return fmt.Errorf("CreateRestore: error inserting restore: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetRestore(id string) (Restore, error) {
	row, err := database.readQueries.GetRestore(database.ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Restore{}, ErrRestoreNotFound
	}
	if err != nil {
		return Restore{}, fmt.Errorf("GetRestore: error querying restore: %w", err)
	}

	restore := Restore{
		ID:       row.ID,
		Store:    row.Store,
		Snapshot: row.Snapshot,
		SrcPath:  row.SrcPath,
		DestTarget: Target{
			Name: row.DestTarget,
			Path: row.Path.String,
			AgentHost: AgentHost{
				Name:            row.AgentName.String,
				IP:              row.AgentIp.String,
				Auth:            row.AgentAuth.String,
				TokenUsed:       row.AgentTokenUsed.String,
				OperatingSystem: row.AgentOs.String,
			},
			VolumeID:         row.VolumeID.String,
			MountScript:      row.MountScript.String,
			VolumeType:       row.VolumeType.String,
			VolumeName:       row.VolumeName.String,
			VolumeFS:         row.VolumeFs.String,
			VolumeTotalBytes: int(row.VolumeTotalBytes.Int64),
			VolumeUsedBytes:  int(row.VolumeUsedBytes.Int64),
			VolumeFreeBytes:  int(row.VolumeFreeBytes.Int64),
			VolumeTotal:      row.VolumeTotal.String,
			VolumeUsed:       row.VolumeUsed.String,
			VolumeFree:       row.VolumeFree.String,
		},
		DestSubpath: fromNullString(row.DestSubpath),
		Comment:     fromNullString(row.Comment),
		CurrentPID:  fromNullStringToInt(row.CurrentPid),
		History: JobHistory{
			LastRunUpid:        fromNullString(row.LastRunUpid),
			LastSuccessfulUpid: fromNullString(row.LastSuccessfulUpid),
		},
		Retry:         fromNullInt64(row.Retry),
		RetryInterval: fromNullInt64(row.RetryInterval),
		PreScript:     row.PreScript,
		PostScript:    row.PostScript,
	}

	restore.DestTarget.populateInfo()

	if row.Namespace.Valid {
		restore.Namespace = row.Namespace.String
	}

	database.populateRestoreExtras(&restore)

	return restore, nil
}

func (database *Database) populateRestoreExtras(restore *Restore) {
	if restore.History.LastRunUpid != "" {
		task, err := proxmox.GetTaskByUPID(restore.History.LastRunUpid)
		if err == nil {
			restore.History.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				restore.History.LastRunState = task.ExitStatus
				restore.History.Duration = task.EndTime - task.StartTime
			} else if task.StartTime > 0 {
				restore.History.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if restore.History.LastSuccessfulUpid != "" {
		if successTask, err := proxmox.GetTaskByUPID(restore.History.LastSuccessfulUpid); err == nil {
			restore.History.LastSuccessfulEndtime = successTask.EndTime
		}
	}
}

func (database *Database) UpdateRestore(tx *Transaction, restore Restore) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
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
		q = database.queries.WithTx(tx.Tx)
	}

	// Validation
	if !utils.IsValidID(restore.ID) && restore.ID != "" {
		return fmt.Errorf("UpdateRestore: invalid id string -> %s", restore.ID)
	}
	if restore.DestTarget.Name == "" {
		return errors.New("dest target is empty")
	}
	if restore.Snapshot == "" {
		return errors.New("snapshot is empty")
	}
	if restore.Store == "" {
		return errors.New("datastore is empty")
	}
	if !utils.IsValidPathString(restore.SrcPath) {
		return fmt.Errorf("invalid source path string: %s", restore.SrcPath)
	}
	if !utils.IsValidPathString(restore.DestSubpath) {
		return fmt.Errorf("invalid dest path string: %s", restore.DestSubpath)
	}
	if restore.RetryInterval <= 0 {
		restore.RetryInterval = 1
	}
	if restore.Retry < 0 {
		restore.Retry = 0
	}

	err = q.UpdateRestore(database.ctx, sqlc.UpdateRestoreParams{
		Store:              restore.Store,
		Namespace:          sql.NullString{String: restore.Namespace, Valid: restore.Namespace != ""},
		Snapshot:           restore.Snapshot,
		SrcPath:            restore.SrcPath,
		DestTarget:         restore.DestTarget.Name,
		DestSubpath:        toNullString(restore.DestSubpath),
		Comment:            toNullString(restore.Comment),
		CurrentPid:         intToNullString(restore.CurrentPID),
		LastRunUpid:        toNullString(restore.History.LastRunUpid),
		LastSuccessfulUpid: toNullString(restore.History.LastSuccessfulUpid),
		Retry:              toNullInt64(restore.Retry),
		RetryInterval:      toNullInt64(restore.RetryInterval),
		PreScript:          restore.PreScript,
		PostScript:         restore.PostScript,
		ID:                 restore.ID,
	})
	if err != nil {
		return fmt.Errorf("UpdateRestore: error updating restore: %w", err)
	}

	if restore.History.LastRunUpid != "" {
		go database.linkRestoreLog(restore.ID, restore.History.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

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
		syslog.L.Error(fmt.Errorf("linkRestoreLog: original log path does not exist: %w", err)).
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

func (database *Database) GetAllRestores() ([]Restore, error) {
	rows, err := database.readQueries.ListAllRestores(database.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllRestores: error querying restores: %w", err)
	}

	restores := make([]Restore, len(rows))
	for i, row := range rows {
		restore := Restore{
			ID:       row.ID,
			Store:    row.Store,
			Snapshot: row.Snapshot,
			SrcPath:  row.SrcPath,
			DestTarget: Target{
				Name: row.DestTarget,
				Path: row.Path.String,
				AgentHost: AgentHost{
					Name:            row.AgentName.String,
					IP:              row.AgentIp.String,
					Auth:            row.AgentAuth.String,
					TokenUsed:       row.AgentTokenUsed.String,
					OperatingSystem: row.AgentOs.String,
				},
				VolumeID:         row.VolumeID.String,
				MountScript:      row.MountScript.String,
				VolumeType:       row.VolumeType.String,
				VolumeName:       row.VolumeName.String,
				VolumeFS:         row.VolumeFs.String,
				VolumeTotalBytes: int(row.VolumeTotalBytes.Int64),
				VolumeUsedBytes:  int(row.VolumeUsedBytes.Int64),
				VolumeFreeBytes:  int(row.VolumeFreeBytes.Int64),
				VolumeTotal:      row.VolumeTotal.String,
				VolumeUsed:       row.VolumeUsed.String,
				VolumeFree:       row.VolumeFree.String,
			},
			DestSubpath: fromNullString(row.DestSubpath),
			Comment:     fromNullString(row.Comment),
			CurrentPID:  fromNullStringToInt(row.CurrentPid),
			History: JobHistory{
				LastRunUpid:        fromNullString(row.LastRunUpid),
				LastSuccessfulUpid: fromNullString(row.LastSuccessfulUpid),
			},
			Retry:         fromNullInt64(row.Retry),
			RetryInterval: fromNullInt64(row.RetryInterval),
			PreScript:     row.PreScript,
			PostScript:    row.PostScript,
		}

		if row.Namespace.Valid {
			restore.Namespace = row.Namespace.String
		}

		restore.DestTarget.populateInfo()

		database.populateRestoreExtras(&restore)
		restores[i] = restore
	}

	return restores, nil
}

func (database *Database) GetAllQueuedRestores() ([]Restore, error) {
	rows, err := database.readQueries.ListQueuedRestores(database.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllQueuedRestores: error querying restores: %w", err)
	}

	restores := make([]Restore, len(rows))
	for i, row := range rows {
		restore := Restore{
			ID:       row.ID,
			Store:    row.Store,
			Snapshot: row.Snapshot,
			SrcPath:  row.SrcPath,
			DestTarget: Target{
				Name:      row.DestTarget,
				AgentHost: AgentHost{},
			},
			DestSubpath: fromNullString(row.DestSubpath),
			Comment:     fromNullString(row.Comment),
			CurrentPID:  fromNullStringToInt(row.CurrentPid),
			History: JobHistory{
				LastRunUpid:        fromNullString(row.LastRunUpid),
				LastSuccessfulUpid: fromNullString(row.LastSuccessfulUpid),
			},
			Retry:         fromNullInt64(row.Retry),
			RetryInterval: fromNullInt64(row.RetryInterval),
			PreScript:     row.PreScript,
			PostScript:    row.PostScript,
		}

		if row.Namespace.Valid {
			restore.Namespace = row.Namespace.String
		}

		database.populateRestoreExtras(&restore)
		restores[i] = restore
	}

	return restores, nil
}

func (database *Database) DeleteRestore(tx *Transaction, id string) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
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
		q = database.queries.WithTx(tx.Tx)
	}

	rowsAffected, err := q.DeleteRestore(database.ctx, id)
	if err != nil {
		return fmt.Errorf("DeleteRestore: error deleting restore %s: %w", id, err)
	}

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

func (r *Restore) GetStreamID() string {
	if r.DestTarget.Type == TargetTypeLocal {
		return ""
	}

	if r.DestTarget.Type == TargetTypeS3 {
		return r.DestTarget.S3Info.Endpoint + "|" + r.ID + "|restore"
	}

	return r.DestTarget.AgentHost.Name + "|" + r.ID + "|restore"
}

