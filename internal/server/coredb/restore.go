//go:build linux

package coredb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func (db *Store) generateUniqueRestoreID(restore Restore) (string, error) {
	baseID := validate.Slugify(restore.DestTarget.Name)
	if baseID == "" {
		return "", fmt.Errorf("invalid target: slugified value is empty")
	}

	for idx := range maxAttempts {
		var newID string
		if idx == 0 {
			newID = baseID
		} else {
			newID = fmt.Sprintf("%s-%d", baseID, idx)
		}

		_, err := db.readQueries.RestoreExists(db.ctx, newID)
		if errors.Is(err, sql.ErrNoRows) {
			return newID, nil
		}
		if err != nil {
			return "", fmt.Errorf("generateUniqueRestoreID: error checking restore existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique restore ID after %d attempts", maxAttempts)
}

func (db *Store) CreateRestore(tx *Transaction, restore Restore) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateRestore: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateRestore: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateRestore: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateRestore: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if restore.ID == "" {
		id, err := db.generateUniqueRestoreID(restore)
		if err != nil {
			return fmt.Errorf("CreateRestore: failed to generate unique id -> %w", err)
		}
		restore.ID = id
	}

	if restore.DestTarget.Name == "" {
		return fmt.Errorf("%w: dest target is empty", ErrValidationFailed)
	}
	if restore.Snapshot == "" {
		return fmt.Errorf("%w: snapshot is empty", ErrValidationFailed)
	}
	if restore.Store == "" {
		return fmt.Errorf("%w: datastore is empty", ErrValidationFailed)
	}
	if !validate.IsValidID(restore.ID) && restore.ID != "" {
		return fmt.Errorf("CreateRestore: invalid id string -> %s", restore.ID)
	}
	if !validate.IsValidPathString(restore.SrcPath) {
		return fmt.Errorf("invalid source path string: %s", restore.SrcPath)
	}
	if !validate.IsValidPathString(restore.DestSubpath) {
		return fmt.Errorf("invalid dest path string: %s", restore.DestSubpath)
	}
	if restore.RetryInterval <= 0 {
		restore.RetryInterval = 1
	}
	if restore.Retry < 0 {
		restore.Retry = 0
	}
	if restore.Mode < 0 {
		restore.Mode = 0
	}

	err = q.CreateRestore(db.ctx, corequery.CreateRestoreParams{
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
		RestoreMode:        int64(restore.Mode),
		LastRunStatus:      toNullInt64(int(restore.History.LastRunStatus)),
		RetryCount:         toNullInt64(restore.History.RetryCount),
		NotificationMode:   toNullString(restore.NotificationMode),
	})
	if err != nil {
		return fmt.Errorf("CreateRestore: error inserting restore: %w", err)
	}
	if err = db.storeRestoreDatabaseOptions(q, restore); err != nil {
		return fmt.Errorf("CreateRestore: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetRestore(id string) (Restore, error) {
	row, err := db.readQueries.GetRestore(db.ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Restore{}, ErrRestoreNotFound
	}
	if err != nil {
		return Restore{}, fmt.Errorf("GetRestore: error querying restore: %w", err)
	}

	restore := Restore{
		ID:               row.ID,
		Store:            row.Store,
		Snapshot:         row.Snapshot,
		SrcPath:          row.SrcPath,
		NotificationMode: fromNullString(row.NotificationMode),
		DestTarget: Target{
			Name:   row.DestTarget,
			Type:   TargetType(fromNullString(row.TargetType)),
			Access: FilesystemAccess(row.FilesystemAccess),
			Path:   row.Path,
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
			LastRunStatus:      JobStatus(fromNullInt64(row.LastRunStatus)),
			RetryCount:         fromNullInt64(row.RetryCount),
		},
		Retry:               fromNullInt64(row.Retry),
		RetryInterval:       fromNullInt64(row.RetryInterval),
		Mode:                int(row.RestoreMode),
		PreScript:           row.PreScript,
		PostScript:          row.PostScript,
		SourceDatabase:      row.SourceDatabase,
		DestinationDatabase: row.DestinationDatabase,
		ReplaceExisting:     row.ReplaceExisting != 0,
	}
	restore.DestTarget.DatabaseHost = row.DatabaseHost
	restore.DestTarget.DatabasePort = int(row.DatabasePort)
	restore.DestTarget.DatabaseUsername = row.DatabaseUsername
	restore.DestTarget.DatabaseTLSMode = row.DatabaseTlsMode
	restore.DestTarget.DatabaseCACertificate = row.DatabaseCaCertificate
	restore.DestTarget.DatabaseDefaultClientDir = row.DatabaseDefaultClientDir
	restore.DestTarget.DatabaseVariant = row.DatabaseVariant
	restore.DestTarget.DatabaseClientFamily = row.DatabaseDefaultClientFamily

	restore.DestTarget.populateInfo()

	if row.Namespace.Valid {
		restore.Namespace = row.Namespace.String
	}

	db.populateRestoreExtras(&restore)

	return restore, nil
}

func (db *Store) populateRestoreExtras(restore *Restore) {
	if restore.History.LastRunUpid != "" {
		task, err := tasklog.GetTaskByUPID(restore.History.LastRunUpid)
		if err == nil {
			restore.History.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				restore.History.LastRunState = task.ExitStatus
				restore.History.Duration = task.EndTime - task.StartTime
			} else if qs := tasklog.QueuedState(task.UPID); qs != "" {
				restore.History.LastRunState = qs
			} else if task.StartTime > 0 {
				restore.History.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if restore.History.LastSuccessfulUpid != "" {
		if successTask, err := tasklog.GetTaskByUPID(restore.History.LastSuccessfulUpid); err == nil {
			restore.History.LastSuccessfulEndtime = successTask.EndTime
		}
	}
}

func (db *Store) UpdateRestore(tx *Transaction, restore Restore) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateRestore: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateRestore: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateRestore: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateRestore: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if !validate.IsValidID(restore.ID) && restore.ID != "" {
		return fmt.Errorf("UpdateRestore: invalid id string -> %s", restore.ID)
	}
	if restore.DestTarget.Name == "" {
		return fmt.Errorf("%w: dest target is empty", ErrValidationFailed)
	}
	if restore.Snapshot == "" {
		return fmt.Errorf("%w: snapshot is empty", ErrValidationFailed)
	}
	if restore.Store == "" {
		return fmt.Errorf("%w: datastore is empty", ErrValidationFailed)
	}
	if !validate.IsValidPathString(restore.SrcPath) {
		return fmt.Errorf("invalid source path string: %s", restore.SrcPath)
	}
	if !validate.IsValidPathString(restore.DestSubpath) {
		return fmt.Errorf("invalid dest path string: %s", restore.DestSubpath)
	}
	if restore.RetryInterval <= 0 {
		restore.RetryInterval = 1
	}
	if restore.Retry < 0 {
		restore.Retry = 0
	}
	if restore.Mode < 0 {
		restore.Mode = 0
	}

	err = q.UpdateRestore(db.ctx, corequery.UpdateRestoreParams{
		Store:              restore.Store,
		Namespace:          sql.NullString{String: restore.Namespace, Valid: restore.Namespace != ""},
		Snapshot:           restore.Snapshot,
		SrcPath:            restore.SrcPath,
		RestoreMode:        int64(restore.Mode),
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
		LastRunStatus:      toNullInt64(int(restore.History.LastRunStatus)),
		RetryCount:         toNullInt64(restore.History.RetryCount),
		NotificationMode:   toNullString(restore.NotificationMode),
		ID:                 restore.ID,
	})
	if err != nil {
		return fmt.Errorf("UpdateRestore: error updating restore: %w", err)
	}
	if err = db.storeRestoreDatabaseOptions(q, restore); err != nil {
		return fmt.Errorf("UpdateRestore: %w", err)
	}

	if restore.History.LastRunUpid != "" {
		go db.linkRestoreLog(restore.ID, restore.History.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

func (db *Store) linkRestoreLog(restoreID, upid string) {
	restoreLogsPath := filepath.Join(conf.RestoreLogsBasePath, restoreID)
	if err := os.MkdirAll(restoreLogsPath, 0755); err != nil {
		log.Error(fmt.Errorf("linkRestoreLog: failed to create log dir: %w", err), "", "id", restoreID)

		return
	}

	restoreLogPath := filepath.Join(restoreLogsPath, upid)
	if _, err := os.Lstat(restoreLogPath); err != nil && !os.IsNotExist(err) {
		log.Error(fmt.Errorf("linkRestoreLog: failed to stat potential symlink: %w", err), "", "path", restoreLogPath)

		return
	}

	origLogPath, err := tasklog.UPIDLogPath(upid)
	if err != nil {
		log.Error(fmt.Errorf("linkRestoreLog: failed to get original log path: %w", err), "", "upid", upid, "id", restoreID)

		return
	}

	if _, err := os.Stat(origLogPath); err != nil {
		log.Error(fmt.Errorf("linkRestoreLog: original log path does not exist: %w", err), "", "id", restoreID, "orig_path", origLogPath)

		return
	}

	if err := os.Remove(restoreLogPath); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}

	err = os.Symlink(origLogPath, restoreLogPath)
	if err != nil {
		log.Error(fmt.Errorf("linkRestoreLog: failed to create symlink: %w", err), "", "link", restoreLogPath, "source", origLogPath, "id", restoreID)

	}
}

func (db *Store) GetAllRestores() ([]Restore, error) {
	rows, err := db.readQueries.ListAllRestores(db.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllRestores: error querying restores: %w", err)
	}

	restores := make([]Restore, len(rows))
	for i, row := range rows {
		restore := Restore{
			ID:               row.ID,
			Store:            row.Store,
			Snapshot:         row.Snapshot,
			SrcPath:          row.SrcPath,
			Mode:             int(row.RestoreMode),
			NotificationMode: fromNullString(row.NotificationMode),
			DestTarget: Target{
				Name:   row.DestTarget,
				Type:   TargetType(fromNullString(row.TargetType)),
				Access: FilesystemAccess(row.FilesystemAccess),
				Path:   row.Path,
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
				LastRunStatus:      JobStatus(fromNullInt64(row.LastRunStatus)),
				RetryCount:         fromNullInt64(row.RetryCount),
			},
			Retry:               fromNullInt64(row.Retry),
			RetryInterval:       fromNullInt64(row.RetryInterval),
			PreScript:           row.PreScript,
			PostScript:          row.PostScript,
			SourceDatabase:      row.SourceDatabase,
			DestinationDatabase: row.DestinationDatabase,
			ReplaceExisting:     row.ReplaceExisting != 0,
		}
		restore.DestTarget.DatabaseHost = row.DatabaseHost
		restore.DestTarget.DatabasePort = int(row.DatabasePort)
		restore.DestTarget.DatabaseUsername = row.DatabaseUsername
		restore.DestTarget.DatabaseTLSMode = row.DatabaseTlsMode
		restore.DestTarget.DatabaseCACertificate = row.DatabaseCaCertificate
		restore.DestTarget.DatabaseDefaultClientDir = row.DatabaseDefaultClientDir
		restore.DestTarget.DatabaseVariant = row.DatabaseVariant
		restore.DestTarget.DatabaseClientFamily = row.DatabaseDefaultClientFamily

		if row.Namespace.Valid {
			restore.Namespace = row.Namespace.String
		}

		restore.DestTarget.populateInfo()

		db.populateRestoreExtras(&restore)
		restores[i] = restore
	}

	return restores, nil
}

func (db *Store) DeleteRestore(tx *Transaction, id string) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteRestore: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteRestore: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteRestore: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteRestore: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	rowsAffected, err := q.DeleteRestore(db.ctx, id)
	if err != nil {
		return fmt.Errorf("DeleteRestore: error deleting restore %s: %w", id, err)
	}

	if rowsAffected == 0 {
		return ErrRestoreNotFound
	}

	restoreLogsPath := filepath.Join(conf.RestoreLogsBasePath, id)
	if err := os.RemoveAll(restoreLogsPath); err != nil && !os.IsNotExist(err) {
		if !os.IsNotExist(err) {
			log.Error(fmt.Errorf("DeleteRestore: failed removing restore logs: %w", err), "", "id", id)

		}
	}

	commitNeeded = true
	return nil
}

func (r *Restore) GetAllUPIDs() []Tasks {
	restoreLogsPath := filepath.Join(conf.RestoreLogsBasePath, r.ID)
	if err := os.MkdirAll(restoreLogsPath, 0755); err != nil {
		log.Error(fmt.Errorf("GetAllUPIDs: failed to get log dir: %w", err), "", "id", r.ID)

		return nil
	}

	logs, err := os.ReadDir(restoreLogsPath)
	if err != nil {
		log.Error(fmt.Errorf("GetAllUPIDs: failed to read dir: %w", err), "", "id", r.ID)

		return nil
	}

	upids := make([]Tasks, 0, len(logs))

	for _, entry := range logs {
		if tasklog.IsQueuedUPID(entry.Name()) {
			if err := os.Remove(filepath.Join(restoreLogsPath, entry.Name())); err != nil && !os.IsNotExist(err) {
				log.Error(fmt.Errorf("GetAllUPIDs: failed removing queued task link: %w", err), "", "id", r.ID)
			}
			continue
		}
		task, err := tasklog.GetTaskByUPID(entry.Name())
		if err != nil {
			continue
		}
		upids = append(upids, Tasks{
			UPID:    task.UPID,
			Endtime: task.EndTime,
			Status:  task.ExitStatus,
		})
	}

	return upids
}

func (r *Restore) GetStreamID() string {
	if r.DestTarget.IsLocal() {
		return ""
	}

	if r.DestTarget.Type == TargetTypeS3 {
		return r.DestTarget.S3Info.Endpoint + "|" + r.ID + "|restore"
	}

	return r.DestTarget.AgentHost.Name + "|" + r.ID + "|restore"
}

type Restore struct {
	ID                  string     `json:"id"`
	Store               string     `json:"store"`
	Snapshot            string     `json:"snapshot"`
	Namespace           string     `json:"ns"`
	Mode                int        `json:"mode"`
	SrcPath             string     `json:"src-path"`
	DestTarget          Target     `json:"dest-target"`
	DestSubpath         string     `json:"dest-subpath"`
	PreScript           string     `json:"pre_script"`
	PostScript          string     `json:"post_script"`
	Comment             string     `json:"comment"`
	NotificationMode    string     `json:"notification-mode"`
	Retry               int        `json:"retry"`
	RetryInterval       int        `json:"retry-interval"`
	CurrentPID          int        `json:"current_pid"`
	ExpectedSize        int        `json:"expected_size,omitempty"`
	UPIDs               []string   `json:"upids"`
	CurrentStats        JobStats   `json:"current-stats"`
	History             JobHistory `json:"history"`
	SourceDatabase      string     `json:"source_database,omitempty"`
	DestinationDatabase string     `json:"destination_database,omitempty"`
	ReplaceExisting     bool       `json:"replace_existing,omitempty"`
}

func (db *Store) storeRestoreDatabaseOptions(q *corequery.Queries, restore Restore) error {
	if restore.SourceDatabase == "" && restore.DestinationDatabase == "" && !restore.ReplaceExisting {
		return q.DeleteRestoreDatabaseOptions(db.ctx, restore.ID)
	}

	target, err := q.GetTarget(db.ctx, restore.DestTarget.Name)
	if err != nil {
		return fmt.Errorf("error fetching destination target: %w", err)
	}
	if TargetType(target.TargetType) != TargetTypePostgreSQL && TargetType(target.TargetType) != TargetTypeMySQL {
		return q.DeleteRestoreDatabaseOptions(db.ctx, restore.ID)
	}
	return q.UpsertRestoreDatabaseOptions(db.ctx, corequery.UpsertRestoreDatabaseOptionsParams{
		RestoreID:           restore.ID,
		SourceDatabase:      restore.SourceDatabase,
		DestinationDatabase: restore.DestinationDatabase,
		ReplaceExisting:     boolToNullInt64(restore.ReplaceExisting).Int64,
	})
}
