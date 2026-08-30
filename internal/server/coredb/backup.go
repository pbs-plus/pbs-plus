//go:build linux

package coredb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func (db *Store) generateUniqueJobId(backup Backup) (string, error) {
	baseID := validate.Slugify(backup.Target.Name)
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

		_, err := db.readQueries.BackupExists(db.ctx, newID)
		if errors.Is(err, sql.ErrNoRows) {
			return newID, nil
		}
		if err != nil {
			return "", fmt.Errorf("generateUniqueJobId: error checking backup existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique backup ID after %d attempts", maxAttempts)
}

func (db *Store) CreateBackup(tx *Transaction, backup Backup) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateBackup: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateBackup: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateBackup: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateBackup: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if backup.ID == "" {
		id, err := db.generateUniqueJobId(backup)
		if err != nil {
			return fmt.Errorf("CreateBackup: failed to generate unique id -> %w", err)
		}
		backup.ID = id
	}

	if backup.Target.Name == "" {
		return fmt.Errorf("%w: target is empty", ErrValidationFailed)
	}
	if backup.Store == "" {
		return fmt.Errorf("%w: datastore is empty", ErrValidationFailed)
	}
	if !validate.IsValidID(backup.ID) && backup.ID != "" {
		return fmt.Errorf("CreateBackup: invalid id string -> %s", backup.ID)
	}
	if !validate.IsValidNamespace(backup.Namespace) && backup.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", backup.Namespace)
	}
	if err := validate.ValidateOnCalendar(backup.Schedule); err != nil && backup.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", backup.Schedule)
	}
	if !validate.IsValidPathString(backup.Subpath) {
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

	err = q.CreateBackup(db.ctx, corequery.CreateBackupParams{
		ID:                 backup.ID,
		Store:              backup.Store,
		Mode:               toNullString(backup.Mode),
		SourceMode:         toNullString(backup.SourceMode),
		ReadMode:           toNullString(backup.ReadMode),
		Target:             backup.Target.Name,
		Subpath:            toNullString(backup.Subpath),
		Schedule:           toNullString(backup.Schedule),
		Comment:            toNullString(backup.Comment),
		NotificationMode:   toNullString(backup.NotificationMode),
		Namespace:          toNullString(backup.Namespace),
		CurrentPid:         intToNullString(backup.CurrentPID),
		LastRunUpid:        toNullString(backup.History.LastRunUpid),
		LastSuccessfulUpid: toNullString(backup.History.LastSuccessfulUpid),
		Retry:              toNullInt64(backup.Retry),
		RetryInterval:      toNullInt64(backup.RetryInterval),
		MaxDirEntries:      toNullInt64(backup.MaxDirEntries),
		PreScript:          backup.PreScript,
		PostScript:         backup.PostScript,
		IncludeXattr:       boolToNullInt64(backup.IncludeXattr),
		LegacyXattr:        boolToNullInt64(backup.LegacyXattr),
		LastRunStatus:      toNullInt64(int(backup.History.LastRunStatus)),
		RetryCount:         toNullInt64(backup.History.RetryCount),
	})
	if err != nil {
		return fmt.Errorf("CreateBackup: error inserting backup: %w", err)
	}

	for _, exclusion := range backup.Exclusions {
		if exclusion.JobID == "" {
			exclusion.JobID = backup.ID
		}
		err = q.CreateExclusion(db.ctx, corequery.CreateExclusionParams{
			JobID:   exclusion.JobID,
			Path:    exclusion.Path,
			Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
		})
		if err != nil {
			log.Error(fmt.Errorf("CreateBackup: failed to create exclusion: %w", err), "", "path", exclusion.Path, "backup_id", backup.ID)

			return fmt.Errorf("CreateBackup: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetBackup(id string) (Backup, error) {
	row, err := db.readQueries.GetBackup(db.ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Backup{}, ErrBackupNotFound
	}
	if err != nil {
		return Backup{}, fmt.Errorf("GetBackup: error querying backup: %w", err)
	}

	backup := Backup{
		ID:         row.ID,
		Store:      row.Store,
		Mode:       interfaceToString(row.Mode),
		SourceMode: interfaceToString(row.SourceMode),
		ReadMode:   interfaceToString(row.ReadMode),
		Target: Target{
			Name:   row.Target,
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
		Subpath:          row.Subpath.String,
		Schedule:         row.Schedule.String,
		Comment:          row.Comment.String,
		NotificationMode: row.NotificationMode.String,
		Namespace:        row.Namespace.String,
		CurrentPID:       fromNullStringToInt(row.CurrentPid),
		History: JobHistory{
			LastRunUpid:        fromNullString(row.LastRunUpid),
			LastSuccessfulUpid: fromNullString(row.LastSuccessfulUpid),
			LastRunStatus:      JobStatus(fromNullInt64(row.LastRunStatus)),
			RetryCount:         fromNullInt64(row.RetryCount),
		},
		Retry:         fromNullInt64(row.Retry),
		RetryInterval: fromNullInt64(row.RetryInterval),
		MaxDirEntries: fromNullInt64(row.MaxDirEntries),
		PreScript:     row.PreScript,
		PostScript:    row.PostScript,
		IncludeXattr:  fromNullInt64ToBool(row.IncludeXattr),
		LegacyXattr:   fromNullInt64ToBool(row.LegacyXattr),
	}

	exclusions, err := db.readQueries.GetBackupExclusions(db.ctx, id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Backup{}, fmt.Errorf("GetBackup: error getting exclusions: %w", err)
	}

	backup.Target.populateInfo()

	backup.Exclusions = make([]Exclusion, len(exclusions))
	exclusionPaths := make([]string, len(exclusions))
	for i, excl := range exclusions {
		backup.Exclusions[i] = Exclusion{
			JobID: excl.JobID,
			Path:  excl.Path,
		}
		exclusionPaths[i] = excl.Path
	}
	backup.RawExclusions = strings.Join(exclusionPaths, "\n")

	db.populateBackupExtras(&backup)

	return backup, nil
}

func (db *Store) populateBackupExtras(backup *Backup) {
	if backup.History.LastRunUpid != "" {
		if r, ok := tasklog.ResolveHistoryFields(backup.History.LastRunUpid); ok {
			if r.Starttime > 0 {
				backup.History.LastRunStarttime = r.Starttime
			}
			if r.Endtime > 0 {
				backup.History.LastRunEndtime = r.Endtime
				backup.History.Duration = r.Duration
			} else if r.Starttime > 0 {
				backup.History.Duration = r.Duration
			}
			if r.State != "" {
				backup.History.LastRunState = r.State
			}
		}
	}
	if backup.History.LastSuccessfulUpid != "" {
		if successTask, err := tasklog.GetTaskByUPID(backup.History.LastSuccessfulUpid); err == nil {
			backup.History.LastSuccessfulEndtime = successTask.EndTime
		}
	}

	if nextSchedule, err := backup.getNextSchedule(db.ctx); err == nil && nextSchedule != nil {
		backup.NextRun = nextSchedule.Unix()
	}
}

func (db *Store) UpdateBackup(tx *Transaction, backup Backup) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateBackup: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateBackup: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateBackup: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateBackup: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if !validate.IsValidID(backup.ID) && backup.ID != "" {
		return fmt.Errorf("UpdateBackup: invalid id string -> %s", backup.ID)
	}
	if backup.Target.Name == "" {
		return fmt.Errorf("%w: target is empty", ErrValidationFailed)
	}
	if backup.Store == "" {
		return fmt.Errorf("%w: datastore is empty", ErrValidationFailed)
	}
	if backup.RetryInterval <= 0 {
		backup.RetryInterval = 1
	}
	if backup.Retry < 0 {
		backup.Retry = 0
	}
	if !validate.IsValidNamespace(backup.Namespace) && backup.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", backup.Namespace)
	}
	if err := validate.ValidateOnCalendar(backup.Schedule); err != nil && backup.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", backup.Schedule)
	}
	if !validate.IsValidPathString(backup.Subpath) {
		return fmt.Errorf("invalid subpath string: %s", backup.Subpath)
	}
	if strings.TrimSpace(backup.ReadMode) == "" {
		backup.ReadMode = "standard"
	}
	if strings.TrimSpace(backup.SourceMode) == "" {
		backup.SourceMode = "snapshot"
	}

	err = q.UpdateBackup(db.ctx, corequery.UpdateBackupParams{
		Store:              backup.Store,
		Mode:               backup.Mode,
		SourceMode:         backup.SourceMode,
		ReadMode:           backup.ReadMode,
		Target:             backup.Target.Name,
		Subpath:            toNullString(backup.Subpath),
		Schedule:           toNullString(backup.Schedule),
		Comment:            toNullString(backup.Comment),
		NotificationMode:   toNullString(backup.NotificationMode),
		Namespace:          toNullString(backup.Namespace),
		CurrentPid:         intToNullString(backup.CurrentPID),
		LastRunUpid:        toNullString(backup.History.LastRunUpid),
		Retry:              toNullInt64(backup.Retry),
		RetryInterval:      toNullInt64(backup.RetryInterval),
		LastSuccessfulUpid: toNullString(backup.History.LastSuccessfulUpid),
		PreScript:          backup.PreScript,
		PostScript:         backup.PostScript,
		MaxDirEntries:      toNullInt64(backup.MaxDirEntries),
		IncludeXattr:       boolToNullInt64(backup.IncludeXattr),
		LegacyXattr:        boolToNullInt64(backup.LegacyXattr),
		LastRunStatus:      toNullInt64(int(backup.History.LastRunStatus)),
		RetryCount:         toNullInt64(backup.History.RetryCount),
		ID:                 backup.ID,
	})
	if err != nil {
		return fmt.Errorf("UpdateBackup: error updating backup: %w", err)
	}

	err = q.DeleteBackupExclusions(db.ctx, backup.ID)
	if err != nil {
		return fmt.Errorf("UpdateBackup: error removing old exclusions: %w", err)
	}

	for _, exclusion := range backup.Exclusions {
		err = q.CreateExclusion(db.ctx, corequery.CreateExclusionParams{
			JobID:   backup.ID,
			Path:    exclusion.Path,
			Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
		})
		if err != nil {
			log.Error(fmt.Errorf("UpdateBackup: failed to create exclusion: %w", err), "", "path", exclusion.Path, "backup_id", backup.ID)

			return fmt.Errorf("UpdateBackup: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if backup.History.LastRunUpid != "" {
		go db.linkBackupLog(backup.ID, backup.History.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

func (db *Store) linkBackupLog(backupID, upid string) {
	backupLogsPath := filepath.Join(conf.BackupLogsBasePath, backupID)
	if err := os.MkdirAll(backupLogsPath, 0755); err != nil {
		log.Error(fmt.Errorf("linkBackupLog: failed to create log dir: %w", err), "", "id", backupID)

		return
	}

	backupLogPath := filepath.Join(backupLogsPath, upid)
	if _, err := os.Lstat(backupLogPath); err != nil && !os.IsNotExist(err) {
		log.Error(fmt.Errorf("linkBackupLog: failed to stat potential symlink: %w", err), "", "path", backupLogPath)

		return
	}

	origLogPath, err := tasklog.UPIDLogPath(upid)
	if err != nil {
		log.Error(fmt.Errorf("linkBackupLog: failed to get original log path: %w", err), "", "upid", upid, "id", backupID)

		return
	}

	if _, err := os.Stat(origLogPath); err != nil {
		log.Error(fmt.Errorf("linkBackupLog: original log path does not exist: %w", err), "", "id", backupID, "orig_path", origLogPath)

		return
	}

	if err := os.Remove(backupLogPath); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}

	err = os.Symlink(origLogPath, backupLogPath)
	if err != nil {
		log.Error(fmt.Errorf("linkBackupLog: failed to create symlink: %w", err), "", "link", backupLogPath, "source", origLogPath, "id", backupID)

	}
}

func (db *Store) GetAllBackups() ([]Backup, error) {
	rows, err := db.readQueries.ListAllBackups(db.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllBackups: error querying backups: %w", err)
	}

	allExclusions, err := db.readQueries.ListAllBackupExclusions(db.ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("GetAllBackups: error querying exclusions: %w", err)
	}

	exclusionsByJob := make(map[string][]Exclusion)
	for _, excl := range allExclusions {
		exclusionsByJob[excl.JobID] = append(exclusionsByJob[excl.JobID], Exclusion{
			JobID: excl.JobID,
			Path:  excl.Path,
		})
	}

	backups := make([]Backup, len(rows))
	for i, row := range rows {
		backup := Backup{
			ID:         row.ID,
			Store:      row.Store,
			Mode:       interfaceToString(row.Mode),
			SourceMode: interfaceToString(row.SourceMode),
			ReadMode:   interfaceToString(row.ReadMode),
			Target: Target{
				Name:   row.Target,
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
			Subpath:          row.Subpath.String,
			Schedule:         row.Schedule.String,
			Comment:          row.Comment.String,
			NotificationMode: row.NotificationMode.String,
			Namespace:        row.Namespace.String,
			CurrentPID:       fromNullStringToInt(row.CurrentPid),
			History: JobHistory{
				LastRunUpid:        fromNullString(row.LastRunUpid),
				LastSuccessfulUpid: fromNullString(row.LastSuccessfulUpid),
				LastRunStatus:      JobStatus(fromNullInt64(row.LastRunStatus)),
				RetryCount:         fromNullInt64(row.RetryCount),
			},
			Retry:         fromNullInt64(row.Retry),
			RetryInterval: fromNullInt64(row.RetryInterval),
			MaxDirEntries: fromNullInt64(row.MaxDirEntries),
			PreScript:     row.PreScript,
			PostScript:    row.PostScript,
			IncludeXattr:  fromNullInt64ToBool(row.IncludeXattr),
			LegacyXattr:   fromNullInt64ToBool(row.LegacyXattr),

			Exclusions: exclusionsByJob[row.ID]}
		if backup.Exclusions == nil {
			backup.Exclusions = make([]Exclusion, 0)
		}

		backup.Target.populateInfo()

		pathSlice := make([]string, len(backup.Exclusions))
		for k, exclusion := range backup.Exclusions {
			pathSlice[k] = exclusion.Path
		}
		backup.RawExclusions = strings.Join(pathSlice, "\n")

		db.populateBackupExtras(&backup)
		backups[i] = backup
	}

	return backups, nil
}

func (db *Store) DeleteBackup(tx *Transaction, id string) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteBackup: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteBackup: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteBackup: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteBackup: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	err = q.DeleteBackupExclusions(db.ctx, id)
	if err != nil {
		log.Error(fmt.Errorf("DeleteBackup: error deleting exclusions: %w", err), "", "id", id)

		return fmt.Errorf("DeleteBackup: error deleting exclusions: %w", err)
	}

	rowsAffected, err := q.DeleteBackup(db.ctx, id)
	if err != nil {
		return fmt.Errorf("DeleteBackup: error deleting backup %s: %w", id, err)
	}

	if rowsAffected == 0 {
		return ErrBackupNotFound
	}

	backupLogsPath := filepath.Join(conf.BackupLogsBasePath, id)
	if err := os.RemoveAll(backupLogsPath); err != nil && !os.IsNotExist(err) {
		if !os.IsNotExist(err) {
			log.Error(fmt.Errorf("DeleteBackup: failed removing backup logs: %w", err), "", "id", id)

		}
	}

	commitNeeded = true
	return nil
}

func (b *Backup) GetStreamID() string {
	if b.Target.IsLocal() {
		return ""
	}

	if b.Target.Type == TargetTypeS3 {
		return b.Target.S3Info.Endpoint + "|" + b.ID
	}

	return b.Target.AgentHost.Name + "|" + b.ID
}

func (b *Backup) GetAllUPIDs() []Tasks {
	backupLogsPath := filepath.Join(conf.BackupLogsBasePath, b.ID)
	if err := os.MkdirAll(backupLogsPath, 0755); err != nil {
		log.Error(fmt.Errorf("GetAllUPIDs: failed to get log dir: %w", err), "", "id", b.ID)

		return nil
	}

	logs, err := os.ReadDir(backupLogsPath)
	if err != nil {
		log.Error(fmt.Errorf("GetAllUPIDs: failed to read dir: %w", err), "", "id", b.ID)

		return nil
	}

	upids := make([]Tasks, 0, len(logs))

	for _, entry := range logs {
		if tasklog.IsQueuedUPID(entry.Name()) {
			if err := os.Remove(filepath.Join(backupLogsPath, entry.Name())); err != nil && !os.IsNotExist(err) {
				log.Error(fmt.Errorf("GetAllUPIDs: failed removing queued task link: %w", err), "", "id", b.ID)
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

type Backup struct {
	ID               string      `json:"id"`
	Store            string      `json:"store"`
	SourceMode       string      `json:"sourcemode"`
	ReadMode         string      `json:"readmode"`
	Mode             string      `json:"mode"`
	Target           Target      `json:"target"`
	IncludeXattr     bool        `json:"include-xattr"`
	LegacyXattr      bool        `json:"legacy-xattr"`
	Subpath          string      `json:"subpath"`
	Schedule         string      `json:"schedule"`
	Comment          string      `json:"comment"`
	NotificationMode string      `json:"notification-mode"`
	PreScript        string      `json:"pre_script"`
	PostScript       string      `json:"post_script"`
	Namespace        string      `json:"ns"`
	NextRun          int64       `json:"next-run"`
	Retry            int         `json:"retry"`
	RetryInterval    int         `json:"retry-interval"`
	MaxDirEntries    int         `json:"max-dir-entries"`
	CurrentPID       int         `json:"current_pid"`
	Exclusions       []Exclusion `json:"exclusions"`
	RawExclusions    string      `json:"rawexclusions"`
	UPIDs            []Tasks     `json:"upids"`
	CurrentStats     JobStats    `json:"current-stats"`
	History          JobHistory  `json:"history"`
}

type Tasks struct {
	UPID    string `json:"upid"`
	Endtime int64  `json:"endtime"`
	Status  string `json:"status"`
}

type JobStats struct {
	CurrentFileCount   int `json:"current_file_count,omitempty"`
	CurrentFolderCount int `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed  int `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed  int `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal  int `json:"current_bytes_total,omitempty"`
	StatCacheHits      int `json:"stat_cache_hits,omitempty"`
}

type JobHistory struct {
	LastRunUpid           string    `json:"last-run-upid"`
	LastRunStarttime      int64     `json:"last-run-starttime"`
	LastRunState          string    `json:"last-run-state"` // human-readable message (legacy, for display)
	LastRunStatus         JobStatus `json:"last-run-status"`
	LastRunEndtime        int64     `json:"last-run-endtime"`
	LastSuccessfulEndtime int64     `json:"last-successful-endtime"`
	LastSuccessfulUpid    string    `json:"last-successful-upid"`
	RetryCount            int       `json:"retry-count"`
	LatestSnapshotSize    int       `json:"latest_snapshot_size,omitempty"`
	Duration              int64     `json:"duration"`
}
