//go:build linux

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func (database *Database) generateUniqueJobId(backup Backup) (string, error) {
	baseID := utils.Slugify(backup.Target.Name)
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

		_, err := database.readQueries.BackupExists(database.ctx, newID)
		if errors.Is(err, sql.ErrNoRows) {
			return newID, nil
		}
		if err != nil {
			return "", fmt.Errorf("generateUniqueJobId: error checking backup existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique backup ID after %d attempts", maxAttempts)
}

func (database *Database) CreateBackup(tx *Transaction, backup Backup) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateBackup: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateBackup: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateBackup: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	if backup.ID == "" {
		id, err := database.generateUniqueJobId(backup)
		if err != nil {
			return fmt.Errorf("CreateBackup: failed to generate unique id -> %w", err)
		}
		backup.ID = id
	}

	// Validation
	if backup.Target.Name == "" {
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

	err = q.CreateBackup(database.ctx, sqlc.CreateBackupParams{
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
		IncludeXattr:       sql.NullInt64{Int64: boolToInt64(backup.IncludeXattr), Valid: true},
		LegacyXattr:        sql.NullInt64{Int64: boolToInt64(backup.LegacyXattr), Valid: true},
	})
	if err != nil {
		return fmt.Errorf("CreateBackup: error inserting backup: %w", err)
	}

	for _, exclusion := range backup.Exclusions {
		if exclusion.JobId == "" {
			exclusion.JobId = backup.ID
		}
		err = q.CreateExclusion(database.ctx, sqlc.CreateExclusionParams{
			JobID:   toNullString(exclusion.JobId),
			Path:    exclusion.Path,
			Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
		})
		if err != nil {
			syslog.L.Error(fmt.Errorf("CreateBackup: failed to create exclusion: %w", err)).
				WithField("backup_id", backup.ID).
				WithField("path", exclusion.Path).
				Write()
			return fmt.Errorf("CreateBackup: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = backup.setSchedule(database.ctx); err != nil {
		syslog.L.Error(fmt.Errorf("CreateBackup: failed to set schedule: %w", err)).
			WithField("id", backup.ID).
			Write()
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetBackup(id string) (Backup, error) {
	row, err := database.readQueries.GetBackup(database.ctx, id)
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
			Name: row.Target,
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
		Subpath:          row.Subpath.String,
		Schedule:         row.Schedule.String,
		Comment:          row.Comment.String,
		NotificationMode: row.NotificationMode.String,
		Namespace:        row.Namespace.String,
		CurrentPID:       fromNullStringToInt(row.CurrentPid),
		History: JobHistory{
			LastRunUpid:        fromNullString(row.LastRunUpid),
			LastSuccessfulUpid: fromNullString(row.LastSuccessfulUpid),
		},
		Retry:         fromNullInt64(row.Retry),
		RetryInterval: fromNullInt64(row.RetryInterval),
		MaxDirEntries: fromNullInt64(row.MaxDirEntries),
		PreScript:     row.PreScript,
		PostScript:    row.PostScript,
		IncludeXattr:  fromNullInt64ToBool(row.IncludeXattr),
		LegacyXattr:   fromNullInt64ToBool(row.LegacyXattr),
	}

	exclusions, err := database.readQueries.GetBackupExclusions(database.ctx, toNullString(id))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Backup{}, fmt.Errorf("GetBackup: error getting exclusions: %w", err)
	}

	backup.Target.populateInfo()

	backup.Exclusions = make([]Exclusion, len(exclusions))
	exclusionPaths := make([]string, len(exclusions))
	for i, excl := range exclusions {
		backup.Exclusions[i] = Exclusion{
			JobId: excl.JobID.String,
			Path:  excl.Path,
		}
		exclusionPaths[i] = excl.Path
	}
	backup.RawExclusions = strings.Join(exclusionPaths, "\n")

	database.populateBackupExtras(&backup)

	return backup, nil
}

func (database *Database) populateBackupExtras(backup *Backup) {
	if backup.History.LastRunUpid != "" {
		task, err := proxmox.GetTaskByUPID(backup.History.LastRunUpid)
		if err == nil {
			backup.History.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				backup.History.LastRunState = task.ExitStatus
				backup.History.Duration = task.EndTime - task.StartTime
			} else if task.StartTime > 0 {
				backup.History.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if backup.History.LastSuccessfulUpid != "" {
		if successTask, err := proxmox.GetTaskByUPID(backup.History.LastSuccessfulUpid); err == nil {
			backup.History.LastSuccessfulEndtime = successTask.EndTime
		}
	}

	if nextSchedule, err := backup.getNextSchedule(database.ctx); err == nil && nextSchedule != nil {
		backup.NextRun = nextSchedule.Unix()
	}
}

func (database *Database) UpdateBackup(tx *Transaction, backup Backup) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
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
	q = database.queries.WithTx(tx.Tx)

	// Validation
	if !utils.IsValidID(backup.ID) && backup.ID != "" {
		return fmt.Errorf("UpdateBackup: invalid id string -> %s", backup.ID)
	}
	if backup.Target.Name == "" {
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

	err = q.UpdateBackup(database.ctx, sqlc.UpdateBackupParams{
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
		ID:                 backup.ID,
	})
	if err != nil {
		return fmt.Errorf("UpdateBackup: error updating backup: %w", err)
	}

	err = q.DeleteBackupExclusions(database.ctx, toNullString(backup.ID))
	if err != nil {
		return fmt.Errorf("UpdateBackup: error removing old exclusions: %w", err)
	}

	for _, exclusion := range backup.Exclusions {
		err = q.CreateExclusion(database.ctx, sqlc.CreateExclusionParams{
			JobID:   toNullString(backup.ID),
			Path:    exclusion.Path,
			Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
		})
		if err != nil {
			syslog.L.Error(fmt.Errorf("UpdateBackup: failed to create exclusion: %w", err)).
				WithField("backup_id", backup.ID).
				WithField("path", exclusion.Path).
				Write()
			return fmt.Errorf("UpdateBackup: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = backup.setSchedule(database.ctx); err != nil {
		syslog.L.Error(fmt.Errorf("UpdateBackup: failed to set schedule: %w", err)).
			WithField("id", backup.ID).
			Write()
	}

	if backup.History.LastRunUpid != "" {
		go database.linkBackupLog(backup.ID, backup.History.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

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
		syslog.L.Error(fmt.Errorf("linkBackupLog: original log path does not exist: %w", err)).
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

func (database *Database) GetAllBackups() ([]Backup, error) {
	rows, err := database.readQueries.ListAllBackups(database.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllBackups: error querying backups: %w", err)
	}

	allExclusions, err := database.readQueries.ListAllBackupExclusions(database.ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("GetAllBackups: error querying exclusions: %w", err)
	}

	exclusionsByJob := make(map[string][]Exclusion)
	for _, excl := range allExclusions {
		exclusionsByJob[fromNullString(excl.JobID)] = append(exclusionsByJob[fromNullString(excl.JobID)], Exclusion{
			JobId: fromNullString(excl.JobID),
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
				Name: row.Target,
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
			Subpath:          row.Subpath.String,
			Schedule:         row.Schedule.String,
			Comment:          row.Comment.String,
			NotificationMode: row.NotificationMode.String,
			Namespace:        row.Namespace.String,
			CurrentPID:       fromNullStringToInt(row.CurrentPid),
			History: JobHistory{
				LastRunUpid:        fromNullString(row.LastRunUpid),
				LastSuccessfulUpid: fromNullString(row.LastSuccessfulUpid),
			},
			Retry:         fromNullInt64(row.Retry),
			RetryInterval: fromNullInt64(row.RetryInterval),
			MaxDirEntries: fromNullInt64(row.MaxDirEntries),
			PreScript:     row.PreScript,
			PostScript:    row.PostScript,
			IncludeXattr:  fromNullInt64ToBool(row.IncludeXattr),
			LegacyXattr:   fromNullInt64ToBool(row.LegacyXattr),
		}

		backup.Exclusions = exclusionsByJob[row.ID]
		if backup.Exclusions == nil {
			backup.Exclusions = make([]Exclusion, 0)
		}

		backup.Target.populateInfo()

		pathSlice := make([]string, len(backup.Exclusions))
		for k, exclusion := range backup.Exclusions {
			pathSlice[k] = exclusion.Path
		}
		backup.RawExclusions = strings.Join(pathSlice, "\n")

		database.populateBackupExtras(&backup)
		backups[i] = backup
	}

	return backups, nil
}

func (database *Database) GetAllQueuedBackups() ([]Backup, error) {
	rows, err := database.readQueries.ListQueuedBackups(database.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllQueuedBackups: error querying backups: %w", err)
	}

	allExclusions, err := database.readQueries.ListAllBackupExclusions(database.ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("GetAllQueuedBackups: error querying exclusions: %w", err)
	}

	exclusionsByJob := make(map[string][]Exclusion)
	for _, excl := range allExclusions {
		exclusionsByJob[fromNullString(excl.JobID)] = append(exclusionsByJob[fromNullString(excl.JobID)], Exclusion{
			JobId: fromNullString(excl.JobID),
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
				Name:            row.Target,
				AgentHost:       AgentHost{},
				MountScript:     row.MountScript.String,
				VolumeUsedBytes: int(row.VolumeUsedBytes.Int64),
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
			},
			Retry:         fromNullInt64(row.Retry),
			RetryInterval: fromNullInt64(row.RetryInterval),
			MaxDirEntries: fromNullInt64(row.MaxDirEntries),
			PreScript:     row.PreScript,
			PostScript:    row.PostScript,
			IncludeXattr:  fromNullInt64ToBool(row.IncludeXattr),
			LegacyXattr:   fromNullInt64ToBool(row.LegacyXattr),
		}

		backup.Exclusions = exclusionsByJob[row.ID]
		if backup.Exclusions == nil {
			backup.Exclusions = make([]Exclusion, 0)
		}

		pathSlice := make([]string, len(backup.Exclusions))
		for k, exclusion := range backup.Exclusions {
			pathSlice[k] = exclusion.Path
		}
		backup.RawExclusions = strings.Join(pathSlice, "\n")

		database.populateBackupExtras(&backup)
		backups[i] = backup
	}

	return backups, nil
}

func (database *Database) DeleteBackup(tx *Transaction, id string) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
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
	q = database.queries.WithTx(tx.Tx)

	err = q.DeleteBackupExclusions(database.ctx, toNullString(id))
	if err != nil {
		syslog.L.Error(fmt.Errorf("DeleteBackup: error deleting exclusions: %w", err)).
			WithField("id", id).
			Write()
		return fmt.Errorf("DeleteBackup: error deleting exclusions: %w", err)
	}

	rowsAffected, err := q.DeleteBackup(database.ctx, id)
	if err != nil {
		return fmt.Errorf("DeleteBackup: error deleting backup %s: %w", id, err)
	}

	if rowsAffected == 0 {
		return ErrBackupNotFound
	}

	backupLogsPath := filepath.Join(constants.BackupLogsBasePath, id)
	if err := os.RemoveAll(backupLogsPath); err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Error(fmt.Errorf("DeleteBackup: failed removing backup logs: %w", err)).
				WithField("id", id).
				Write()
		}
	}

	if err := deleteBackupSchedule(database.ctx, id); err != nil {
		syslog.L.Error(fmt.Errorf("DeleteBackup: failed deleting schedule: %w", err)).
			WithField("id", id).
			Write()
	}

	commitNeeded = true
	return nil
}

func (b *Backup) GetStreamID() string {
	if b.Target.Type == TargetTypeLocal {
		return ""
	}

	if b.Target.Type == TargetTypeS3 {
		return b.Target.S3Info.Endpoint + "|" + b.ID
	}

	return b.Target.AgentHost.Name + "|" + b.ID
}

func (b *Backup) GetAllUPIDs() []string {
	backupLogsPath := filepath.Join(constants.BackupLogsBasePath, b.ID)
	if err := os.MkdirAll(backupLogsPath, 0755); err != nil {
		syslog.L.Error(fmt.Errorf("GetAllUPIDs: failed to get log dir: %w", err)).
			WithField("id", b.ID).
			Write()
		return nil
	}

	logs, err := os.ReadDir(backupLogsPath)
	if err != nil {
		syslog.L.Error(fmt.Errorf("GetAllUPIDs: failed to read dir: %w", err)).
			WithField("id", b.ID).
			Write()
		return nil
	}

	upids := make([]string, 0, len(logs))

	for _, log := range logs {
		upids = append(upids, log.Name())
	}

	return upids
}
