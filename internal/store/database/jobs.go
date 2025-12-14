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

func (database *Database) generateUniqueJobID(job types.Job) (string, error) {
	baseID := utils.Slugify(job.Target)
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
			QueryRow("SELECT 1 FROM jobs WHERE id = ? LIMIT 1", newID).
			Scan(&exists)

		if errors.Is(err, sql.ErrNoRows) {
			return newID, nil
		}
		if err != nil {
			return "", fmt.Errorf("generateUniqueJobID: error checking job existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique job ID after %d attempts", maxAttempts)
}

func (database *Database) CreateJob(tx *sql.Tx, job types.Job) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateJob: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if job.ID == "" {
		id, err := database.generateUniqueJobID(job)
		if err != nil {
			return fmt.Errorf("CreateJob: failed to generate unique id -> %w", err)
		}
		job.ID = id
	}

	if job.Target == "" {
		return errors.New("target is empty")
	}
	if job.Store == "" {
		return errors.New("datastore is empty")
	}
	if !utils.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("CreateJob: invalid id string -> %s", job.ID)
	}
	if !utils.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", job.Namespace)
	}
	if err := utils.ValidateOnCalendar(job.Schedule); err != nil && job.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", job.Schedule)
	}
	if !utils.IsValidPathString(job.Subpath) {
		return fmt.Errorf("invalid subpath string: %s", job.Subpath)
	}
	if job.RetryInterval <= 0 {
		job.RetryInterval = 1
	}
	if job.Retry < 0 {
		job.Retry = 0
	}
	if job.MaxDirEntries <= 0 {
		job.MaxDirEntries = 1048576
	}

	_, err = tx.Exec(`
        INSERT INTO jobs (
            id, store, mode, source_mode, read_mode, target, volume_name, subpath, schedule, comment,
            notification_mode, namespace, current_pid, last_run_upid, last_successful_upid, retry,
            retry_interval, max_dir_entries, pre_script, post_script
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, job.ID, job.Store, job.Mode, job.SourceMode, job.ReadMode, job.Target, job.VolumeName, job.Subpath,
		job.Schedule, job.Comment, job.NotificationMode, job.Namespace, job.CurrentPID,
		job.LastRunUpid, job.LastSuccessfulUpid, job.Retry, job.RetryInterval,
		job.MaxDirEntries, job.PreScript, job.PostScript)
	if err != nil {
		return fmt.Errorf("CreateJob: error inserting job: %w", err)
	}

	for _, exclusion := range job.Exclusions {
		if exclusion.JobID == "" {
			exclusion.JobID = job.ID
		}
		if err = database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(fmt.Errorf("CreateJob: failed to create exclusion: %w", err)).
				WithField("job_id", job.ID).
				WithField("path", exclusion.Path).
				Write()
			return fmt.Errorf("CreateJob: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = system.SetSchedule(job); err != nil {
		syslog.L.Error(fmt.Errorf("CreateJob: failed to set schedule: %w", err)).
			WithField("id", job.ID).
			Write()
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetJob(id string) (types.Job, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.volume_name, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            v.meta_used_bytes, t.mount_script,
            t.target_type
        FROM jobs j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
        LEFT JOIN volumes v ON v.target_name = j.target AND (j.volume_name IS NULL OR v.volume_name = j.volume_name)
        WHERE j.id = ?
    `
	rows, err := database.readDb.Query(query, id)
	if err != nil {
		return types.Job{}, fmt.Errorf("GetJob: error querying job data: %w", err)
	}
	defer rows.Close()

	var job types.Job
	var exclusionPaths []string
	var found bool = false

	for rows.Next() {
		found = true
		var exclusionPath sql.NullString
		var usedBytes sql.NullInt64
		var mountScript sql.NullString
		var targetType sql.NullString

		err := rows.Scan(
			&job.ID, &job.Store, &job.Mode, &job.SourceMode, &job.ReadMode,
			&job.Target, &job.VolumeName, &job.Subpath, &job.Schedule, &job.Comment,
			&job.NotificationMode, &job.Namespace, &job.CurrentPID, &job.LastRunUpid,
			&job.LastSuccessfulUpid, &job.Retry, &job.RetryInterval, &job.MaxDirEntries,
			&job.PreScript, &job.PostScript, &exclusionPath, &usedBytes, &mountScript, &targetType,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetJob: error scanning job data: %w", err)).
				WithField("id", id).
				Write()
			return types.Job{}, fmt.Errorf("GetJob: error scanning job data for id %s: %w", id, err)
		}

		if mountScript.Valid {
			job.TargetMountScript = mountScript.String
		} else {
			job.TargetMountScript = ""
		}
		if targetType.Valid {
			job.TargetType = targetType.String
		}

		if exclusionPath.Valid && exclusionPath.String != "" {
			exclusionPaths = append(exclusionPaths, exclusionPath.String)
		}
		if usedBytes.Valid && job.ExpectedSize == 0 {
			job.ExpectedSize = int(usedBytes.Int64)
		}
	}

	if err = rows.Err(); err != nil {
		return types.Job{}, fmt.Errorf("GetJob: error iterating job results: %w", err)
	}

	if !found {
		return types.Job{}, ErrJobNotFound
	}

	job.Exclusions = make([]types.Exclusion, 0, len(exclusionPaths))
	for _, path := range exclusionPaths {
		job.Exclusions = append(job.Exclusions, types.Exclusion{
			JobID: job.ID,
			Path:  path,
		})
	}
	job.RawExclusions = strings.Join(exclusionPaths, "\n")

	database.populateJobExtras(&job)

	return job, nil
}

func (database *Database) populateJobExtras(job *types.Job) {
	if job.LastRunUpid != "" {
		task, err := proxmox.GetTaskByUPID(job.LastRunUpid)
		if err == nil {
			job.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				job.LastRunState = task.ExitStatus
				job.Duration = task.EndTime - task.StartTime
			} else if task.StartTime > 0 {
				job.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if job.LastSuccessfulUpid != "" {
		if successTask, err := proxmox.GetTaskByUPID(job.LastSuccessfulUpid); err == nil {
			job.LastSuccessfulEndtime = successTask.EndTime
		}
	}

	if nextSchedule, err := system.GetNextSchedule(*job); err == nil && nextSchedule != nil {
		job.NextRun = nextSchedule.Unix()
	}
}

func (database *Database) UpdateJob(tx *sql.Tx, job types.Job) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateJob: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if !utils.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("UpdateJob: invalid id string -> %s", job.ID)
	}
	if job.Target == "" {
		return errors.New("target is empty")
	}
	if job.Store == "" {
		return errors.New("datastore is empty")
	}
	if job.RetryInterval <= 0 {
		job.RetryInterval = 1
	}
	if job.Retry < 0 {
		job.Retry = 0
	}
	if !utils.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", job.Namespace)
	}
	if err := utils.ValidateOnCalendar(job.Schedule); err != nil && job.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", job.Schedule)
	}
	if !utils.IsValidPathString(job.Subpath) {
		return fmt.Errorf("invalid subpath string: %s", job.Subpath)
	}

	_, err = tx.Exec(`
        UPDATE jobs SET store = ?, mode = ?, source_mode = ?, read_mode = ?, target = ?, volume_name = ?,
            subpath = ?, schedule = ?, comment = ?, notification_mode = ?,
            namespace = ?, current_pid = ?, last_run_upid = ?, retry = ?,
            retry_interval = ?, last_successful_upid = ?, pre_script = ?, post_script = ?,
            max_dir_entries = ?
        WHERE id = ?
    `, job.Store, job.Mode, job.SourceMode, job.ReadMode, job.Target, job.VolumeName, job.Subpath,
		job.Schedule, job.Comment, job.NotificationMode, job.Namespace,
		job.CurrentPID, job.LastRunUpid, job.Retry, job.RetryInterval,
		job.LastSuccessfulUpid, job.PreScript, job.PostScript, job.MaxDirEntries, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error updating job: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM exclusions WHERE job_id = ?`, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error removing old exclusions for job %s: %w", job.ID, err)
	}

	for _, exclusion := range job.Exclusions {
		exclusion.JobID = job.ID
		if err = database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(fmt.Errorf("UpdateJob: failed to create exclusion: %w", err)).
				WithField("job_id", job.ID).
				WithField("path", exclusion.Path).
				Write()
			return fmt.Errorf("UpdateJob: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = system.SetSchedule(job); err != nil {
		syslog.L.Error(fmt.Errorf("UpdateJob: failed to set schedule: %w", err)).
			WithField("id", job.ID).
			Write()
	}

	if job.LastRunUpid != "" {
		go database.linkJobLog(job.ID, job.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

func (database *Database) linkJobLog(jobID, upid string) {
	jobLogsPath := filepath.Join(constants.JobLogsBasePath, jobID)
	if err := os.MkdirAll(jobLogsPath, 0755); err != nil {
		syslog.L.Error(fmt.Errorf("linkJobLog: failed to create log dir: %w", err)).
			WithField("id", jobID).
			Write()
		return
	}

	jobLogPath := filepath.Join(jobLogsPath, upid)
	if _, err := os.Lstat(jobLogPath); err != nil && !os.IsNotExist(err) {
		syslog.L.Error(fmt.Errorf("linkJobLog: failed to stat potential symlink: %w", err)).
			WithField("path", jobLogPath).
			Write()
		return
	}

	origLogPath, err := proxmox.GetLogPath(upid)
	if err != nil {
		syslog.L.Error(fmt.Errorf("linkJobLog: failed to get original log path: %w", err)).
			WithField("id", jobID).
			WithField("upid", upid).
			Write()
		return
	}

	if _, err := os.Stat(origLogPath); err != nil {
		syslog.L.Error(fmt.Errorf("linkJobLog: original log path does not exist or error stating: %w", err)).
			WithField("orig_path", origLogPath).
			WithField("id", jobID).
			Write()
		return
	}

	_ = os.Remove(jobLogPath)

	err = os.Symlink(origLogPath, jobLogPath)
	if err != nil {
		syslog.L.Error(fmt.Errorf("linkJobLog: failed to create symlink: %w", err)).
			WithField("id", jobID).
			WithField("source", origLogPath).
			WithField("link", jobLogPath).
			Write()
	}
}

func (database *Database) GetAllJobs() ([]types.Job, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.volume_name, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            v.meta_used_bytes, t.mount_script,
            t.target_type
        FROM jobs j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
        LEFT JOIN volumes v ON v.target_name = j.target AND (j.volume_name IS NULL OR v.volume_name = j.volume_name)
        ORDER BY j.id
    `
	rows, err := database.readDb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllJobs: error querying jobs: %w", err)
	}
	defer rows.Close()

	jobsMap := make(map[string]*types.Job)
	var jobOrder []string

	for rows.Next() {
		var jobID, store, mode, sourceMode, readMode, target, volumeName, subpath, schedule, comment, notificationMode, namespace, lastRunUpid, lastSuccessfulUpid string
		var preScript, postScript string
		var retry, maxDirEntries int
		var retryInterval int
		var currentPID int
		var targetType, exclusionPath sql.NullString
		var usedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&jobID, &store, &mode, &sourceMode, &readMode, &target, &volumeName, &subpath, &schedule, &comment,
			&notificationMode, &namespace, &currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval, &maxDirEntries, &preScript, &postScript,
			&exclusionPath, &usedBytes, &mountScript, &targetType,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllJobs: error scanning row: %w", err)).Write()
			continue
		}

		job, exists := jobsMap[jobID]
		if !exists {
			job = &types.Job{
				ID:                 jobID,
				Store:              store,
				Mode:               mode,
				SourceMode:         sourceMode,
				ReadMode:           readMode,
				Target:             target,
				VolumeName:         volumeName,
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
			}
			if usedBytes.Valid {
				job.ExpectedSize = int(usedBytes.Int64)
			}
			jobsMap[jobID] = job
			jobOrder = append(jobOrder, jobID)
			database.populateJobExtras(job)
		}

		if targetType.Valid {
			job.TargetType = targetType.String
		}
		if mountScript.Valid {
			job.TargetMountScript = mountScript.String
		} else {
			job.TargetMountScript = ""
		}
		if exclusionPath.Valid && exclusionPath.String != "" {
			job.Exclusions = append(job.Exclusions, types.Exclusion{
				JobID: jobID,
				Path:  exclusionPath.String,
			})
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllJobs: error iterating job results: %w", err)
	}

	jobs := make([]types.Job, len(jobOrder))
	for i, jobID := range jobOrder {
		job := jobsMap[jobID]
		pathSlice := make([]string, len(job.Exclusions))
		for k, exclusion := range job.Exclusions {
			pathSlice[k] = exclusion.Path
		}
		job.RawExclusions = strings.Join(pathSlice, "\n")
		jobs[i] = *job
	}

	return jobs, nil
}

func (database *Database) GetAllQueuedJobs() ([]types.Job, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.volume_name, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            v.meta_used_bytes, t.mount_script
        FROM jobs j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
        LEFT JOIN volumes v ON v.target_name = j.target AND (j.volume_name IS NULL OR v.volume_name = j.volume_name)
        WHERE j.last_run_upid LIKE "%pbsplusgen-queue%"
        ORDER BY j.id
    `
	rows, err := database.readDb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllQueuedJobs: error querying jobs: %w", err)
	}
	defer rows.Close()

	jobsMap := make(map[string]*types.Job)
	var jobOrder []string

	for rows.Next() {
		var jobID, store, mode, sourceMode, readMode, target, volumeName, subpath, schedule, comment, notificationMode, namespace, lastRunUpid, lastSuccessfulUpid string
		var preScript, postScript string
		var retry, maxDirEntries int
		var retryInterval int
		var currentPID int
		var exclusionPath sql.NullString
		var usedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&jobID, &store, &mode, &sourceMode, &readMode, &target, &volumeName, &subpath, &schedule, &comment,
			&notificationMode, &namespace, &currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval, &maxDirEntries, &preScript, &postScript,
			&exclusionPath, &usedBytes, &mountScript,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllQueuedJobs: error scanning row: %w", err)).Write()
			continue
		}

		job, exists := jobsMap[jobID]
		if !exists {
			job = &types.Job{
				ID:                 jobID,
				Store:              store,
				Mode:               mode,
				SourceMode:         sourceMode,
				ReadMode:           readMode,
				Target:             target,
				VolumeName:         volumeName,
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
			}
			if usedBytes.Valid {
				job.ExpectedSize = int(usedBytes.Int64)
			}
			jobsMap[jobID] = job
			jobOrder = append(jobOrder, jobID)
			database.populateJobExtras(job)
		}

		if mountScript.Valid {
			job.TargetMountScript = mountScript.String
		} else {
			job.TargetMountScript = ""
		}

		if exclusionPath.Valid && exclusionPath.String != "" {
			job.Exclusions = append(job.Exclusions, types.Exclusion{
				JobID: jobID,
				Path:  exclusionPath.String,
			})
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllQueuedJobs: error iterating job results: %w", err)
	}

	jobs := make([]types.Job, len(jobOrder))
	for i, jobID := range jobOrder {
		job := jobsMap[jobID]
		pathSlice := make([]string, len(job.Exclusions))
		for k, exclusion := range job.Exclusions {
			pathSlice[k] = exclusion.Path
		}
		job.RawExclusions = strings.Join(pathSlice, "\n")
		jobs[i] = *job
	}

	return jobs, nil
}

func (database *Database) DeleteJob(tx *sql.Tx, id string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("DeleteJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteJob: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	_, err = tx.Exec("DELETE FROM exclusions WHERE job_id = ?", id)
	if err != nil {
		syslog.L.Error(fmt.Errorf("DeleteJob: error deleting exclusions: %w", err)).
			WithField("id", id).
			Write()
		return fmt.Errorf("DeleteJob: error deleting exclusions for job %s: %w", id, err)
	}

	res, err := tx.Exec("DELETE FROM jobs WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("DeleteJob: error deleting job %s: %w", id, err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrJobNotFound
	}

	jobLogsPath := filepath.Join(constants.JobLogsBasePath, id)
	if err := os.RemoveAll(jobLogsPath); err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Error(fmt.Errorf("DeleteJob: failed removing job logs: %w", err)).
				WithField("id", id).
				Write()
		}
	}

	if err := system.DeleteSchedule(id); err != nil {
		syslog.L.Error(fmt.Errorf("DeleteJob: failed deleting schedule: %w", err)).
			WithField("id", id).
			Write()
	}

	commitNeeded = true
	return nil
}
