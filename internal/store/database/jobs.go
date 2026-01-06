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

// generateUniqueJobID produces a unique job id based on the job’s target.
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
			return "", fmt.Errorf(
				"generateUniqueJobID: error checking job existence: %w", err)
		}
	}
	return "", fmt.Errorf("failed to generate a unique job ID after %d attempts",
		maxAttempts)
}

// CreateJob creates a new job record and adds any associated exclusions.
func (database *Database) CreateJob(tx *sql.Tx, job types.Job) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback() // Rollback on panic
				panic(p)          // Re-panic after rollback
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateJob: failed to commit transaction: %w", cErr) // Assign commit error back
					syslog.L.Error(err).Write()
				}
			} else {
				// Rollback if commit isn't explicitly needed (e.g., early return without error)
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
	if strings.TrimSpace(job.ReadMode) == "" {
		job.ReadMode = "standard"
	}
	if strings.TrimSpace(job.SourceMode) == "" {
		job.SourceMode = "snapshot"
	}

	_, err = tx.Exec(`
        INSERT INTO jobs (
            id, store, mode, source_mode, read_mode, target, subpath, schedule, comment,
            notification_mode, namespace, current_pid, last_run_upid, last_successful_upid, retry,
            retry_interval, max_dir_entries, pre_script, post_script, include_xattr
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, job.ID, job.Store, job.Mode, job.SourceMode, job.ReadMode, job.Target, job.Subpath,
		job.Schedule, job.Comment, job.NotificationMode, job.Namespace, job.CurrentPID,
		job.LastRunUpid, job.LastSuccessfulUpid, job.Retry, job.RetryInterval,
		job.MaxDirEntries, job.PreScript, job.PostScript, job.IncludeXattr)
	if err != nil {
		return fmt.Errorf("CreateJob: error inserting job: %w", err)
	}

	for _, exclusion := range job.Exclusions {
		if exclusion.JobID == "" {
			exclusion.JobID = job.ID
		}
		// Assuming CreateExclusion uses the provided tx
		if err = database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(fmt.Errorf("CreateJob: failed to create exclusion: %w", err)).
				WithField("job_id", job.ID).
				WithField("path", exclusion.Path).
				Write()
			// Return the first error encountered during exclusion creation
			return fmt.Errorf("CreateJob: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = system.SetSchedule(database.ctx, job); err != nil {
		syslog.L.Error(fmt.Errorf("CreateJob: failed to set schedule: %w", err)).
			WithField("id", job.ID).
			Write()
		// Decide if schedule setting failure should rollback the DB transaction
		// return fmt.Errorf("CreateJob: failed to set schedule: %w", err) // Uncomment to rollback
	}

	commitNeeded = true
	return nil
}

// GetJob retrieves a job by id and assembles its exclusions.
func (database *Database) GetJob(id string) (types.Job, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            t.drive_used_bytes, t.mount_script, j.include_xattr
        FROM jobs j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
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
		var driveUsedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&job.ID, &job.Store, &job.Mode, &job.SourceMode, &job.ReadMode,
			&job.Target, &job.Subpath, &job.Schedule, &job.Comment,
			&job.NotificationMode, &job.Namespace, &job.CurrentPID, &job.LastRunUpid,
			&job.LastSuccessfulUpid, &job.Retry, &job.RetryInterval, &job.MaxDirEntries,
			&job.PreScript, &job.PostScript, &exclusionPath, &driveUsedBytes,
			&mountScript, &job.IncludeXattr,
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
			// Optionally, handle the NULL case, e.g., set to an empty string
			job.TargetMountScript = ""
		}

		if exclusionPath.Valid && exclusionPath.String != "" {
			exclusionPaths = append(exclusionPaths, exclusionPath.String)
		}
		// Only set ExpectedSize once from the first row (or any row) where it's valid
		if driveUsedBytes.Valid && job.ExpectedSize == 0 {
			job.ExpectedSize = int(driveUsedBytes.Int64)
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

// populateJobExtras fills in details not directly from the database tables.
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

	if nextSchedule, err := system.GetNextSchedule(database.ctx, *job); err == nil && nextSchedule != nil {
		job.NextRun = nextSchedule.Unix()
	}
}

// UpdateJob updates an existing job and its exclusions.
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
	if strings.TrimSpace(job.ReadMode) == "" {
		job.ReadMode = "standard"
	}
	if strings.TrimSpace(job.SourceMode) == "" {
		job.SourceMode = "snapshot"
	}

	_, err = tx.Exec(`
        UPDATE jobs SET store = ?, mode = ?, source_mode = ?, read_mode = ?, target = ?,
            subpath = ?, schedule = ?, comment = ?, notification_mode = ?,
            namespace = ?, current_pid = ?, last_run_upid = ?, retry = ?,
            retry_interval = ?, last_successful_upid = ?, pre_script = ?, post_script = ?,
            max_dir_entries = ?, include_xattr = ?
        WHERE id = ?
    `, job.Store, job.Mode, job.SourceMode, job.ReadMode, job.Target, job.Subpath,
		job.Schedule, job.Comment, job.NotificationMode, job.Namespace,
		job.CurrentPID, job.LastRunUpid, job.Retry, job.RetryInterval,
		job.LastSuccessfulUpid, job.PreScript, job.PostScript, job.MaxDirEntries,
		job.IncludeXattr, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error updating job: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM exclusions WHERE job_id = ?`, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error removing old exclusions for job %s: %w", job.ID, err)
	}

	for _, exclusion := range job.Exclusions {
		exclusion.JobID = job.ID // Ensure correct JobID
		if err = database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(fmt.Errorf("UpdateJob: failed to create exclusion: %w", err)).
				WithField("job_id", job.ID).
				WithField("path", exclusion.Path).
				Write()
			return fmt.Errorf("UpdateJob: failed to create exclusion '%s': %w", exclusion.Path, err)
		}
	}

	if err = system.SetSchedule(database.ctx, job); err != nil {
		syslog.L.Error(fmt.Errorf("UpdateJob: failed to set schedule: %w", err)).
			WithField("id", job.ID).
			Write()
		// Decide if schedule setting failure should rollback the DB transaction
		// return fmt.Errorf("UpdateJob: failed to set schedule: %w", err) // Uncomment to rollback
	}

	if job.LastRunUpid != "" {
		go database.linkJobLog(job.ID, job.LastRunUpid)
	}

	commitNeeded = true
	return nil
}

// linkJobLog handles the asynchronous log linking.
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

// GetAllJobs returns all job records.
func (database *Database) GetAllJobs() ([]types.Job, error) {
	query := `
        SELECT
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            t.drive_used_bytes, t.mount_script, t.path, j.include_xattr
        FROM jobs j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
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
		var jobID, store, mode, sourceMode, readMode, target, subpath, schedule, comment, notificationMode, namespace, lastRunUpid, lastSuccessfulUpid string
		var preScript, postScript string
		var retry, maxDirEntries int
		var retryInterval int
		var currentPID int
		var includeXAttr bool
		var targetPath, exclusionPath sql.NullString
		var driveUsedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&jobID, &store, &mode, &sourceMode, &readMode, &target, &subpath, &schedule, &comment,
			&notificationMode, &namespace, &currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval, &maxDirEntries, &preScript, &postScript,
			&exclusionPath, &driveUsedBytes, &mountScript, &targetPath, &includeXAttr,
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
				job.ExpectedSize = int(driveUsedBytes.Int64)
			}
			jobsMap[jobID] = job
			jobOrder = append(jobOrder, jobID)
			database.populateJobExtras(job) // Populate non-SQL extras once per job
		}

		if targetPath.Valid {
			job.TargetPath = targetPath.String
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
            j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, j.schedule, j.comment,
            j.notification_mode, j.namespace, j.current_pid, j.last_run_upid, j.last_successful_upid,
            j.retry, j.retry_interval, j.max_dir_entries, j.pre_script, j.post_script,
            e.path,
            t.drive_used_bytes, t.mount_script, j.include_xattr
        FROM jobs j
        LEFT JOIN exclusions e ON j.id = e.job_id
        LEFT JOIN targets t ON j.target = t.name
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
		var jobID, store, mode, sourceMode, readMode, target, subpath, schedule, comment, notificationMode, namespace, lastRunUpid, lastSuccessfulUpid string
		var preScript, postScript string
		var retry, maxDirEntries int
		var retryInterval int
		var currentPID int
		var includeXAttr bool
		var exclusionPath sql.NullString
		var driveUsedBytes sql.NullInt64
		var mountScript sql.NullString

		err := rows.Scan(
			&jobID, &store, &mode, &sourceMode, &readMode, &target, &subpath, &schedule, &comment,
			&notificationMode, &namespace, &currentPID, &lastRunUpid, &lastSuccessfulUpid,
			&retry, &retryInterval, &maxDirEntries, &preScript, &postScript,
			&exclusionPath, &driveUsedBytes, &mountScript, &includeXAttr,
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
				job.ExpectedSize = int(driveUsedBytes.Int64)
			}
			jobsMap[jobID] = job
			jobOrder = append(jobOrder, jobID)
			database.populateJobExtras(job) // Populate non-SQL extras once per job
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

// DeleteJob deletes a job and any related exclusions.
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

	// Delete associated exclusions first (or rely on ON DELETE CASCADE)
	_, err = tx.Exec("DELETE FROM exclusions WHERE job_id = ?", id)
	if err != nil {
		syslog.L.Error(fmt.Errorf("DeleteJob: error deleting exclusions: %w", err)).
			WithField("id", id).
			Write()
		// Return error if exclusions deletion fails
		return fmt.Errorf("DeleteJob: error deleting exclusions for job %s: %w", id, err)
	}

	// Delete the job itself
	res, err := tx.Exec("DELETE FROM jobs WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("DeleteJob: error deleting job %s: %w", id, err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrJobNotFound
	}

	// Filesystem and system operations outside transaction
	jobLogsPath := filepath.Join(constants.JobLogsBasePath, id)
	if err := os.RemoveAll(jobLogsPath); err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Error(fmt.Errorf("DeleteJob: failed removing job logs: %w", err)).
				WithField("id", id).
				Write()
			// Decide if this failure should prevent commit (if tx was passed in)
			// or just be logged. Currently just logged.
		}
	}

	if err := system.DeleteSchedule(database.ctx, id); err != nil {
		syslog.L.Error(fmt.Errorf("DeleteJob: failed deleting schedule: %w", err)).
			WithField("id", id).
			Write()
		// Decide if this failure should prevent commit or just be logged.
	}

	commitNeeded = true
	return nil
}
