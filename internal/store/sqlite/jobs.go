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

	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/system"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
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
		var count int
		err := database.readDb.
			QueryRow("SELECT COUNT(*) FROM jobs WHERE id = ?", newID).
			Scan(&count)
		if err != nil {
			return "", fmt.Errorf(
				"generateUniqueJobID: error querying job: %w", err)
		}
		if count == 0 {
			return newID, nil
		}
	}
	return "", fmt.Errorf("failed to generate a unique job ID after %d attempts",
		maxAttempts)
}

// CreateJob creates a new job record and adds any associated exclusions.
func (database *Database) CreateJob(tx *sql.Tx, job types.Job) error {
	if tx == nil {
		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
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

	// Ensure retry parameters are sane.
	if job.RetryInterval <= 0 {
		job.RetryInterval = 1
	}
	if job.Retry < 0 {
		job.Retry = 0
	}

	// Insert the job.
	_, err := tx.Exec(`
        INSERT INTO jobs (
            id, store, mode, source_mode, target, subpath, schedule, comment,
            notification_mode, namespace, current_pid, last_run_upid, last_successful_upid, retry,
            retry_interval, raw_exclusions
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, job.ID, job.Store, job.Mode, job.SourceMode, job.Target, job.Subpath,
		job.Schedule, job.Comment, job.NotificationMode, job.Namespace, job.CurrentPID,
		job.LastRunUpid, job.LastSuccessfulUpid, job.Retry, job.RetryInterval, job.RawExclusions)
	if err != nil {
		return fmt.Errorf("CreateJob: error inserting job: %w", err)
	}

	// Handle any job-specific exclusions.
	for _, exclusion := range job.Exclusions {
		if exclusion.JobID == "" {
			exclusion.JobID = job.ID
		}
		if err := database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(err).WithField("id", job.ID).Write()
			continue
		}
	}

	if err := system.SetSchedule(job); err != nil {
		syslog.L.Error(err).WithField("id", job.ID).Write()
	}

	return nil
}

// GetJob retrieves a job by id and assembles its exclusions.
func (database *Database) GetJob(id string) (types.Job, error) {
	row := database.readDb.QueryRow(`
        SELECT id, store, mode, source_mode, target, subpath, schedule, comment,
               notification_mode, namespace, current_pid, last_run_upid, last_successful_upid,
							 retry, retry_interval, raw_exclusions
        FROM jobs WHERE id = ?
    `, id)

	var job types.Job
	err := row.Scan(&job.ID, &job.Store, &job.Mode, &job.SourceMode,
		&job.Target, &job.Subpath, &job.Schedule, &job.Comment,
		&job.NotificationMode, &job.Namespace, &job.CurrentPID, &job.LastRunUpid,
		&job.LastSuccessfulUpid, &job.Retry, &job.RetryInterval, &job.RawExclusions)
	if err != nil {
		return types.Job{}, fmt.Errorf("GetJob: error fetching job: %w", err)
	}

	// Retrieve and attach exclusions.
	exclusions, err := database.GetAllJobExclusions(id)
	if err == nil && exclusions != nil {
		job.Exclusions = exclusions
		pathSlice := []string{}
		for _, exclusion := range exclusions {
			pathSlice = append(pathSlice, exclusion.Path)
		}
		job.RawExclusions = strings.Join(pathSlice, "\n")
	}

	jobLogsPath := filepath.Join(constants.JobLogsBasePath, job.ID)
	upids, err := os.ReadDir(jobLogsPath)
	if err == nil {
		job.UPIDs = make([]string, len(upids))
		for i, upid := range upids {
			job.UPIDs[i] = upid.Name()
		}
	}

	if job.LastRunUpid != "" {
		task, err := proxmox.Session.GetTaskByUPID(job.LastRunUpid)
		if err == nil {
			job.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				job.LastRunState = task.ExitStatus
				job.Duration = task.EndTime - task.StartTime
			} else {
				job.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if job.LastSuccessfulUpid != "" {
		if successTask, err := proxmox.Session.GetTaskByUPID(job.LastSuccessfulUpid); err == nil {
			job.LastSuccessfulEndtime = successTask.EndTime
		}
	}

	// Get global exclusions
	globalExclusions, err := database.GetAllGlobalExclusions()
	if err == nil && globalExclusions != nil {
		job.Exclusions = append(job.Exclusions, globalExclusions...)
	}

	if nextSchedule, err := system.GetNextSchedule(job); err == nil && nextSchedule != nil {
		job.NextRun = nextSchedule.Unix()
	}
	return job, nil
}

// UpdateJob updates an existing job and its exclusions.
func (database *Database) UpdateJob(tx *sql.Tx, job types.Job) error {
	if tx == nil {
		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
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

	_, err := tx.Exec(`
        UPDATE jobs SET store = ?, mode = ?, source_mode = ?, target = ?,
            subpath = ?, schedule = ?, comment = ?, notification_mode = ?,
            namespace = ?, current_pid = ?, last_run_upid = ?, retry = ?,
            retry_interval = ?, raw_exclusions = ?, last_successful_upid = ?
        WHERE id = ?
    `, job.Store, job.Mode, job.SourceMode, job.Target, job.Subpath,
		job.Schedule, job.Comment, job.NotificationMode, job.Namespace,
		job.CurrentPID, job.LastRunUpid, job.Retry, job.RetryInterval,
		job.RawExclusions, job.LastSuccessfulUpid, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error updating job: %w", err)
	}

	// Remove old exclusions and insert updated ones.
	if _, err := tx.Exec(`
        DELETE FROM exclusions WHERE job_id = ?
    `, job.ID); err != nil {
		return fmt.Errorf("UpdateJob: error removing old exclusions: %w", err)
	}

	for _, exclusion := range job.Exclusions {
		// Only update those belonging to the job.
		if exclusion.JobID != job.ID {
			continue
		}
		if err := database.CreateExclusion(tx, exclusion); err != nil {
			syslog.L.Error(err).WithField("id", job.ID).Write()
			continue
		}
	}

	if err := system.SetSchedule(job); err != nil {
		syslog.L.Error(err).WithField("id", job.ID).Write()
	}

	if job.LastRunUpid != "" {
		jobLogsPath := filepath.Join(constants.JobLogsBasePath, job.ID)
		if err := os.MkdirAll(jobLogsPath, 0755); err != nil {
			syslog.L.Error(err).WithField("id", job.ID).Write()
		} else {
			jobLogPath := filepath.Join(jobLogsPath, job.LastRunUpid)
			if _, err := os.Lstat(jobLogPath); err != nil {
				origLogPath, err := proxmox.GetLogPath(job.LastRunUpid)
				if err != nil {
					syslog.L.Error(err).WithField("id", job.ID).Write()
				}
				err = os.Symlink(origLogPath, jobLogPath)
				if err != nil {
					syslog.L.Error(err).WithField("id", job.ID).Write()
				}
			}
		}
	}

	return nil
}

// GetAllJobs returns all job records.
func (database *Database) GetAllJobs() ([]types.Job, error) {
	rows, err := database.readDb.Query("SELECT id FROM jobs")
	if err != nil {
		return nil, fmt.Errorf("GetAllJobs: error querying jobs: %w", err)
	}
	defer rows.Close()

	var jobs []types.Job
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		job, err := database.GetJob(id)
		if err != nil {
			syslog.L.Error(err).WithField("id", id).Write()
			continue
		}
		if target, err := database.GetTarget(job.Target); err == nil {
			job.ExpectedSize = utils.HumanReadableBytes(int64(target.DriveUsedBytes))
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// DeleteJob deletes a job and any related exclusions.
func (database *Database) DeleteJob(tx *sql.Tx, id string) error {
	if tx == nil {
		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
	}

	_, err := tx.Exec("DELETE FROM jobs WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("DeleteJob: error deleting job: %w", err)
	}

	// Delete associated exclusions.
	if _, err := tx.Exec("DELETE FROM exclusions WHERE job_id = ?", id); err != nil {
		syslog.L.Error(err).WithField("id", id).Write()
	}

	jobLogsPath := filepath.Join(constants.JobLogsBasePath, id)
	if err := os.RemoveAll(jobLogsPath); err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Error(err).WithField("id", id).Write()
		}
	}

	if err := system.DeleteSchedule(id); err != nil {
		syslog.L.Error(err).WithField("id", id).Write()
	}

	return nil
}
