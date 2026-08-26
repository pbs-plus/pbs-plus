//go:build linux

package coredb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func (db *Store) CreateVerificationJob(tx *Transaction, job VerificationJob) (err error) {
	var commitNeeded bool
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateVerificationJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateVerificationJob: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateVerificationJob: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateVerificationJob: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if job.ID == "" {
		var baseID string
		if job.BackupJobID != "" {
			baseID = validate.Slugify(job.BackupJobID) + "-verify"
		} else if job.Namespace != "" {
			baseID = validate.Slugify(job.Store+"-"+strings.ReplaceAll(job.Namespace, "/", "-")) + "-verify"
		} else {
			baseID = validate.Slugify(job.Store) + "-verify"
		}
		if baseID == "-verify" {
			return errors.New("invalid backup_job_id: slugified value is empty")
		}
		for idx := range maxAttempts {
			var newID string
			if idx == 0 {
				newID = baseID
			} else {
				newID = fmt.Sprintf("%s-%d", baseID, idx)
			}
			_, err := db.readQueries.VerificationJobExists(db.ctx, newID)
			if errors.Is(err, sql.ErrNoRows) {
				job.ID = newID
				break
			}
			if err != nil {
				return fmt.Errorf("CreateVerificationJob: error checking existence: %w", err)
			}
		}
		if job.ID == "" {
			return fmt.Errorf("failed to generate a unique verification job ID after %d attempts", maxAttempts)
		}
	}

	if job.BackupJobID == "" && job.TargetMode != "namespace" {
		return fmt.Errorf("%w: backup_job_id is required", ErrValidationFailed)
	}
	if job.Store == "" {
		return fmt.Errorf("%w: store is required", ErrValidationFailed)
	}
	if !validate.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("CreateVerificationJob: invalid id string -> %s", job.ID)
	}
	if !validate.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", job.Namespace)
	}
	if err := validate.ValidateOnCalendar(job.Schedule); err != nil && job.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", job.Schedule)
	}
	if job.RetryInterval <= 0 {
		job.RetryInterval = 1
	}
	if job.Retry < 0 {
		job.Retry = 0
	}
	if job.Mode == "" {
		job.Mode = "random_spot"
	}
	if job.SpotConfig.SampleCount <= 0 && job.SpotConfig.SampleCountPercent <= 0 {
		job.SpotConfig.SampleCount = 10
	}

	spotConfigJSON, err := json.Marshal(job.SpotConfig)
	if err != nil {
		return fmt.Errorf("CreateVerificationJob: failed to marshal spot_config: %w", err)
	}

	err = q.CreateVerificationJob(db.ctx, corequery.CreateVerificationJobParams{
		ID:                    job.ID,
		BackupJobID:           job.BackupJobID,
		Store:                 job.Store,
		Namespace:             toNullString(job.Namespace),
		Mode:                  job.Mode,
		Schedule:              toNullString(job.Schedule),
		Comment:               toNullString(job.Comment),
		SpotConfig:            toNullString(string(spotConfigJSON)),
		LastRunUpid:           toNullString(job.History.LastRunUpid),
		LastSuccessfulUpid:    toNullString(job.History.LastSuccessfulUpid),
		LastRunStatus:         toNullInt64(int(job.History.LastRunStatus)),
		RetryCount:            toNullInt64(job.History.RetryCount),
		Retry:                 toNullInt64(job.Retry),
		RetryInterval:         toNullInt64(job.RetryInterval),
		LastRunStarttime:      toNullInt64(int(job.History.LastRunStarttime)),
		LastRunEndtime:        toNullInt64(int(job.History.LastRunEndtime)),
		LastSuccessfulEndtime: toNullInt64(int(job.History.LastSuccessfulEndtime)),
		RunOnBackupComplete:   boolToNullInt64(job.RunOnBackupComplete),
		PendingSince:          toNullInt64(int(job.PendingSince)),
		NotificationMode:      toNullString(job.NotificationMode),
	})
	if err != nil {
		return fmt.Errorf("CreateVerificationJob: error inserting verification job: %w", err)
	}

	targetMode := job.TargetMode
	if targetMode == "" {
		targetMode = "backup_job"
	}
	_, err = tx.ExecContext(db.ctx,
		"UPDATE verification_jobs SET target_mode = ?, recursive = ? WHERE id = ?",
		targetMode, job.Recursive, job.ID)
	if err != nil {
		return fmt.Errorf("CreateVerificationJob: error setting target_mode/recursive: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetVerificationJob(id string) (VerificationJob, error) {
	row, err := db.readQueries.GetVerificationJob(db.ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return VerificationJob{}, ErrNotFound
	}
	if err != nil {
		return VerificationJob{}, fmt.Errorf("GetVerificationJob: error querying: %w", err)
	}

	job := VerificationJob{
		ID:                  row.ID,
		BackupJobID:         row.BackupJobID,
		Store:               row.Store,
		Namespace:           fromNullString(row.Namespace),
		Mode:                row.Mode,
		Schedule:            fromNullString(row.Schedule),
		Comment:             fromNullString(row.Comment),
		NotificationMode:    fromNullString(row.NotificationMode),
		Retry:               fromNullInt64(row.Retry),
		RetryInterval:       fromNullInt64(row.RetryInterval),
		RunOnBackupComplete: fromNullInt64ToBool(row.RunOnBackupComplete),
		PendingSince:        int64(fromNullInt64(row.PendingSince)),
		History: JobHistory{
			LastRunUpid:           fromNullString(row.LastRunUpid),
			LastSuccessfulUpid:    fromNullString(row.LastSuccessfulUpid),
			LastRunStatus:         JobStatus(fromNullInt64(row.LastRunStatus)),
			RetryCount:            fromNullInt64(row.RetryCount),
			LastRunStarttime:      int64(fromNullInt64(row.LastRunStarttime)),
			LastRunEndtime:        int64(fromNullInt64(row.LastRunEndtime)),
			LastSuccessfulEndtime: int64(fromNullInt64(row.LastSuccessfulEndtime)),
		},
		CreatedAt: int64(fromNullInt64(row.CreatedAt)),
	}

	if spotConfigStr := fromNullString(row.SpotConfig); spotConfigStr != "" {
		if err := json.Unmarshal([]byte(spotConfigStr), &job.SpotConfig); err != nil {
			log.Error(err, "failed to unmarshal spot_config", "id", id)
		}
	}

	db.populateVerificationJobExtras(&job)
	return job, nil
}

func (db *Store) populateVerificationJobExtras(job *VerificationJob) {
	if db.Reader() != nil {
		var targetMode string
		var recursive int
		if err := db.Reader().QueryRowContext(db.ctx,
			"SELECT COALESCE(target_mode, 'backup_job'), COALESCE(recursive, 0) FROM verification_jobs WHERE id = ?",
			job.ID,
		).Scan(&targetMode, &recursive); err == nil {
			job.TargetMode = targetMode
			job.Recursive = recursive != 0
		}
	}

	if job.Schedule != "" {
		ev, err := calendar.Parse(job.Schedule)
		if err == nil {
			if nextRun, err := calendar.ComputeNextEvent(ev, time.Now(), time.Local); err == nil {
				job.NextRun = nextRun.Unix()
			}
		}
	}

	if job.History.LastRunUpid != "" {
		task, err := tasklog.GetTaskByUPID(job.History.LastRunUpid)
		if err == nil {
			job.History.LastRunStarttime = task.StartTime
			job.History.LastRunEndtime = task.EndTime
			if task.Status == "stopped" {
				job.History.LastRunState = task.ExitStatus
				job.History.Duration = task.EndTime - task.StartTime
			} else if task.StartTime > 0 {
				job.History.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if job.History.LastSuccessfulUpid != "" {
		if successTask, err := tasklog.GetTaskByUPID(job.History.LastSuccessfulUpid); err == nil {
			job.History.LastSuccessfulEndtime = successTask.EndTime
		}
	}
}

func (db *Store) GetAllVerificationJobs() ([]VerificationJob, error) {
	rows, err := db.readQueries.ListAllVerificationJobs(db.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllVerificationJobs: error querying: %w", err)
	}

	jobs := make([]VerificationJob, len(rows))
	for i, row := range rows {
		job := VerificationJob{
			ID:                  row.ID,
			BackupJobID:         row.BackupJobID,
			Store:               row.Store,
			Namespace:           fromNullString(row.Namespace),
			Mode:                row.Mode,
			Schedule:            fromNullString(row.Schedule),
			Comment:             fromNullString(row.Comment),
			NotificationMode:    fromNullString(row.NotificationMode),
			Retry:               fromNullInt64(row.Retry),
			RetryInterval:       fromNullInt64(row.RetryInterval),
			RunOnBackupComplete: fromNullInt64ToBool(row.RunOnBackupComplete),
			PendingSince:        int64(fromNullInt64(row.PendingSince)),
			History: JobHistory{
				LastRunUpid:           fromNullString(row.LastRunUpid),
				LastSuccessfulUpid:    fromNullString(row.LastSuccessfulUpid),
				LastRunStatus:         JobStatus(fromNullInt64(row.LastRunStatus)),
				RetryCount:            fromNullInt64(row.RetryCount),
				LastRunStarttime:      int64(fromNullInt64(row.LastRunStarttime)),
				LastRunEndtime:        int64(fromNullInt64(row.LastRunEndtime)),
				LastSuccessfulEndtime: int64(fromNullInt64(row.LastSuccessfulEndtime)),
			},
			CreatedAt: int64(fromNullInt64(row.CreatedAt)),
		}

		if spotConfigStr := fromNullString(row.SpotConfig); spotConfigStr != "" {
			if err := json.Unmarshal([]byte(spotConfigStr), &job.SpotConfig); err != nil {
				log.Error(err, "failed to unmarshal spot_config", "id", row.ID)
			}
		}

		db.populateVerificationJobExtras(&job)
		jobs[i] = job
	}

	return jobs, nil
}

func (db *Store) UpdateVerificationJob(tx *Transaction, job VerificationJob) (err error) {
	var commitNeeded bool
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateVerificationJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateVerificationJob: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateVerificationJob: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateVerificationJob: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if !validate.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("UpdateVerificationJob: invalid id string -> %s", job.ID)
	}
	if job.BackupJobID == "" && job.TargetMode != "namespace" {
		return fmt.Errorf("%w: backup_job_id is required", ErrValidationFailed)
	}
	if !validate.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("invalid namespace string: %s", job.Namespace)
	}
	if err := validate.ValidateOnCalendar(job.Schedule); err != nil && job.Schedule != "" {
		return fmt.Errorf("invalid schedule string: %s", job.Schedule)
	}
	if job.RetryInterval <= 0 {
		job.RetryInterval = 1
	}
	if job.Retry < 0 {
		job.Retry = 0
	}

	spotConfigJSON, err := json.Marshal(job.SpotConfig)
	if err != nil {
		return fmt.Errorf("UpdateVerificationJob: failed to marshal spot_config: %w", err)
	}

	err = q.UpdateVerificationJob(db.ctx, corequery.UpdateVerificationJobParams{
		BackupJobID:           job.BackupJobID,
		Store:                 job.Store,
		Namespace:             toNullString(job.Namespace),
		Mode:                  job.Mode,
		Schedule:              toNullString(job.Schedule),
		Comment:               toNullString(job.Comment),
		SpotConfig:            toNullString(string(spotConfigJSON)),
		LastRunUpid:           toNullString(job.History.LastRunUpid),
		LastSuccessfulUpid:    toNullString(job.History.LastSuccessfulUpid),
		LastRunStatus:         toNullInt64(int(job.History.LastRunStatus)),
		RetryCount:            toNullInt64(job.History.RetryCount),
		Retry:                 toNullInt64(job.Retry),
		RetryInterval:         toNullInt64(job.RetryInterval),
		LastRunStarttime:      toNullInt64(int(job.History.LastRunStarttime)),
		LastRunEndtime:        toNullInt64(int(job.History.LastRunEndtime)),
		LastSuccessfulEndtime: toNullInt64(int(job.History.LastSuccessfulEndtime)),
		RunOnBackupComplete:   boolToNullInt64(job.RunOnBackupComplete),
		PendingSince:          toNullInt64(int(job.PendingSince)),
		NotificationMode:      toNullString(job.NotificationMode),
		ID:                    job.ID,
	})
	if err != nil {
		return fmt.Errorf("UpdateVerificationJob: error updating: %w", err)
	}

	targetMode := job.TargetMode
	if targetMode == "" {
		targetMode = "backup_job"
	}
	_, err = tx.ExecContext(db.ctx,
		"UPDATE verification_jobs SET target_mode = ?, recursive = ? WHERE id = ?",
		targetMode, job.Recursive, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateVerificationJob: error setting target_mode/recursive: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) DeleteVerificationJob(tx *Transaction, id string) (err error) {
	var commitNeeded bool
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteVerificationJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteVerificationJob: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteVerificationJob: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteVerificationJob: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	_, err = q.DeleteVerificationResults(db.ctx, id)
	if err != nil {
		log.Error(fmt.Errorf("DeleteVerificationJob: error deleting results: %w", err), "", "id", id)
	}

	rowsAffected, err := q.DeleteVerificationJob(db.ctx, id)
	if err != nil {
		return fmt.Errorf("DeleteVerificationJob: error deleting: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}

	commitNeeded = true
	return nil
}

func (db *Store) CreateVerificationResult(result *VerificationResult) error {
	detailsJSON, err := json.Marshal(result.Details)
	if err != nil {
		return fmt.Errorf("CreateVerificationResult: failed to marshal details: %w", err)
	}

	res, err := db.queries.CreateVerificationResult(db.ctx, corequery.CreateVerificationResultParams{
		VerificationJobID: result.VerificationJobID,
		Upid:              toNullString(result.UPID),
		Snapshot:          result.Snapshot,
		SnapshotTime:      result.SnapshotTime,
		TotalFiles:        toNullInt64(result.TotalFiles),
		VerifiedFiles:     toNullInt64(result.VerifiedFiles),
		FailedFiles:       toNullInt64(result.FailedFiles),
		SkippedFiles:      toNullInt64(result.SkippedFiles),
		Status:            toNullString(result.Status),
		StartedAt:         toNullInt64(int(result.StartedAt)),
		CompletedAt:       toNullInt64(int(result.CompletedAt)),
		Details:           toNullString(string(detailsJSON)),
		TotalPopulation:   int64(result.TotalPopulation),
	})
	if err != nil {
		return fmt.Errorf("CreateVerificationResult: error inserting: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("CreateVerificationResult: error getting last insert id: %w", err)
	}
	result.ID = int(id)

	return nil
}

func (db *Store) UpdateVerificationResult(result VerificationResult) error {
	detailsJSON, err := json.Marshal(result.Details)
	if err != nil {
		return fmt.Errorf("UpdateVerificationResult: failed to marshal details: %w", err)
	}

	return db.queries.UpdateVerificationResult(db.ctx, corequery.UpdateVerificationResultParams{
		Upid:            toNullString(result.UPID),
		TotalFiles:      toNullInt64(result.TotalFiles),
		VerifiedFiles:   toNullInt64(result.VerifiedFiles),
		FailedFiles:     toNullInt64(result.FailedFiles),
		SkippedFiles:    toNullInt64(result.SkippedFiles),
		Status:          toNullString(result.Status),
		CompletedAt:     toNullInt64(int(result.CompletedAt)),
		Details:         toNullString(string(detailsJSON)),
		TotalPopulation: int64(result.TotalPopulation),
		ID:              int64(result.ID),
	})
}

func (db *Store) MarkVerificationResultStatus(id int, status string, completedAt int64) error {
	return db.queries.MarkVerificationResultStatus(db.ctx, corequery.MarkVerificationResultStatusParams{
		Status:      toNullString(status),
		CompletedAt: toNullInt64(int(completedAt)),
		ID:          int64(id),
	})
}

func (db *Store) GetVerificationResults(jobID string) ([]VerificationResult, error) {
	rows, err := db.readQueries.GetVerificationResults(db.ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("GetVerificationResults: error querying: %w", err)
	}

	results := make([]VerificationResult, len(rows))
	for i, row := range rows {
		r := VerificationResult{
			ID:                int(row.ID),
			VerificationJobID: row.VerificationJobID,
			UPID:              fromNullString(row.Upid),
			Snapshot:          row.Snapshot,
			SnapshotTime:      row.SnapshotTime,
			TotalFiles:        fromNullInt64(row.TotalFiles),
			VerifiedFiles:     fromNullInt64(row.VerifiedFiles),
			FailedFiles:       fromNullInt64(row.FailedFiles),
			SkippedFiles:      fromNullInt64(row.SkippedFiles),
			Status:            fromNullString(row.Status),
			StartedAt:         int64(fromNullInt64(row.StartedAt)),
			CompletedAt:       int64(fromNullInt64(row.CompletedAt)),
			TotalPopulation:   int(row.TotalPopulation),
		}

		if detailsStr := fromNullString(row.Details); detailsStr != "" {
			if err := json.Unmarshal([]byte(detailsStr), &r.Details); err != nil {
				log.Error(err, "failed to unmarshal details", "id", row.ID)
			}
		}

		results[i] = r
	}

	return results, nil
}

func (db *Store) GetLatestVerificationResult(jobID string) (VerificationResult, error) {
	row, err := db.readQueries.GetLatestVerificationResult(db.ctx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return VerificationResult{}, ErrNotFound
	}
	if err != nil {
		return VerificationResult{}, fmt.Errorf("GetLatestVerificationResult: error querying: %w", err)
	}

	r := VerificationResult{
		ID:                int(row.ID),
		VerificationJobID: row.VerificationJobID,
		UPID:              fromNullString(row.Upid),
		Snapshot:          row.Snapshot,
		SnapshotTime:      row.SnapshotTime,
		TotalFiles:        fromNullInt64(row.TotalFiles),
		VerifiedFiles:     fromNullInt64(row.VerifiedFiles),
		FailedFiles:       fromNullInt64(row.FailedFiles),
		SkippedFiles:      fromNullInt64(row.SkippedFiles),
		Status:            fromNullString(row.Status),
		StartedAt:         int64(fromNullInt64(row.StartedAt)),
		CompletedAt:       int64(fromNullInt64(row.CompletedAt)),
		TotalPopulation:   int(row.TotalPopulation),
	}

	if detailsStr := fromNullString(row.Details); detailsStr != "" {
		if err := json.Unmarshal([]byte(detailsStr), &r.Details); err != nil {
			log.Error(err, "failed to unmarshal details", "id", row.ID)
		}
	}

	return r, nil
}
