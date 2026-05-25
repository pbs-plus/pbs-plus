//go:build linux

package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/server/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func (database *Database) CreateVerificationJob(tx *Transaction, job VerificationJob) (err error) {
	var commitNeeded bool
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateVerificationJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateVerificationJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateVerificationJob: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateVerificationJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	if job.ID == "" {
		baseID := validate.Slugify(job.BackupJobID) + "-verify"
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
			_, err := database.readQueries.VerificationJobExists(database.ctx, newID)
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

	if job.BackupJobID == "" {
		return errors.New("backup_job_id is required")
	}
	if job.Store == "" {
		return errors.New("store is required")
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
	if job.SpotConfig.SampleCount <= 0 {
		job.SpotConfig.SampleCount = 10
	}

	spotConfigJSON, err := json.Marshal(job.SpotConfig)
	if err != nil {
		return fmt.Errorf("CreateVerificationJob: failed to marshal spot_config: %w", err)
	}

	err = q.CreateVerificationJob(database.ctx, sqlc.CreateVerificationJobParams{
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
	})
	if err != nil {
		return fmt.Errorf("CreateVerificationJob: error inserting verification job: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetVerificationJob(id string) (VerificationJob, error) {
	row, err := database.readQueries.GetVerificationJob(database.ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return VerificationJob{}, ErrNotFound
	}
	if err != nil {
		return VerificationJob{}, fmt.Errorf("GetVerificationJob: error querying: %w", err)
	}

	job := VerificationJob{
		ID:            row.ID,
		BackupJobID:   row.BackupJobID,
		Store:         row.Store,
		Namespace:     fromNullString(row.Namespace),
		Mode:          row.Mode,
		Schedule:      fromNullString(row.Schedule),
		Comment:       fromNullString(row.Comment),
		Retry:         fromNullInt64(row.Retry),
		RetryInterval: fromNullInt64(row.RetryInterval),
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
			syslog.L.Error(err).WithField("id", id).WithMessage("failed to unmarshal spot_config").Write()
		}
	}

	database.populateVerificationJobExtras(&job)
	return job, nil
}

func (database *Database) populateVerificationJobExtras(job *VerificationJob) {
	if job.Schedule != "" {
		ev, err := calendar.Parse(job.Schedule)
		if err == nil {
			if nextRun, err := calendar.ComputeNextEvent(ev, time.Now(), time.Local); err == nil {
				job.NextRun = nextRun.Unix()
			}
		}
	}

	// Enrich history from task logs (same pattern as backup/restore)
	if job.History.LastRunUpid != "" {
		task, err := proxmox.GetTaskByUPID(job.History.LastRunUpid)
		if err == nil {
			job.History.LastRunStarttime = task.StartTime
			job.History.LastRunEndtime = task.EndTime
			if task.ExitStatus != "" {
				job.History.LastRunState = task.ExitStatus
				job.History.Duration = task.EndTime - task.StartTime
			} else {
				job.History.Duration = time.Now().Unix() - task.StartTime
			}
		}
	}
	if job.History.LastSuccessfulUpid != "" {
		if successTask, err := proxmox.GetTaskByUPID(job.History.LastSuccessfulUpid); err == nil {
			job.History.LastSuccessfulEndtime = successTask.EndTime
		}
	}
}

func (database *Database) GetAllVerificationJobs() ([]VerificationJob, error) {
	rows, err := database.readQueries.ListAllVerificationJobs(database.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllVerificationJobs: error querying: %w", err)
	}

	jobs := make([]VerificationJob, len(rows))
	for i, row := range rows {
		job := VerificationJob{
			ID:            row.ID,
			BackupJobID:   row.BackupJobID,
			Store:         row.Store,
			Namespace:     fromNullString(row.Namespace),
			Mode:          row.Mode,
			Schedule:      fromNullString(row.Schedule),
			Comment:       fromNullString(row.Comment),
			Retry:         fromNullInt64(row.Retry),
			RetryInterval: fromNullInt64(row.RetryInterval),
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
				syslog.L.Error(err).WithField("id", row.ID).WithMessage("failed to unmarshal spot_config").Write()
			}
		}

		database.populateVerificationJobExtras(&job)
		jobs[i] = job
	}

	return jobs, nil
}

func (database *Database) UpdateVerificationJob(tx *Transaction, job VerificationJob) (err error) {
	var commitNeeded bool
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateVerificationJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateVerificationJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateVerificationJob: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateVerificationJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	if !validate.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("UpdateVerificationJob: invalid id string -> %s", job.ID)
	}
	if job.BackupJobID == "" {
		return errors.New("backup_job_id is required")
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

	err = q.UpdateVerificationJob(database.ctx, sqlc.UpdateVerificationJobParams{
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
		ID:                    job.ID,
	})
	if err != nil {
		return fmt.Errorf("UpdateVerificationJob: error updating: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) DeleteVerificationJob(tx *Transaction, id string) (err error) {
	var commitNeeded bool
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteVerificationJob: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteVerificationJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteVerificationJob: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteVerificationJob: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	_, err = q.DeleteVerificationResults(database.ctx, id)
	if err != nil {
		syslog.L.Error(fmt.Errorf("DeleteVerificationJob: error deleting results: %w", err)).
			WithField("id", id).Write()
	}

	rowsAffected, err := q.DeleteVerificationJob(database.ctx, id)
	if err != nil {
		return fmt.Errorf("DeleteVerificationJob: error deleting: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}

	commitNeeded = true
	return nil
}

func (database *Database) CreateVerificationResult(result *VerificationResult) error {
	detailsJSON, err := json.Marshal(result.Details)
	if err != nil {
		return fmt.Errorf("CreateVerificationResult: failed to marshal details: %w", err)
	}

	res, err := database.queries.CreateVerificationResult(database.ctx, sqlc.CreateVerificationResultParams{
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

func (database *Database) UpdateVerificationResult(result VerificationResult) error {
	detailsJSON, err := json.Marshal(result.Details)
	if err != nil {
		return fmt.Errorf("UpdateVerificationResult: failed to marshal details: %w", err)
	}

	return database.queries.UpdateVerificationResult(database.ctx, sqlc.UpdateVerificationResultParams{
		Upid:          toNullString(result.UPID),
		TotalFiles:    toNullInt64(result.TotalFiles),
		VerifiedFiles: toNullInt64(result.VerifiedFiles),
		FailedFiles:   toNullInt64(result.FailedFiles),
		SkippedFiles:  toNullInt64(result.SkippedFiles),
		Status:        toNullString(result.Status),
		CompletedAt:   toNullInt64(int(result.CompletedAt)),
		Details:       toNullString(string(detailsJSON)),
		ID:            int64(result.ID),
	})
}

func (database *Database) GetVerificationResults(jobID string) ([]VerificationResult, error) {
	rows, err := database.readQueries.GetVerificationResults(database.ctx, jobID)
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
		}

		if detailsStr := fromNullString(row.Details); detailsStr != "" {
			if err := json.Unmarshal([]byte(detailsStr), &r.Details); err != nil {
				syslog.L.Error(err).WithField("id", row.ID).WithMessage("failed to unmarshal details").Write()
			}
		}

		results[i] = r
	}

	return results, nil
}

func (database *Database) GetLatestVerificationResult(jobID string) (VerificationResult, error) {
	row, err := database.readQueries.GetLatestVerificationResult(database.ctx, jobID)
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
	}

	if detailsStr := fromNullString(row.Details); detailsStr != "" {
		if err := json.Unmarshal([]byte(detailsStr), &r.Details); err != nil {
			syslog.L.Error(err).WithField("id", row.ID).WithMessage("failed to unmarshal details").Write()
		}
	}

	return r, nil
}
