//go:build linux

package database

import (
	"database/sql"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database/sqlc"
)

type NotificationBatch struct {
	Name             string `json:"name"`
	Comment          string `json:"comment"`
	NotificationMode string `json:"notification-mode"`
	WaitTimeoutSecs  int    `json:"wait-timeout-secs"`
	SendOnTimeout    bool   `json:"send-on-timeout"`
	CreatedAt        int64  `json:"created-at"`
}

type NotificationBatchJob struct {
	BatchName string `json:"batch-name"`
	JobType   string `json:"job-type"`
	JobID     string `json:"job-id"`
}

func notificationBatchFromRow(r sqlc.NotificationBatch) NotificationBatch {
	return NotificationBatch{
		Name:             r.Name,
		Comment:          fromNullString(r.Comment),
		NotificationMode: fromNullString(r.NotificationMode),
		WaitTimeoutSecs:  fromNullInt64(r.WaitTimeoutSecs),
		SendOnTimeout:    fromNullInt64ToBool(r.SendOnTimeout),
		CreatedAt:        int64(fromNullInt64(r.CreatedAt)),
	}
}

func (database *Database) CreateNotificationBatch(batch NotificationBatch) error {
	return database.queries.CreateNotificationBatch(database.ctx, sqlc.CreateNotificationBatchParams{
		Name:             batch.Name,
		Comment:          toNullString(batch.Comment),
		NotificationMode: toNullString(batch.NotificationMode),
		WaitTimeoutSecs:  toNullInt64(batch.WaitTimeoutSecs),
		SendOnTimeout:    boolToNullInt64(batch.SendOnTimeout),
	})
}

func (database *Database) GetNotificationBatch(name string) (NotificationBatch, error) {
	r, err := database.readQueries.GetNotificationBatch(database.ctx, name)
	if err != nil {
		return NotificationBatch{}, err
	}
	return notificationBatchFromRow(r), nil
}

func (database *Database) ListNotificationBatches() ([]NotificationBatch, error) {
	rows, err := database.readQueries.ListNotificationBatches(database.ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NotificationBatch, len(rows))
	for i, r := range rows {
		out[i] = notificationBatchFromRow(r)
	}
	return out, nil
}

func (database *Database) UpdateNotificationBatch(batch NotificationBatch) error {
	return database.queries.UpdateNotificationBatch(database.ctx, sqlc.UpdateNotificationBatchParams{
		Comment:          toNullString(batch.Comment),
		NotificationMode: toNullString(batch.NotificationMode),
		WaitTimeoutSecs:  toNullInt64(batch.WaitTimeoutSecs),
		SendOnTimeout:    boolToNullInt64(batch.SendOnTimeout),
		Name:             batch.Name,
	})
}

func (database *Database) DeleteNotificationBatch(name string) error {
	return database.queries.DeleteNotificationBatch(database.ctx, name)
}

func (database *Database) AddJobToBatch(batchName, jobType, jobID string) error {
	return database.queries.AddJobToBatch(database.ctx, sqlc.AddJobToBatchParams{
		BatchName: batchName,
		JobType:   jobType,
		JobID:     jobID,
	})
}

func (database *Database) RemoveJobFromBatch(batchName, jobType, jobID string) error {
	return database.queries.RemoveJobFromBatch(database.ctx, sqlc.RemoveJobFromBatchParams{
		BatchName: batchName,
		JobType:   jobType,
		JobID:     jobID,
	})
}

func (database *Database) GetBatchForJob(jobType, jobID string) (NotificationBatch, error) {
	r, err := database.readQueries.GetBatchForJob(database.ctx, sqlc.GetBatchForJobParams{
		JobType: jobType,
		JobID:   jobID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return NotificationBatch{}, nil
		}
		return NotificationBatch{}, err
	}
	return notificationBatchFromRow(r), nil
}

func (database *Database) GetBatchJobs(batchName string) ([]NotificationBatchJob, error) {
	rows, err := database.readQueries.GetBatchJobsByBatch(database.ctx, batchName)
	if err != nil {
		return nil, err
	}
	out := make([]NotificationBatchJob, len(rows))
	for i, r := range rows {
		out[i] = NotificationBatchJob{
			BatchName: r.BatchName,
			JobType:   r.JobType,
			JobID:     r.JobID,
		}
	}
	return out, nil
}

func (database *Database) RemoveJobsByBatch(batchName string) error {
	return database.queries.RemoveJobsByBatch(database.ctx, batchName)
}

func (database *Database) RemoveJobFromAllBatches(jobType, jobID string) error {
	return database.queries.RemoveJobFromAllBatches(database.ctx, sqlc.RemoveJobFromAllBatchesParams{
		JobType: jobType,
		JobID:   jobID,
	})
}

func (database *Database) ListBatchJobs() ([]NotificationBatchJob, error) {
	rows, err := database.readQueries.ListBatchJobs(database.ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NotificationBatchJob, len(rows))
	for i, r := range rows {
		out[i] = NotificationBatchJob{
			BatchName: r.BatchName,
			JobType:   r.JobType,
			JobID:     r.JobID,
		}
	}
	return out, nil
}

func (database *Database) GetBackupLastRunEndtime(jobID string) int64 {
	b, err := database.GetBackup(jobID)
	if err != nil {
		return 0
	}
	return b.History.LastRunEndtime
}

// GetRestoreLastRunEndtime returns the last run endtime for a restore job, or 0 if not found.
func (database *Database) GetRestoreLastRunEndtime(jobID string) int64 {
	r, err := database.GetRestore(jobID)
	if err != nil {
		return 0
	}
	return r.History.LastRunEndtime
}

// AllBatchJobsCompleted checks if all jobs in a batch have completed
func (database *Database) AllBatchJobsCompleted(batchName string) bool {
	jobs, err := database.GetBatchJobs(batchName)
	if err != nil || len(jobs) == 0 {
		return false
	}

	for _, j := range jobs {
		switch j.JobType {
		case "backup":
			if database.GetBackupLastRunEndtime(j.JobID) == 0 {
				return false
			}
		case "restore":
			if database.GetRestoreLastRunEndtime(j.JobID) == 0 {
				return false
			}
		case "verification":
			v, err := database.GetVerificationJob(j.JobID)
			if err != nil || v.History.LastRunEndtime == 0 {
				return false
			}
		}
	}
	return true
}

func (database *Database) WaitForBatchCompletion(batchName string, timeout time.Duration, checkInterval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if database.AllBatchJobsCompleted(batchName) {
			return true
		}
		time.Sleep(checkInterval)
	}
	return false
}
