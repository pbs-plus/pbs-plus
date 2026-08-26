//go:build linux

package coredb

import (
	"database/sql"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
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

func notificationBatchFromRow(r corequery.NotificationBatch) NotificationBatch {
	return NotificationBatch{
		Name:             r.Name,
		Comment:          fromNullString(r.Comment),
		NotificationMode: fromNullString(r.NotificationMode),
		WaitTimeoutSecs:  fromNullInt64(r.WaitTimeoutSecs),
		SendOnTimeout:    fromNullInt64ToBool(r.SendOnTimeout),
		CreatedAt:        int64(fromNullInt64(r.CreatedAt)),
	}
}

func (db *Store) CreateNotificationBatch(batch NotificationBatch) error {
	return db.queries.CreateNotificationBatch(db.ctx, corequery.CreateNotificationBatchParams{
		Name:             batch.Name,
		Comment:          toNullString(batch.Comment),
		NotificationMode: toNullString(batch.NotificationMode),
		WaitTimeoutSecs:  toNullInt64(batch.WaitTimeoutSecs),
		SendOnTimeout:    boolToNullInt64(batch.SendOnTimeout),
	})
}

func (db *Store) GetNotificationBatch(name string) (NotificationBatch, error) {
	r, err := db.readQueries.GetNotificationBatch(db.ctx, name)
	if err != nil {
		return NotificationBatch{}, err
	}
	return notificationBatchFromRow(r), nil
}

func (db *Store) ListNotificationBatches() ([]NotificationBatch, error) {
	rows, err := db.readQueries.ListNotificationBatches(db.ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NotificationBatch, len(rows))
	for i, r := range rows {
		out[i] = notificationBatchFromRow(r)
	}
	return out, nil
}

func (db *Store) UpdateNotificationBatch(batch NotificationBatch) error {
	return db.queries.UpdateNotificationBatch(db.ctx, corequery.UpdateNotificationBatchParams{
		Comment:          toNullString(batch.Comment),
		NotificationMode: toNullString(batch.NotificationMode),
		WaitTimeoutSecs:  toNullInt64(batch.WaitTimeoutSecs),
		SendOnTimeout:    boolToNullInt64(batch.SendOnTimeout),
		Name:             batch.Name,
	})
}

func (db *Store) DeleteNotificationBatch(name string) error {
	return db.queries.DeleteNotificationBatch(db.ctx, name)
}

func (db *Store) AddJobToBatch(batchName, jobType, jobID string) error {
	return db.queries.AddJobToBatch(db.ctx, corequery.AddJobToBatchParams{
		BatchName: batchName,
		JobType:   jobType,
		JobID:     jobID,
	})
}

func (db *Store) RemoveJobFromBatch(batchName, jobType, jobID string) error {
	return db.queries.RemoveJobFromBatch(db.ctx, corequery.RemoveJobFromBatchParams{
		BatchName: batchName,
		JobType:   jobType,
		JobID:     jobID,
	})
}

func (db *Store) GetBatchForJob(jobType, jobID string) (NotificationBatch, error) {
	r, err := db.readQueries.GetBatchForJob(db.ctx, corequery.GetBatchForJobParams{
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

func (db *Store) GetBatchJobs(batchName string) ([]NotificationBatchJob, error) {
	rows, err := db.readQueries.GetBatchJobsByBatch(db.ctx, batchName)
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

func (db *Store) RemoveJobsByBatch(batchName string) error {
	return db.queries.RemoveJobsByBatch(db.ctx, batchName)
}

func (db *Store) RemoveJobFromAllBatches(jobType, jobID string) error {
	return db.queries.RemoveJobFromAllBatches(db.ctx, corequery.RemoveJobFromAllBatchesParams{
		JobType: jobType,
		JobID:   jobID,
	})
}

func (db *Store) ListBatchJobs() ([]NotificationBatchJob, error) {
	rows, err := db.readQueries.ListBatchJobs(db.ctx)
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

func (db *Store) GetBackupLastRunEndtime(jobID string) int64 {
	b, err := db.GetBackup(jobID)
	if err != nil {
		return 0
	}
	return b.History.LastRunEndtime
}

// GetRestoreLastRunEndtime returns the last run endtime for a restore job, or 0 if not found.
func (db *Store) GetRestoreLastRunEndtime(jobID string) int64 {
	r, err := db.GetRestore(jobID)
	if err != nil {
		return 0
	}
	return r.History.LastRunEndtime
}

// AllBatchJobsCompleted checks if all jobs in a batch have completed
func (db *Store) AllBatchJobsCompleted(batchName string) bool {
	jobs, err := db.GetBatchJobs(batchName)
	if err != nil || len(jobs) == 0 {
		return false
	}

	for _, j := range jobs {
		switch j.JobType {
		case "backup":
			if db.GetBackupLastRunEndtime(j.JobID) == 0 {
				return false
			}
		case "restore":
			if db.GetRestoreLastRunEndtime(j.JobID) == 0 {
				return false
			}
		case "verification":
			v, err := db.GetVerificationJob(j.JobID)
			if err != nil || v.History.LastRunEndtime == 0 {
				return false
			}
		}
	}
	return true
}

func (db *Store) WaitForBatchCompletion(batchName string, timeout time.Duration, checkInterval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if db.AllBatchJobsCompleted(batchName) {
			return true
		}
		time.Sleep(checkInterval)
	}
	return false
}
