package scheduler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

const schedulerTickInterval = 30 * time.Second

type Scheduler struct {
	ctx           context.Context
	cancel        context.CancelFunc
	storeInstance *store.Store
}

func NewScheduler(ctx context.Context, storeInstance *store.Store) *Scheduler {
	newCtx, cancel := context.WithCancel(ctx)
	return &Scheduler{ctx: newCtx, cancel: cancel, storeInstance: storeInstance}
}

func (s *Scheduler) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error(fmt.Errorf("scheduler panic: %v", r), "Scheduler: panic recovered")
			}
		}()
		s.run()
	}()
}

func (s *Scheduler) run() {
	ticker := time.NewTicker(schedulerTickInterval)
	defer ticker.Stop()
	log.Info("internal scheduler started")

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkBackups()
			s.checkRestores()
			s.checkVerifications()
		}
	}
}

func (s *Scheduler) submitBackup(b database.Backup, trigger string, occurrence time.Time) error {
	request, err := jobs.NewWorkflowSubmit(
		jobs.WorkflowBackup,
		b.ID,
		trigger,
		fmt.Sprintf("backup:%s:%s:%d", b.ID, trigger, occurrence.Unix()),
		jobs.BackupInput{},
		[]string{"backup:" + b.ID, "target:" + b.Target.Name},
		b.Retry+1,
		time.Duration(max(b.RetryInterval, 1))*time.Minute,
	)
	if err != nil {
		return err
	}
	_, _, err = s.storeInstance.Engine.Submit(s.storeInstance.Ctx, request)
	return err
}

func (s *Scheduler) checkBackups() {
	backups, err := s.storeInstance.Database.GetAllBackups()
	if err != nil {
		log.Error(err, "Scheduler: failed to get all backups")
		return
	}

	now := time.Now()

	for _, b := range backups {
		if b.Schedule != "" {
			if nextRun, ok := s.shouldRunScheduled(b.Schedule, b.History.LastRunStarttime, now); ok {
				log.Info("scheduler: scheduled backup is due, submitting", "backupID", b.ID, "nextRun", nextRun)
				if err := s.submitBackup(b, "scheduled", nextRun); err != nil {
					log.Error(err, "Scheduler: failed to submit backup", "backupID", b.ID)
				}
			}
		}
		if b.Retry > 0 && s.shouldRetryBackup(b, now) {
			log.Info("scheduler: backup retry is due, submitting", "backupID", b.ID)
			if err := s.submitBackup(b, "retry", time.Unix(b.History.LastRunEndtime, 0)); err != nil {
				log.Error(err, "Scheduler: failed to submit backup retry", "backupID", b.ID)
			}
		}
	}
}

// shouldRunScheduled reports the occurrence of schedule due after lastRun.
// Occurrence-keyed dedupe makes submission exactly-once across restarts; downtime yields one catch-up run.
func (s *Scheduler) shouldRunScheduled(schedule string, lastRun int64, now time.Time) (time.Time, bool) {
	ev, err := calendar.Parse(schedule)
	if err != nil {
		return time.Time{}, false
	}
	refTime := now
	if lastRun > 0 {
		refTime = time.Unix(lastRun, 0)
	}
	nextRun, err := calendar.ComputeNextEvent(ev, refTime, time.Local)
	if err != nil || nextRun.After(now) {
		return time.Time{}, false
	}
	return nextRun, true
}

func (s *Scheduler) shouldRetryBackup(b database.Backup, now time.Time) bool {
	if b.History.LastRunEndtime == 0 {
		return false
	}
	if now.Sub(time.Unix(b.History.LastRunEndtime, 0)) < time.Duration(b.RetryInterval)*time.Minute {
		return false
	}
	if !lastRunRetryable(b.History.LastRunStatus, b.History.LastRunState) {
		return false
	}
	return b.History.RetryCount < b.Retry
}

func lastRunRetryable(status database.JobStatus, state string) bool {
	if status == database.JobStatusUnknown {
		return database.JobStatusFromString(state).ShouldRetry()
	}
	return status.ShouldRetry()
}

func (s *Scheduler) checkRestores() {
	restores, err := s.storeInstance.Database.GetAllRestores()
	if err != nil {
		log.Error(err, "Scheduler: failed to get all restores")
		return
	}

	now := time.Now()

	for _, r := range restores {
		if r.Retry == 0 || !s.shouldRetryRestore(r, now) {
			continue
		}
		request, err := jobs.NewWorkflowSubmit(
			jobs.WorkflowRestore,
			r.ID,
			"retry",
			fmt.Sprintf("restore:%s:retry:%d-%d", r.ID, r.History.LastRunEndtime, r.History.RetryCount),
			jobs.RestoreInput{},
			[]string{"restore:" + r.ID, "target:" + r.DestTarget.Name},
			r.Retry+1,
			time.Duration(max(r.RetryInterval, 1))*time.Minute,
		)
		if err != nil {
			log.Error(err, "Scheduler: failed to build restore submit", "restoreID", r.ID)
			continue
		}
		if _, _, err := s.storeInstance.Engine.Submit(s.storeInstance.Ctx, request); err != nil {
			log.Error(err, "Scheduler: failed to submit restore", "restoreID", r.ID)
		}
	}
}

func (s *Scheduler) shouldRetryRestore(r database.Restore, now time.Time) bool {
	if r.History.LastRunEndtime == 0 {
		return false
	}
	if now.Sub(time.Unix(r.History.LastRunEndtime, 0)) < time.Duration(r.RetryInterval)*time.Minute {
		return false
	}
	if !lastRunRetryable(r.History.LastRunStatus, r.History.LastRunState) {
		return false
	}
	return r.History.RetryCount < r.Retry
}

func (s *Scheduler) checkVerifications() {
	vJobs, err := s.storeInstance.Database.GetAllVerificationJobs()
	if err != nil {
		log.Error(err, "Scheduler: failed to get verification jobs")
		return
	}

	now := time.Now()

	for _, vJob := range vJobs {
		if vJob.Schedule == "" {
			continue
		}
		if _, due := s.shouldRunScheduled(vJob.Schedule, vJob.History.LastRunEndtime, now); !due {
			continue
		}
		if vJob.RunOnBackupComplete {
			if vJob.PendingSince == 0 {
				vJob.PendingSince = now.Unix()
				if err := s.storeInstance.Database.UpdateVerificationJob(nil, vJob); err != nil {
					log.Error(err, "Scheduler: failed to set pending_since", "verificationJobID", vJob.ID)
				}
			}
			continue
		}
		if err := s.submitVerification(vJob, "scheduled", time.Unix(vJob.History.LastRunEndtime, 0)); err != nil {
			log.Error(err, "Scheduler: failed to submit verification", "verificationJobID", vJob.ID)
		}
	}
}

func (s *Scheduler) submitVerification(vJob database.VerificationJob, trigger string, occurrence time.Time) error {
	request, err := jobs.NewWorkflowSubmit(
		jobs.WorkflowVerification,
		vJob.ID,
		trigger,
		fmt.Sprintf("verification:%s:%s:%d", vJob.ID, trigger, occurrence.Unix()),
		jobs.VerificationInput{},
		[]string{"verification:" + vJob.ID},
		vJob.Retry+1,
		time.Duration(max(vJob.RetryInterval, 1))*time.Minute,
	)
	if err != nil {
		return err
	}
	_, _, err = s.storeInstance.Engine.Submit(s.storeInstance.Ctx, request)
	return err
}

// TriggerPendingVerifications submits verification jobs that waited on backup completion.
func (s *Scheduler) TriggerPendingVerifications(backupJobID string) {
	vJobs, err := s.storeInstance.Database.GetAllVerificationJobs()
	if err != nil {
		log.Error(err, "TriggerPendingVerifications: failed to list verification jobs")
		return
	}

	completedBackup, err := s.storeInstance.Database.GetBackup(backupJobID)
	if err != nil {
		log.Error(err, "TriggerPendingVerifications: failed to get backup job")
		return
	}

	for _, vJob := range vJobs {
		if vJob.PendingSince == 0 {
			continue
		}
		matched := false
		if vJob.TargetMode == "backup_job" && vJob.BackupJobID == backupJobID {
			matched = true
		} else if vJob.TargetMode == "namespace" && vJob.Store == completedBackup.Store {
			if vJob.Recursive {
				matched = vJob.Namespace == "" || completedBackup.Namespace == vJob.Namespace || strings.HasPrefix(completedBackup.Namespace, vJob.Namespace+"/")
			} else {
				matched = completedBackup.Namespace == vJob.Namespace
			}
		}
		if !matched {
			continue
		}

		vJob.PendingSince = 0
		if err := s.storeInstance.Database.UpdateVerificationJob(nil, vJob); err != nil {
			log.Error(err, "failed to clear pending_since", "verificationJobID", vJob.ID)
			continue
		}
		if err := s.submitVerification(vJob, "backup_complete", time.Unix(vJob.History.LastRunEndtime, 0)); err != nil {
			log.Error(err, "failed to submit verification", "backupJobID", backupJobID, "verificationJobID", vJob.ID)
		}
	}
}
