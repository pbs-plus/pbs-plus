package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/server/backup"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf"
	mtfdb "github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/server/restore"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/verification"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const schedulerTickInterval = 30 * time.Second

type Scheduler struct {
	ctx            context.Context
	cancel         context.CancelFunc
	storeInstance  *store.Store
	manager        *jobs.Manager
	lastEnqueued   map[string]time.Time
	lastEnqueuedMu sync.Mutex
}

func NewScheduler(ctx context.Context, storeInstance *store.Store, manager *jobs.Manager) *Scheduler {
	newCtx, cancel := context.WithCancel(ctx)
	return &Scheduler{
		ctx:           newCtx,
		cancel:        cancel,
		storeInstance: storeInstance,
		manager:       manager,
		lastEnqueued:  make(map[string]time.Time),
	}
}

func (s *Scheduler) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				syslog.L.Error(fmt.Errorf("scheduler panic: %v", r)).WithMessage("Scheduler: panic recovered").Write()
			}
		}()
		s.run()
	}()
}

func (s *Scheduler) run() {
	ticker := time.NewTicker(schedulerTickInterval)
	defer ticker.Stop()

	syslog.L.Info().WithMessage("Internal scheduler started").Write()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkBackups()
			s.checkRestores()
			s.checkVerifications()
			s.checkMtfJobs()
		}
	}
}

func (s *Scheduler) checkBackups() {
	backups, err := s.storeInstance.Database.GetAllBackups()
	if err != nil {
		syslog.L.Error(err).WithMessage("Scheduler: failed to get all backups").Write()
		return
	}

	now := time.Now()

	for _, b := range backups {
		if s.manager.IsRunning(b.ID) {
			continue
		}

		if b.Schedule != "" {
			if nextRun, ok := s.shouldRunScheduled(b, now); ok {
				syslog.L.Info().WithField("backupID", b.ID).WithMessage("Scheduler: scheduled backup is due, enqueuing").Write()
				s.markEnqueued(b.ID, nextRun)
				job := backup.NewBackupJob(b, s.storeInstance, false, false, nil)
				go s.enqueueBackup(b.ID, job)
				continue
			}
		}

		if b.Retry > 0 && s.shouldRetryBackup(b, now) {
			syslog.L.Info().WithField("backupID", b.ID).WithMessage("Scheduler: backup retry is due, enqueuing").Write()
			job := backup.NewBackupJob(b, s.storeInstance, false, false, nil)
			go s.enqueueBackup(b.ID, job)
		}
	}
}

// as the reference point to prevent duplicate launches.
// On restart, the in-memory lastEnqueued map is lost, so we guard against
// scheduled time fell within the current check interval.
func (s *Scheduler) shouldRunScheduled(b database.Backup, now time.Time) (time.Time, bool) {
	ev, err := calendar.Parse(b.Schedule)
	if err != nil {
		return time.Time{}, false
	}

	// Determine reference time: use the latest of lastEnqueued or LastRunStarttime
	refTime := now
	if b.History.LastRunStarttime > 0 {
		refTime = time.Unix(b.History.LastRunStarttime, 0)
	}
	if lastEnq, ok := s.getEnqueued(b.ID); ok && lastEnq.After(refTime) {
		refTime = lastEnq
	}

	nextRun, err := calendar.ComputeNextEvent(ev, refTime, time.Local)
	if err != nil {
		return time.Time{}, false
	}

	if nextRun.After(now) {
		return time.Time{}, false
	}

	// the current check interval (30s). This prevents catch-up runs on restart
	if now.Sub(nextRun) < schedulerTickInterval {
		return nextRun, true
	}

	// The scheduled time was missed by more than one check interval.
	futureRun, err := calendar.ComputeNextEvent(ev, now, time.Local)
	if err != nil {
		return time.Time{}, false
	}

	// Mark this future run as already counted so we don't re-trigger
	s.markEnqueued(b.ID, futureRun)

	return time.Time{}, false
}

func (s *Scheduler) markEnqueued(backupID string, t time.Time) {
	s.lastEnqueuedMu.Lock()
	defer s.lastEnqueuedMu.Unlock()
	s.lastEnqueued[backupID] = t
}

func (s *Scheduler) getEnqueued(backupID string) (time.Time, bool) {
	s.lastEnqueuedMu.Lock()
	defer s.lastEnqueuedMu.Unlock()
	t, ok := s.lastEnqueued[backupID]
	return t, ok
}

func (s *Scheduler) shouldRetryBackup(b database.Backup, now time.Time) bool {
	if b.History.LastRunEndtime == 0 {
		return false
	}

	lastEnd := time.Unix(b.History.LastRunEndtime, 0)
	if now.Sub(lastEnd) < time.Duration(b.RetryInterval)*time.Minute {
		return false
	}

	// Fall back to legacy string parsing for records before migration
	shouldRetry := b.History.LastRunStatus.ShouldRetry()
	if b.History.LastRunStatus == database.JobStatusUnknown {
		shouldRetry = isFailedState(b.History.LastRunState)
	}

	if !shouldRetry {
		return false
	}

	return b.History.RetryCount < b.Retry
}

func (s *Scheduler) checkRestores() {
	restores, err := s.storeInstance.Database.GetAllRestores()
	if err != nil {
		syslog.L.Error(err).WithMessage("Scheduler: failed to get all restores").Write()
		return
	}

	now := time.Now()

	for _, r := range restores {
		if s.manager.IsRunning(r.ID) {
			continue
		}

		if r.Retry > 0 && s.shouldRetryRestore(r, now) {
			syslog.L.Info().WithField("restoreID", r.ID).WithMessage("Scheduler: restore retry is due, enqueuing").Write()
			job, err := restore.NewRestoreJob(r, s.storeInstance, false, false)
			if err != nil {
				syslog.L.Error(err).WithField("restoreID", r.ID).WithMessage("Scheduler: failed to create restore job").Write()
				continue
			}
			go s.enqueueRestore(r.ID, job)
		}
	}
}

func (s *Scheduler) shouldRetryRestore(r database.Restore, now time.Time) bool {
	if r.History.LastRunEndtime == 0 {
		return false
	}

	lastEnd := time.Unix(r.History.LastRunEndtime, 0)
	if now.Sub(lastEnd) < time.Duration(r.RetryInterval)*time.Minute {
		return false
	}

	shouldRetry := r.History.LastRunStatus.ShouldRetry()
	if r.History.LastRunStatus == database.JobStatusUnknown {
		shouldRetry = isFailedState(r.History.LastRunState)
	}

	if !shouldRetry {
		return false
	}

	return r.History.RetryCount < r.Retry
}

func (s *Scheduler) checkMtfJobs() {
	ms := s.storeInstance.MtfStore
	if ms == nil {
		return
	}
	mjobs, err := ms.ListMtfJobs(s.ctx)
	if err != nil {
		syslog.L.Error(err).WithMessage("Scheduler: failed to get MTF jobs").Write()
		return
	}

	now := time.Now()
	for _, mj := range mjobs {
		if s.manager.IsRunning(mj.ID) {
			continue
		}

		if mj.Schedule != "" {
			if nextRun, ok := s.shouldRunScheduledMtf(mj, now); ok {
				syslog.L.Info().WithField("mtfJobID", mj.ID).WithMessage("Scheduler: scheduled MTF job is due, enqueuing").Write()
				s.markEnqueued(mj.ID, nextRun)
				if job, err := mtf.NewJob(mj.ID, s.storeInstance, false); err == nil {
					go s.enqueueMtf(mj.ID, job)
				} else {
					syslog.L.Error(err).WithField("mtfJobID", mj.ID).Write()
				}
				continue
			}
		}

		if mj.Retry > 0 && s.shouldRetryMtf(mj, now) {
			syslog.L.Info().WithField("mtfJobID", mj.ID).WithMessage("Scheduler: MTF job retry is due, enqueuing").Write()
			if job, err := mtf.NewJob(mj.ID, s.storeInstance, false); err == nil {
				go s.enqueueMtf(mj.ID, job)
			} else {
				syslog.L.Error(err).WithField("mtfJobID", mj.ID).Write()
			}
		}
	}
}

func (s *Scheduler) shouldRunScheduledMtf(mj mtfdb.MTFJob, now time.Time) (time.Time, bool) {
	ev, err := calendar.Parse(mj.Schedule)
	if err != nil {
		return time.Time{}, false
	}
	refTime := now
	if mj.History.LastRunStarttime > 0 {
		refTime = time.Unix(mj.History.LastRunStarttime, 0)
	}
	if lastEnq, ok := s.getEnqueued(mj.ID); ok && lastEnq.After(refTime) {
		refTime = lastEnq
	}
	nextRun, err := calendar.ComputeNextEvent(ev, refTime, time.Local)
	if err != nil {
		return time.Time{}, false
	}
	if nextRun.After(now) {
		return time.Time{}, false
	}
	if now.Sub(nextRun) < schedulerTickInterval {
		return nextRun, true
	}
	futureRun, err := calendar.ComputeNextEvent(ev, now, time.Local)
	if err != nil {
		return time.Time{}, false
	}
	s.markEnqueued(mj.ID, futureRun)
	return time.Time{}, false
}

func (s *Scheduler) shouldRetryMtf(mj mtfdb.MTFJob, now time.Time) bool {
	if mj.History.LastRunEndtime == 0 {
		return false
	}
	lastEnd := time.Unix(mj.History.LastRunEndtime, 0)
	if now.Sub(lastEnd) < time.Duration(mj.RetryInterval)*time.Minute {
		return false
	}
	shouldRetry := mj.History.LastRunStatus.ShouldRetry()
	if mj.History.LastRunStatus == database.JobStatusUnknown {
		shouldRetry = isFailedState(mj.History.LastRunState)
	}
	if !shouldRetry {
		return false
	}
	return mj.History.RetryCount < mj.Retry
}

func (s *Scheduler) enqueueMtf(id string, job *jobs.Job) {
	if err := s.manager.Enqueue(job); err != nil {
		syslog.L.Error(err).WithField("mtfJobID", id).WithMessage("Scheduler: failed to enqueue MTF job").Write()
	}
}

func (s *Scheduler) enqueueBackup(id string, job *jobs.Job) {
	if err := s.manager.Enqueue(job); err != nil {
		syslog.L.Error(err).WithField("backupID", id).WithMessage("Scheduler: failed to enqueue backup").Write()
	}
}
func (s *Scheduler) enqueueRestore(id string, job *jobs.Job) {
	if err := s.manager.Enqueue(job); err != nil {
		syslog.L.Error(err).WithField("restoreID", id).WithMessage("Scheduler: failed to enqueue restore").Write()
	}
}

// isFailedState provides backward compatibility for legacy records that don't have
// the typed LastRunStatus field. It parses the string status to determine if
func isFailedState(state string) bool {
	return database.JobStatusFromString(state).ShouldRetry()
}

func (s *Scheduler) checkVerifications() {
	vJobs, err := s.storeInstance.Database.GetAllVerificationJobs()
	if err != nil {
		syslog.L.Error(err).WithMessage("Scheduler: failed to get verification jobs").Write()
		return
	}

	now := time.Now()

	for _, vJob := range vJobs {
		if s.manager.IsRunning(vJob.ID) {
			continue
		}

		if vJob.Schedule == "" {
			continue
		}

		ev, err := calendar.Parse(vJob.Schedule)
		if err != nil {
			continue
		}

		refTime := now
		if vJob.History.LastRunEndtime > 0 {
			refTime = time.Unix(vJob.History.LastRunEndtime, 0)
		}

		nextRun, err := calendar.ComputeNextEvent(ev, refTime, time.Local)
		if err != nil {
			continue
		}

		if nextRun.After(now) {
			continue
		}

		if now.Sub(nextRun) >= schedulerTickInterval {
			continue
		}

		syslog.L.Info().WithField("verificationJobID", vJob.ID).WithMessage("Scheduler: scheduled verification is due").Write()

		if vJob.RunOnBackupComplete {
			// Don't run yet  -  mark as pending, wait for backup completion
			if vJob.PendingSince == 0 {
				vJob.PendingSince = now.Unix()
				if err := s.storeInstance.Database.UpdateVerificationJob(nil, vJob); err != nil {
					syslog.L.Error(err).WithField("verificationJobID", vJob.ID).WithMessage("Scheduler: failed to set pending_since").Write()
				}
				syslog.L.Info().WithField("verificationJobID", vJob.ID).WithMessage("Scheduler: verification pending until backup completes").Write()
			}
			continue
		}

		job, err := verification.NewVerificationJob(vJob, s.storeInstance, false)
		if err != nil {
			syslog.L.Error(err).WithField("verificationJobID", vJob.ID).WithMessage("Scheduler: failed to create verification job").Write()
			continue
		}

		go func(id string) {
			if err := s.manager.Enqueue(job); err != nil {
				syslog.L.Error(err).WithField("verificationJobID", id).WithMessage("Scheduler: failed to enqueue verification").Write()
			}
		}(vJob.ID)
	}
}

// TriggerPendingVerifications checks for verification jobs that are pending
// (waiting for backup completion) targeting the given backup job, and enqueues them.
func (s *Scheduler) TriggerPendingVerifications(backupJobID string) {
	vJobs, err := s.storeInstance.Database.GetAllVerificationJobs()
	if err != nil {
		syslog.L.Error(err).WithMessage("TriggerPendingVerifications: failed to list verification jobs").Write()
		return
	}

	completedBackup, err := s.storeInstance.Database.GetBackup(backupJobID)
	if err != nil {
		syslog.L.Error(err).WithMessage("TriggerPendingVerifications: failed to get backup job").Write()
		return
	}

	for _, vJob := range vJobs {
		if vJob.PendingSince == 0 {
			continue
		}

		matched := false
		if vJob.TargetMode == "backup_job" && vJob.BackupJobID == backupJobID {
			matched = true
		} else if vJob.TargetMode == "namespace" {
			if vJob.Store == completedBackup.Store {
				if vJob.Recursive {
					matched = vJob.Namespace == "" || completedBackup.Namespace == vJob.Namespace || strings.HasPrefix(completedBackup.Namespace, vJob.Namespace+"/")
				} else {
					matched = completedBackup.Namespace == vJob.Namespace
				}
			}
		}

		if !matched {
			continue
		}
		if s.manager.IsRunning(vJob.ID) {
			continue
		}

		syslog.L.Info().
			WithField("verificationJobID", vJob.ID).
			WithField("backupJobID", backupJobID).
			WithMessage("backup completed, triggering pending verification").Write()

		vJob.PendingSince = 0
		if err := s.storeInstance.Database.UpdateVerificationJob(nil, vJob); err != nil {
			syslog.L.Error(err).WithField("verificationJobID", vJob.ID).WithMessage("failed to clear pending_since").Write()
			continue
		}

		job, err := verification.NewVerificationJob(vJob, s.storeInstance, false)
		if err != nil {
			syslog.L.Error(err).WithField("verificationJobID", vJob.ID).WithMessage("failed to create verification job").Write()
			continue
		}

		go func(id string) {
			if err := s.manager.Enqueue(job); err != nil {
				syslog.L.Error(err).WithField("verificationJobID", id).WithMessage("failed to enqueue verification").Write()
			}
		}(vJob.ID)
	}
}
