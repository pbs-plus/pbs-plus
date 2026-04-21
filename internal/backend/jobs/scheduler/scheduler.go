package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/backend/jobs/backup"
	"github.com/pbs-plus/pbs-plus/internal/backend/jobs/restore"
	"github.com/pbs-plus/pbs-plus/internal/calendarevent"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const schedulerTickInterval = 30 * time.Second

type Scheduler struct {
	ctx            context.Context
	cancel         context.CancelFunc
	storeInstance  *store.Store
	manager        *jobs.Manager
	lastEnqueued   map[string]time.Time // tracks last scheduled run time per backup ID
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

		// Handle calendar schedule
		if b.Schedule != "" {
			if nextRun, ok := s.shouldRunScheduled(b, now); ok {
				syslog.L.Info().WithField("backupID", b.ID).WithMessage("Scheduler: scheduled backup is due, enqueuing").Write()
				s.markEnqueued(b.ID, nextRun)
				job := backup.NewBackupJob(b, s.storeInstance, false, false, nil)
				if err := s.manager.Enqueue(job); err != nil {
					syslog.L.Error(err).WithField("backupID", b.ID).WithMessage("Scheduler: failed to enqueue scheduled backup").Write()
				}
				continue
			}
		}

		// Handle retries using typed status and persistent retry count
		if b.Retry > 0 && s.shouldRetryBackup(b, now) {
			syslog.L.Info().WithField("backupID", b.ID).WithMessage("Scheduler: backup retry is due, enqueuing").Write()
			job := backup.NewBackupJob(b, s.storeInstance, false, false, nil)
			if err := s.manager.Enqueue(job); err != nil {
				syslog.L.Error(err).WithField("backupID", b.ID).WithMessage("Scheduler: failed to enqueue backup retry").Write()
			}
		}
	}
}

// shouldRunScheduled returns the next run time and true if the backup should be
// enqueued now. It uses the later of (last enqueued time, last run start time)
// as the reference point to prevent duplicate launches.
//
// On restart, the in-memory lastEnqueued map is lost, so we guard against
// spuriously catching up on missed schedules by only enqueueing if the
// scheduled time fell within the current check interval.
func (s *Scheduler) shouldRunScheduled(b database.Backup, now time.Time) (time.Time, bool) {
	ev, err := calendarevent.Parse(b.Schedule)
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

	nextRun, err := calendarevent.ComputeNextEvent(ev, refTime, time.Local)
	if err != nil {
		return time.Time{}, false
	}

	// If nextRun is in the future, the schedule hasn't been reached yet.
	if nextRun.After(now) {
		return time.Time{}, false
	}

	// nextRun is in the past. Only enqueue if the scheduled time fell within
	// the current check interval (30s). This prevents catch-up runs on restart
	// for schedules that were missed while the service was down, while still
	// allowing legitimate runs within the current tick.
	if now.Sub(nextRun) < schedulerTickInterval {
		return nextRun, true
	}

	// The scheduled time was missed by more than one check interval.
	// Recompute from now so we know the real next future occurrence.
	futureRun, err := calendarevent.ComputeNextEvent(ev, now, time.Local)
	if err != nil {
		return time.Time{}, false
	}

	// Mark this future run as already counted so we don't re-trigger
	// on the next tick when nextRun is <= now.
	s.markEnqueued(b.ID, futureRun)

	return time.Time{}, false
}

func (s *Scheduler) markEnqueued(backupID string, t time.Time) {
	s.lastEnqueuedMu.Lock()
	s.lastEnqueued[backupID] = t
	s.lastEnqueuedMu.Unlock()
}

func (s *Scheduler) getEnqueued(backupID string) (time.Time, bool) {
	s.lastEnqueuedMu.Lock()
	t, ok := s.lastEnqueued[backupID]
	s.lastEnqueuedMu.Unlock()
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

	// Use typed status for retry decision - much more reliable than string parsing
	// Fall back to legacy string parsing for records before migration
	shouldRetry := b.History.LastRunStatus.ShouldRetry()
	if b.History.LastRunStatus == database.JobStatusUnknown {
		// For pre-migration records or uninitialized status, fall back to string parsing
		shouldRetry = isFailedState(b.History.LastRunState)
	}

	if !shouldRetry {
		return false
	}

	// Use persistent retry count - this survives restarts and log rotation
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

		// Handle retries using typed status and persistent retry count
		if r.Retry > 0 && s.shouldRetryRestore(r, now) {
			syslog.L.Info().WithField("restoreID", r.ID).WithMessage("Scheduler: restore retry is due, enqueuing").Write()
			job, err := restore.NewRestoreJob(r, s.storeInstance, false, false)
			if err != nil {
				syslog.L.Error(err).WithField("restoreID", r.ID).WithMessage("Scheduler: failed to create restore job").Write()
				continue
			}
			if err := s.manager.Enqueue(job); err != nil {
				syslog.L.Error(err).WithField("restoreID", r.ID).WithMessage("Scheduler: failed to enqueue restore retry").Write()
			}
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

	// Use typed status for retry decision
	shouldRetry := r.History.LastRunStatus.ShouldRetry()
	if r.History.LastRunStatus == database.JobStatusUnknown {
		// For pre-migration records, fall back to string parsing
		shouldRetry = isFailedState(r.History.LastRunState)
	}

	if !shouldRetry {
		return false
	}

	// Use persistent retry count
	return r.History.RetryCount < r.Retry
}

// isFailedState provides backward compatibility for legacy records that don't have
// the typed LastRunStatus field. It parses the string status to determine if
// a retry should be attempted.
func isFailedState(state string) bool {
	return database.JobStatusFromString(state).ShouldRetry()
}
