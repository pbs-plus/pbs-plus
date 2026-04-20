package scheduler

import (
	"context"
	"strings"
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
	go s.run()
}

func (s *Scheduler) run() {
	ticker := time.NewTicker(30 * time.Second)
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
				op := backup.NewBackupOperation(b, s.storeInstance, false, false, nil)
				if err := s.manager.Enqueue(op); err != nil {
					syslog.L.Error(err).WithField("backupID", b.ID).WithMessage("Scheduler: failed to enqueue scheduled backup").Write()
				}
				continue
			}
		}

		// Handle retries (only for actual failures, not successes-with-warnings)
		if b.Retry > 0 && isFailedState(b.History.LastRunState) {
			if s.shouldRetryBackup(b, now) {
				syslog.L.Info().WithField("backupID", b.ID).WithMessage("Scheduler: backup retry is due, enqueuing").Write()
				op := backup.NewBackupOperation(b, s.storeInstance, false, false, nil)
				if err := s.manager.Enqueue(op); err != nil {
					syslog.L.Error(err).WithField("backupID", b.ID).WithMessage("Scheduler: failed to enqueue backup retry").Write()
				}
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
	if now.Sub(nextRun) < 30*time.Second {
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

	// Count failed attempts since the last SUCCESSFUL run.
	// Only count actual failures; runs that succeeded with warnings are not failures.
	upids := b.GetAllUPIDs()
	lastSuccessTime := b.History.LastSuccessfulEndtime
	count := 0
	for _, t := range upids {
		if t.Endtime > lastSuccessTime && isFailedState(t.Status) {
			count++
		}
	}

	return count <= b.Retry
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

		// Handle retries (only for actual failures, not successes-with-warnings)
		if r.Retry > 0 && isFailedState(r.History.LastRunState) {
			if s.shouldRetryRestore(r, now) {
				syslog.L.Info().WithField("restoreID", r.ID).WithMessage("Scheduler: restore retry is due, enqueuing").Write()
				op, err := restore.NewRestoreOperation(r, s.storeInstance, false, false)
				if err != nil {
					syslog.L.Error(err).WithField("restoreID", r.ID).WithMessage("Scheduler: failed to create restore operation").Write()
					continue
				}
				if err := s.manager.Enqueue(op); err != nil {
					syslog.L.Error(err).WithField("restoreID", r.ID).WithMessage("Scheduler: failed to enqueue restore retry").Write()
				}
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

	// Count failed attempts since the last SUCCESSFUL run.
	// Only count actual failures; runs that succeeded with warnings are not failures.
	upids := r.GetAllUPIDs()
	lastSuccessTime := r.History.LastSuccessfulEndtime
	count := 0
	for _, t := range upids {
		if t.Endtime > lastSuccessTime && isFailedState(t.Status) {
			count++
		}
	}

	return count <= r.Retry
}

// isFailedState returns true if the given task state represents an actual
// failure, as opposed to a success ("OK"), a success with warnings
// ("WARNINGS: N"), or a cancellation ("operation canceled"). Empty state is
// treated as non-failure so that an uninitialised history never triggers a retry.
func isFailedState(state string) bool {
	if state == "" || state == "OK" {
		return false
	}
	if strings.HasPrefix(state, "WARNINGS: ") {
		return false
	}
	// Manual stop/cancellation should not trigger retries
	if state == "operation canceled" {
		return false
	}
	return true
}
