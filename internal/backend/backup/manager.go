//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Sequential queue for task monitoring (critical section)
	taskMonitorQueue chan *BackupOperation

	// Parallel execution semaphore (after task assigned)
	executionSem chan struct{}

	// Track running jobs to prevent duplicates
	mu          sync.Mutex
	runningJobs map[string]context.CancelFunc
}

func NewManager(ctx context.Context) *Manager {
	newCtx, cancel := context.WithCancel(ctx)

	m := &Manager{
		ctx:              newCtx,
		cancel:           cancel,
		taskMonitorQueue: make(chan *BackupOperation, 100),
		executionSem:     make(chan struct{}, utils.MaxConcurrentClients),
		runningJobs:      make(map[string]context.CancelFunc),
	}

	go m.processQueue()

	return m
}

func (m *Manager) Enqueue(op *BackupOperation) {
	select {
	case <-m.ctx.Done():
		m.createError(op, errors.New("manager is closed"))
		return
	default:
	}

	m.mu.Lock()
	if _, exists := m.runningJobs[op.job.ID]; exists {
		m.mu.Unlock()
		m.createError(op, ErrOneInstance)
		return
	}

	ctx, cancel := context.WithCancel(m.ctx)
	op.ctx = ctx
	op.cancel = cancel
	m.runningJobs[op.job.ID] = cancel
	m.mu.Unlock()

	queueTask, err := proxmox.GenerateQueuedTask(op.job, op.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		if err := updateJobStatus(false, 0, op.job, queueTask.Task, op.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}
	op.queueTask = &queueTask

	select {
	case m.taskMonitorQueue <- op:
	case <-m.ctx.Done():
		m.cleanup(op)
		m.createError(op, errors.New("manager closed before enqueue"))
	case <-op.ctx.Done():
		m.cleanup(op)
		m.createError(op, ErrCanceled)
	}
}

func (m *Manager) processQueue() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case op := <-m.taskMonitorQueue:
			m.runJob(op)
		}
	}
}

func (m *Manager) runJob(op *BackupOperation) {
	select {
	case <-op.ctx.Done():
		m.createError(op, ErrCanceled)
		m.cleanup(op)
		return
	default:
	}

	if err := op.PreScript(op.ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			m.createError(op, ErrCanceled)
		} else {
			m.createError(op, err)
		}
		m.cleanup(op)
		return
	}

	select {
	case m.executionSem <- struct{}{}:
	case <-m.ctx.Done():
		m.cleanup(op)
		return
	case <-op.ctx.Done():
		m.createError(op, ErrCanceled)
		m.cleanup(op)
		return
	}

	err := op.Execute(op.ctx)

	<-m.executionSem

	if err != nil {
		if errors.Is(err, ErrCanceled) || errors.Is(err, context.Canceled) {
			syslog.L.Info().WithJob(op.job.ID).WithMessage("job canceled by user").Write()
		} else if errors.Is(err, ErrMountEmpty) {
			m.createOK(op, err)
		} else {
			m.createError(op, err)
		}
		m.cleanup(op)
		return
	}

	go func() {
		op.Wait()
		m.cleanup(op)
	}()
}

func (m *Manager) StopJob(jobID string) error {
	m.mu.Lock()
	cancel, exists := m.runningJobs[jobID]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("no such job: %s", jobID)
	}

	cancel()
	return nil
}

func (m *Manager) createError(op *BackupOperation, err error) {
	syslog.L.Error(err).WithField("jobId", op.job.ID).Write()

	if !errors.Is(err, ErrOneInstance) {
		task, terr := proxmox.GenerateTaskErrorFile(
			op.job,
			err,
			[]string{
				"Error handling from a scheduled job run request",
				"Job ID: " + op.job.ID,
				"Source Mode: " + op.job.SourceMode,
			},
		)
		if terr != nil {
			syslog.L.Error(terr).WithField("jobId", op.job.ID).Write()
		} else {
			m.updateJobWithTask(op, task)
		}

		if rerr := system.SetRetrySchedule(op.job, op.extraExclusions); rerr != nil {
			syslog.L.Error(rerr).WithField("jobId", op.job.ID).Write()
		}
	}
}

func (m *Manager) createOK(op *BackupOperation, err error) {
	task, terr := proxmox.GenerateTaskOKFile(
		op.job,
		[]string{
			"Done handling from a job run request",
			"Job ID: " + op.job.ID,
			"Source Mode: " + op.job.SourceMode,
			"Response: " + err.Error(),
		},
	)
	if terr != nil {
		syslog.L.Error(terr).WithField("jobId", op.job.ID).Write()
		return
	}

	latest, gerr := op.storeInstance.Database.GetJob(op.job.ID)
	if gerr != nil {
		latest = op.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime
	latest.LastSuccessfulEndtime = task.EndTime
	latest.LastSuccessfulUpid = task.UPID

	if uerr := op.storeInstance.Database.UpdateJob(nil, latest); uerr != nil {
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}

func (m *Manager) updateJobWithTask(op *BackupOperation, task proxmox.Task) {
	latest, gerr := op.storeInstance.Database.GetJob(op.job.ID)
	if gerr != nil {
		latest = op.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime

	if uerr := op.storeInstance.Database.UpdateJob(nil, latest); uerr != nil {
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}

func (m *Manager) cleanup(op *BackupOperation) {
	if op.queueTask != nil {
		op.queueTask.Close()
	}

	m.mu.Lock()
	delete(m.runningJobs, op.job.ID)
	m.mu.Unlock()
}

func (m *Manager) Close() {
	m.cancel()
}
