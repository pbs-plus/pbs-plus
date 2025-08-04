//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/puzpuzpuz/xsync/v3"
)

type Manager struct {
	ctx           context.Context
	cancel        context.CancelFunc
	jobs          chan *BackupOperation
	mu            sync.Mutex
	executeMu     sync.Mutex
	locks         *xsync.MapOf[string, *sync.Mutex]
	jobCancels    *xsync.MapOf[string, context.CancelFunc]
	cancelledJobs *xsync.MapOf[string, struct{}]

	runningJobs atomic.Int32

	maxConcurrentJobs int
	semaphore         chan struct{}
}

func NewManager(ctx context.Context, size int) *Manager {
	newCtx, cancel := context.WithCancel(ctx)
	jq := &Manager{
		ctx:               newCtx,
		cancel:            cancel,
		jobs:              make(chan *BackupOperation, size),
		mu:                sync.Mutex{},
		locks:             xsync.NewMapOf[string, *sync.Mutex](),
		jobCancels:        xsync.NewMapOf[string, context.CancelFunc](),
		cancelledJobs:     xsync.NewMapOf[string, struct{}](),
		maxConcurrentJobs: utils.MaxConcurrentClients,
		semaphore:         make(chan struct{}, utils.MaxConcurrentClients),
	}

	go jq.worker()

	return jq
}

func (jq *Manager) Enqueue(job *BackupOperation) {
	ctx, cancel := context.WithCancel(jq.ctx)
	job.ctx = ctx
	job.cancel = cancel
	jq.jobCancels.Store(job.job.ID, cancel)

	lock, _ := jq.locks.LoadOrStore(job.job.ID, &sync.Mutex{})

	if locked := lock.TryLock(); !locked {
		jq.CreateError(job, ErrOneInstance)
		return
	}

	job.lock = lock

	queueTask, err := proxmox.GenerateQueuedTask(job.job, job.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		if err := updateJobStatus(false, 0, job.job, queueTask.Task, job.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}

	job.queueTask = &queueTask

	jq.mu.Lock()
	defer jq.mu.Unlock()

	if jq.ctx.Err() != nil {
		job.cancel()
		jq.jobCancels.Delete(job.job.ID)
		jq.CreateError(job, fmt.Errorf("Attempt to enqueue on closed queue"))
		return
	}

	select {
	case jq.jobs <- job:
	default:
		job.cancel()
		jq.jobCancels.Delete(job.job.ID)
		jq.CreateError(job, fmt.Errorf("Queue is full. Job rejected."))
		job.lock.Unlock()
	}
}

func (jq *Manager) worker() {
	for {
		select {
		case <-jq.ctx.Done():
			return
		case op := <-jq.jobs:
			// If user canceled while it was still queued:
			if _, canceled := jq.cancelledJobs.Load(op.job.ID); canceled {
				op.queueTask.Close()
				op.lock.Unlock()
				jq.cancelledJobs.Delete(op.job.ID)
				jq.jobCancels.Delete(op.job.ID)
				continue
			}

			go func(opToRun *BackupOperation) {
				err := opToRun.PreScript(opToRun.ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						jq.CreateError(opToRun, errors.New("task aborted"))
						return
					}
					syslog.L.Error(err).WithJob(opToRun.job.ID).WithMessage("error occurred during prescript execution, proceeding to backup operation").Write()
				}

				select {
				case <-jq.ctx.Done():
					opToRun.lock.Unlock()
					return
				case jq.semaphore <- struct{}{}:
				}

				defer func() {
					<-jq.semaphore
					opToRun.queueTask.Close()
					jq.runningJobs.Add(-1)
					jq.jobCancels.Delete(opToRun.job.ID)
					jq.cancelledJobs.Delete(opToRun.job.ID)
				}()

				jq.runningJobs.Add(1)

				// Acquire lock for sequential execution
				jq.executeMu.Lock()
				err = opToRun.Execute(opToRun.ctx)
				// Release lock immediately after Execute finishes
				jq.executeMu.Unlock()

				if err != nil {
					if errors.Is(err, context.Canceled) {
						syslog.L.Info().
							WithJob(opToRun.job.ID).
							WithMessage("job canceled by user").
							Write()
					} else {
						jq.CreateError(opToRun, err)
					}
				} else {
					opToRun.Wait()
				}
			}(op)
		}
	}
}

func (jq *Manager) StopJob(jobID string) error {
	// mark it canceled (so queued ones are dropped)
	jq.cancelledJobs.Store(jobID, struct{}{})

	cancelFunc, ok := jq.jobCancels.Load(jobID)
	if !ok {
		return fmt.Errorf("no such job: %s", jobID)
	}
	cancelFunc()
	return nil
}

func (jq *Manager) CreateError(op *BackupOperation, err error) {
	syslog.L.Error(err).WithField("jobId", op.job.ID).Write()

	if !errors.Is(err, ErrOneInstance) {
		if task, err := proxmox.GenerateTaskErrorFile(op.job, err, []string{"Error handling from a scheduled job run request", "Job ID: " + op.job.ID, "Source Mode: " + op.job.SourceMode}); err != nil {
			syslog.L.Error(err).WithField("jobId", op.job.ID).Write()
		} else {
			// Update job status
			latestJob, err := op.storeInstance.Database.GetJob(op.job.ID)
			if err != nil {
				latestJob = op.job
			}

			latestJob.LastRunUpid = task.UPID
			latestJob.LastRunState = task.Status
			latestJob.LastRunEndtime = task.EndTime

			err = op.storeInstance.Database.UpdateJob(nil, latestJob)
			if err != nil {
				syslog.L.Error(err).WithField("jobId", latestJob.ID).WithField("upid", task.UPID).Write()
			}
		}
		if err := system.SetRetrySchedule(op.job, op.extraExclusions); err != nil {
			syslog.L.Error(err).WithField("jobId", op.job.ID).Write()
		}
	}
}

// Close waits for the worker to finish and closes the job queue.
func (jq *Manager) Close() {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	jq.locks.Clear()
	jq.cancel()
}
