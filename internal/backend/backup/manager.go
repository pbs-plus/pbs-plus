//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/puzpuzpuz/xsync/v3"
)

type Manager struct {
	ctx         context.Context
	cancel      context.CancelFunc
	jobs        chan *BackupOperation
	mu          sync.Mutex
	locks       *xsync.MapOf[string, *sync.Mutex]
	runningJobs atomic.Int32

	maxConcurrentJobs int
	semaphore         chan struct{}
}

func NewManager(ctx context.Context, size int, maxConcurrent int) *Manager {
	if maxConcurrent <= 0 {
		maxConcurrent = 1 // Ensure at least one job can run
	}

	newCtx, cancel := context.WithCancel(ctx)
	jq := &Manager{
		ctx:               newCtx,
		cancel:            cancel,
		jobs:              make(chan *BackupOperation, size),
		mu:                sync.Mutex{},
		locks:             xsync.NewMapOf[string, *sync.Mutex](),
		maxConcurrentJobs: maxConcurrent,
		semaphore:         make(chan struct{}, maxConcurrent),
	}

	go jq.worker()

	return jq
}

func (jq *Manager) Enqueue(job *BackupOperation) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if jq.ctx.Err() != nil {
		jq.CreateError(job, fmt.Errorf("Attempt to enqueue on closed queue"))
		return
	}

	var err error
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
		queueTaskLogPath, err := proxmox.GetLogPath(queueTask.UPID)
		if err == nil {
			defer os.Remove(queueTaskLogPath)
		}

		if err := updateJobStatus(false, 0, job.job, queueTask, job.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}

	select {
	case jq.jobs <- job:
	default:
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
			go func(opToRun *BackupOperation) {
				select {
				case <-jq.ctx.Done():
					opToRun.lock.Unlock()
					return
				case jq.semaphore <- struct{}{}:
				}

				defer func() {
					<-jq.semaphore
				}()

				jq.runningJobs.Add(1)
				defer jq.runningJobs.Add(-1)

				err := opToRun.Execute(jq.ctx)
				if err != nil {
					jq.CreateError(opToRun, err)
				} else {
					opToRun.Wait()
				}
			}(op)
		}
	}
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
