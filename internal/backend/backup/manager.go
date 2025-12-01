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

// Unbounded queue for *BackupOperation using mutex + notify channel.
type opQueue struct {
	mu     sync.Mutex
	buf    []*BackupOperation
	head   int
	notify chan struct{} // broadcast-ish via try-send; receivers loop
}

func newOpQueue() *opQueue {
	return &opQueue{
		buf:    make([]*BackupOperation, 0, 64),
		notify: make(chan struct{}, 1),
	}
}

func (q *opQueue) push(op *BackupOperation) {
	q.mu.Lock()
	q.buf = append(q.buf, op)
	q.mu.Unlock()
	// Try to notify a waiter; drop if already signaled
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *opQueue) pop(ctx context.Context) (*BackupOperation, bool) {
	for {
		q.mu.Lock()
		if q.head < len(q.buf) {
			op := q.buf[q.head]
			q.head++
			// occasional compaction
			if q.head > 1024 && q.head*2 >= len(q.buf) {
				q.buf = append([]*BackupOperation(nil), q.buf[q.head:]...)
				q.head = 0
			}
			q.mu.Unlock()
			return op, true
		}
		q.mu.Unlock()

		// Wait for either a push notification or context cancellation
		select {
		case <-ctx.Done():
			return nil, false
		case <-q.notify:
			// loop to recheck buffer under lock
		}
	}
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Unbounded admission queue
	admitQ *opQueue

	// Bounded inner run queue: capacity = MaxConcurrentClients
	runQ chan *BackupOperation

	// Only one Execute start at a time
	executeMu sync.Mutex

	// Limit concurrent Execute runs
	sem chan struct{}

	// Cancel registry by job ID
	cancels sync.Map // map[string]context.CancelFunc

	// Set of job IDs that are admitted (in runQ or running)
	mu      sync.Mutex
	present map[string]struct{}
}

func NewManager(ctx context.Context) *Manager {
	newCtx, cancel := context.WithCancel(ctx)
	cap := utils.MaxConcurrentClients

	m := &Manager{
		ctx:     newCtx,
		cancel:  cancel,
		admitQ:  newOpQueue(),
		runQ:    make(chan *BackupOperation, cap),
		sem:     make(chan struct{}, cap),
		present: make(map[string]struct{}),
	}

	go m.admitter()
	go m.runner()

	return m
}

// Enqueue: push to unbounded admission queue; rejects only if closed.
func (m *Manager) Enqueue(op *BackupOperation) {
	select {
	case <-m.ctx.Done():
		m.CreateError(op, fmt.Errorf("attempt to enqueue on closed queue"))
		return
	default:
	}
	m.admitQ.push(op)
}

func (m *Manager) admitter() {
	for {
		op, ok := m.admitQ.pop(m.ctx)
		if !ok {
			return
		}

		// Enforce single instance per job ID
		m.mu.Lock()
		if _, dup := m.present[op.job.ID]; dup {
			m.mu.Unlock()
			m.CreateError(op, ErrOneInstance)
			continue
		}
		m.present[op.job.ID] = struct{}{}
		m.mu.Unlock()

		// Prepare per-job context/cancel
		ctx, cancel := context.WithCancel(m.ctx)
		op.ctx = ctx
		op.cancel = cancel
		m.cancels.Store(op.job.ID, cancel)

		// Best-effort queue task + status
		queueTask, err := proxmox.GenerateQueuedTask(op.job, op.web)
		if err != nil {
			syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
		} else {
			if err := updateJobStatus(false, 0, op.job, queueTask.Task, op.storeInstance); err != nil {
				syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
			}
		}
		op.queueTask = &queueTask

		// Forward to bounded run queue (size == MaxConcurrentClients)
		select {
		case <-m.ctx.Done():
			cancel()
			m.cancels.Delete(op.job.ID)
			m.dropPresence(op.job.ID)
			m.CreateError(op, fmt.Errorf("manager closed before scheduling"))
		case m.runQ <- op:
		}
	}
}

func (m *Manager) runner() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case op := <-m.runQ:
			go m.runOne(op)
		}
	}
}

func (m *Manager) runOne(op *BackupOperation) {
	// If PreScript must be serialized too, move under executeMu.
	if err := op.PreScript(op.ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			m.CreateError(op, errors.New("task aborted"))
			m.cleanup(op)
			return
		}

		m.CreateError(op, err)
		m.cleanup(op)
		return
	}

	// Acquire concurrency slot or abort
	select {
	case m.sem <- struct{}{}:
	case <-m.ctx.Done():
		m.cleanup(op)
		return
	case <-op.ctx.Done():
		m.cleanup(op)
		return
	}
	defer func() {
		<-m.sem
		m.cleanup(op)
	}()

	// Only one Execute at a time
	m.executeMu.Lock()
	err := op.Execute(op.ctx)
	m.executeMu.Unlock()

	if err != nil {
		if errors.Is(err, context.Canceled) {
			syslog.L.Info().
				WithJob(op.job.ID).
				WithMessage("job canceled by user").
				Write()
			return
		}
		if errors.Is(err, ErrMountEmpty) {
			m.CreateOK(op, err)
			return
		}
		m.CreateError(op, err)
		return
	}

	op.Wait()
}

func (m *Manager) StopJob(jobID string) error {
	v, ok := m.cancels.Load(jobID)
	if !ok {
		return fmt.Errorf("no such job: %s", jobID)
	}
	cancel := v.(context.CancelFunc)
	cancel()
	return nil
}

func (m *Manager) CreateError(op *BackupOperation, err error) {
	syslog.L.Error(err).WithField("jobId", op.job.ID).Write()

	if !errors.Is(err, ErrOneInstance) {
		if task, terr := proxmox.GenerateTaskErrorFile(
			op.job,
			err,
			[]string{
				"Error handling from a scheduled job run request",
				"Job ID: " + op.job.ID,
				"Source Mode: " + op.job.SourceMode,
			},
		); terr != nil {
			syslog.L.Error(terr).WithField("jobId", op.job.ID).Write()
		} else {
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
		if rerr := system.SetRetrySchedule(op.job, op.extraExclusions); rerr != nil {
			syslog.L.Error(rerr).WithField("jobId", op.job.ID).Write()
		}
	}
}

func (m *Manager) CreateOK(op *BackupOperation, err error) {
	if task, terr := proxmox.GenerateTaskOKFile(
		op.job,
		[]string{
			"Done handling from a job run request",
			"Job ID: " + op.job.ID,
			"Source Mode: " + op.job.SourceMode,
			"Response: " + err.Error(),
		},
	); terr != nil {
		syslog.L.Error(terr).WithField("jobId", op.job.ID).Write()
	} else {
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
}

func (m *Manager) cleanup(op *BackupOperation) {
	if op.queueTask != nil {
		op.queueTask.Close()
	}
	m.cancels.Delete(op.job.ID)
	m.dropPresence(op.job.ID)
}

func (m *Manager) dropPresence(jobID string) {
	m.mu.Lock()
	delete(m.present, jobID)
	m.mu.Unlock()
}

func (m *Manager) Close() {
	m.cancel()
}
