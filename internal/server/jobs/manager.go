package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/safemap"
)

var (
	ErrManagerClosed = errors.New("manager is closed")
	ErrCanceled      = errors.New("operation canceled")
	ErrOneInstance   = errors.New("a job is still running; only one instance allowed")
)

// Job represents a unit of work with lifecycle callbacks.
type Job struct {
	ID        string
	PreExec   func(ctx context.Context) error
	Execute   func(ctx context.Context) error
	OnSuccess func()
	OnError   func(err error)
	Cleanup   func()
}

type contextPair struct {
	cancel context.CancelFunc
	ctx    context.Context
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	executionSem chan struct{}

	enqueueMu   sync.Mutex
	startupMu   sync.Mutex
	runningJobs *safemap.Map[string, contextPair]

	// Dynamic queue: slice-based queue with condition variable.
	// capacityFn returns the maximum number of queued jobs allowed at any moment.
	// It is re-evaluated on every enqueue, so the limit stays in sync with the
	// database as jobs are created or deleted.
	queueMu    sync.Mutex
	queueCond  *sync.Cond
	queue      []*Job
	capacityFn func() int
}

func NewManager(ctx context.Context, maxConcurrent int, capacityFn func() int) *Manager {
	newCtx, cancel := context.WithCancel(ctx)

	m := &Manager{
		ctx:          newCtx,
		cancel:       cancel,
		executionSem: make(chan struct{}, maxConcurrent),
		runningJobs:  safemap.New[string, contextPair](),
		capacityFn:   capacityFn,
	}
	m.queueCond = sync.NewCond(&m.queueMu)

	go m.processQueue()

	return m
}

func (m *Manager) Enqueue(job *Job) error {
	select {
	case <-m.ctx.Done():
		return ErrManagerClosed
	default:
	}

	jobID := job.ID

	m.enqueueMu.Lock()
	if _, exists := m.runningJobs.Get(jobID); exists {
		m.enqueueMu.Unlock()
		return fmt.Errorf("Job %s is already running/in queue.", jobID)
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.runningJobs.Set(jobID, contextPair{ctx: ctx, cancel: cancel})
	m.enqueueMu.Unlock()

	// Wait until the queue has room, respecting the dynamic capacity.
	pushed := make(chan struct{})
	go func() {
		m.queueMu.Lock()
		defer m.queueMu.Unlock()

		for !m.isClosed() {
			cap := m.capacityFn()
			if len(m.queue) < cap {
				m.queue = append(m.queue, job)
				m.queueCond.Signal()
				close(pushed)
				return
			}
			m.queueCond.Wait()
		}
	}()

	select {
	case <-pushed:
		return nil
	case <-m.ctx.Done():
		m.cleanup(job, cancel)
		if job.OnError != nil {
			job.OnError(ErrManagerClosed)
		}
		return ErrManagerClosed
	case <-ctx.Done():
		m.cleanup(job, cancel)
		if job.OnError != nil {
			job.OnError(ErrCanceled)
		}
		return ErrCanceled
	}
}

func (m *Manager) processQueue() {
	for {
		m.queueMu.Lock()
		for len(m.queue) == 0 && !m.isClosed() {
			m.queueCond.Wait()
		}
		if m.isClosed() {
			m.queueMu.Unlock()
			return
		}
		job := m.queue[0]
		m.queue = m.queue[1:]
		m.queueCond.Broadcast() // wake up any enqueuers waiting for capacity
		m.queueMu.Unlock()

		go m.runJob(job)
	}
}

func (m *Manager) runJob(job *Job) {
	pair, exists := m.runningJobs.Get(job.ID)
	if !exists {
		return
	}
	ctx := pair.ctx
	cancel := pair.cancel

	select {
	case <-ctx.Done():
		if job.OnError != nil {
			job.OnError(ErrCanceled)
		}
		m.cleanup(job, cancel)
		return
	default:
	}

	if job.PreExec != nil {
		if err := job.PreExec(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				if job.OnError != nil {
					job.OnError(ErrCanceled)
				}
			} else {
				if job.OnError != nil {
					job.OnError(err)
				}
			}
			m.cleanup(job, cancel)
			return
		}
	}

	select {
	case m.executionSem <- struct{}{}:
	case <-m.ctx.Done():
		m.cleanup(job, cancel)
		return
	case <-ctx.Done():
		if job.OnError != nil {
			job.OnError(ErrCanceled)
		}
		m.cleanup(job, cancel)
		return
	}

	err := job.Execute(ctx)

	if err != nil {
		if errors.Is(err, context.Canceled) {
			if job.OnError != nil {
				job.OnError(ErrCanceled)
			}
		} else {
			if job.OnError != nil {
				job.OnError(err)
			}
		}
		m.cleanup(job, cancel)
		<-m.executionSem
		return
	}

	if job.OnSuccess != nil {
		job.OnSuccess()
	}
	m.cleanup(job, cancel)
	<-m.executionSem
}

func (m *Manager) StopJob(jobID string) error {
	pair, exists := m.runningJobs.Get(jobID)
	if !exists {
		return fmt.Errorf("Job %s is not running", jobID)
	}

	if pair.ctx.Err() != nil {
		return fmt.Errorf("Job %s is currently stopping and cleaning up", jobID)
	}

	pair.cancel()
	return nil
}

func (m *Manager) cleanup(job *Job, cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	if job.Cleanup != nil {
		job.Cleanup()
	}
	m.runningJobs.Del(job.ID)
}

func (m *Manager) IsRunning(jobID string) bool {
	_, exists := m.runningJobs.Get(jobID)
	return exists
}

func (m *Manager) RunningCount() int {
	return m.runningJobs.Len()
}

func (m *Manager) Close() {
	m.cancel()
}

func (m *Manager) StartupMu() *sync.Mutex {
	return &m.startupMu
}

func (m *Manager) isClosed() bool {
	select {
	case <-m.ctx.Done():
		return true
	default:
		return false
	}
}
