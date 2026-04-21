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

	taskMonitorQueue chan *Job
	executionSem     chan struct{}

	detectionMu     sync.Mutex
	singleExecution bool
	runningJobs     *safemap.Map[string, contextPair]
}

func NewManager(ctx context.Context, maxConcurrent int, queueSize int, singleExecution bool) *Manager {
	newCtx, cancel := context.WithCancel(ctx)

	m := &Manager{
		ctx:              newCtx,
		cancel:           cancel,
		taskMonitorQueue: make(chan *Job, queueSize),
		executionSem:     make(chan struct{}, maxConcurrent),
		runningJobs:      safemap.New[string, contextPair](),
		singleExecution:  singleExecution,
	}

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

	m.detectionMu.Lock()
	defer m.detectionMu.Unlock()

	if _, exists := m.runningJobs.Get(jobID); exists {
		return fmt.Errorf("Job %s is already running/in queue.", jobID)
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.runningJobs.Set(jobID, contextPair{ctx: ctx, cancel: cancel})

	select {
	case m.taskMonitorQueue <- job:
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
		select {
		case <-m.ctx.Done():
			return
		case job := <-m.taskMonitorQueue:
			go m.runJob(job)
		}
	}
}

func (m *Manager) runJob(job *Job) {
	pair, exists := m.runningJobs.Get(job.ID)
	if !exists {
		// Job was cancelled before starting
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

	if m.singleExecution {
		m.detectionMu.Lock()
	}

	// Execute the job
	err := job.Execute(ctx)

	if m.singleExecution {
		m.detectionMu.Unlock()
	}

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
