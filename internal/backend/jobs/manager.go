package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrManagerClosed = errors.New("manager is closed")
	ErrOneInstance   = errors.New("a job is still running; only one instance allowed")
	ErrCanceled      = errors.New("operation canceled")
)

type Operation interface {
	GetID() string
	PreExecute() error
	Execute() error
	OnError(err error)
	OnSuccess()
	Cleanup()
	Wait() error
	SetContext(ctx context.Context, cancel context.CancelFunc)
	Context() context.Context
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	taskMonitorQueue chan Operation
	executionSem     chan struct{}

	mu              sync.Mutex
	detectionMu     sync.Mutex
	singleExecution bool
	runningJobs     map[string]context.CancelFunc
}

func NewManager(ctx context.Context, maxConcurrent int, queueSize int, singleExecution bool) *Manager {
	newCtx, cancel := context.WithCancel(ctx)

	m := &Manager{
		ctx:              newCtx,
		cancel:           cancel,
		taskMonitorQueue: make(chan Operation, queueSize),
		executionSem:     make(chan struct{}, maxConcurrent),
		runningJobs:      make(map[string]context.CancelFunc),
		singleExecution:  singleExecution,
	}

	go m.processQueue()

	return m
}

func (m *Manager) Enqueue(op Operation) error {
	select {
	case <-m.ctx.Done():
		return ErrManagerClosed
	default:
	}

	jobID := op.GetID()

	m.mu.Lock()
	if _, exists := m.runningJobs[jobID]; exists {
		m.mu.Unlock()
		op.OnError(ErrOneInstance)
		return ErrOneInstance
	}

	ctx, cancel := context.WithCancel(m.ctx)
	m.runningJobs[jobID] = cancel
	m.mu.Unlock()

	op.SetContext(ctx, cancel)

	select {
	case m.taskMonitorQueue <- op:
		return nil
	case <-m.ctx.Done():
		m.cleanup(jobID)
		op.OnError(ErrManagerClosed)
		return ErrManagerClosed
	case <-ctx.Done():
		m.cleanup(jobID)
		op.OnError(ErrCanceled)
		return ErrCanceled
	}
}

func (m *Manager) processQueue() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case op := <-m.taskMonitorQueue:
			go m.runJob(op)
		}
	}
}

func (m *Manager) runJob(op Operation) {
	jobID := op.GetID()

	select {
	case <-op.Context().Done():
		op.OnError(ErrCanceled)
		m.cleanup(jobID)
		return
	default:
	}

	if err := op.PreExecute(); err != nil {
		if errors.Is(err, context.Canceled) {
			op.OnError(ErrCanceled)
		} else {
			op.OnError(err)
		}
		m.cleanup(jobID)
		return
	}

	select {
	case m.executionSem <- struct{}{}:
	case <-m.ctx.Done():
		m.cleanup(jobID)
		return
	case <-op.Context().Done():
		op.OnError(ErrCanceled)
		m.cleanup(jobID)
		return
	}

	if m.singleExecution {
		m.detectionMu.Lock()
	}

	if err := op.Execute(); err != nil {
		if errors.Is(err, context.Canceled) {
			op.OnError(ErrCanceled)
		} else {
			op.OnError(err)
		}

		if m.singleExecution {
			m.detectionMu.Unlock()
		}
		m.cleanup(jobID)
		<-m.executionSem
		return
	}

	if m.singleExecution {
		m.detectionMu.Unlock()
	}

	go func() {
		if err := op.Wait(); err != nil {
			if errors.Is(err, context.Canceled) {
				op.OnError(ErrCanceled)
			} else {
				op.OnError(err)
			}
			m.cleanup(jobID)
			<-m.executionSem
			return
		}

		op.OnSuccess()
		m.cleanup(jobID)
		<-m.executionSem
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

func (m *Manager) cleanup(jobID string) {
	m.mu.Lock()
	delete(m.runningJobs, jobID)
	m.mu.Unlock()
}

func (m *Manager) IsRunning(jobID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.runningJobs[jobID]
	return exists
}

func (m *Manager) RunningCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.runningJobs)
}

func (m *Manager) Close() {
	m.cancel()
}
