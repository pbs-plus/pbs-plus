//go:build linux

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

// Operation defines the interface that any job type must implement
type Operation interface {
	GetID() string
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	OnError(err error)
	OnSuccess()
	Cleanup()
}

// Manager handles queuing and execution of operations
type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Sequential queue for task monitoring (critical section)
	taskMonitorQueue chan Operation

	// Parallel execution semaphore (after task assigned)
	executionSem chan struct{}

	// Track running jobs to prevent duplicates
	mu          sync.Mutex
	runningJobs map[string]context.CancelFunc
}

// NewManager creates a new job manager with the specified max concurrent operations
func NewManager(ctx context.Context, maxConcurrent int, queueSize int) *Manager {
	newCtx, cancel := context.WithCancel(ctx)

	m := &Manager{
		ctx:              newCtx,
		cancel:           cancel,
		taskMonitorQueue: make(chan Operation, queueSize),
		executionSem:     make(chan struct{}, maxConcurrent),
		runningJobs:      make(map[string]context.CancelFunc),
	}

	go m.processQueue()

	return m
}

// Enqueue adds an operation to the queue
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

	// Store context for the operation if it needs it
	if ctxAware, ok := op.(interface {
		SetContext(context.Context, context.CancelFunc)
	}); ok {
		ctxAware.SetContext(ctx, cancel)
	}

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
			m.runJob(op)
		}
	}
}

func (m *Manager) runJob(op Operation) {
	jobID := op.GetID()

	// Check if context is aware and if it's already canceled
	if ctxAware, ok := op.(interface{ Context() context.Context }); ok {
		select {
		case <-ctxAware.Context().Done():
			op.OnError(ErrCanceled)
			m.cleanup(jobID)
			return
		default:
		}
	}

	// Run pre-execution hook
	if ctxAware, ok := op.(interface{ Context() context.Context }); ok {
		if err := op.PreExecute(ctxAware.Context()); err != nil {
			if errors.Is(err, context.Canceled) {
				op.OnError(ErrCanceled)
			} else {
				op.OnError(err)
			}
			m.cleanup(jobID)
			return
		}
	} else {
		if err := op.PreExecute(m.ctx); err != nil {
			op.OnError(err)
			m.cleanup(jobID)
			return
		}
	}

	// Acquire execution semaphore
	var ctx context.Context
	if ctxAware, ok := op.(interface{ Context() context.Context }); ok {
		ctx = ctxAware.Context()
	} else {
		ctx = m.ctx
	}

	select {
	case m.executionSem <- struct{}{}:
	case <-m.ctx.Done():
		m.cleanup(jobID)
		return
	case <-ctx.Done():
		op.OnError(ErrCanceled)
		m.cleanup(jobID)
		return
	}

	// Execute the operation
	err := op.Execute(ctx)

	// Release semaphore
	<-m.executionSem

	if err != nil {
		if errors.Is(err, ErrCanceled) || errors.Is(err, context.Canceled) {
			op.OnError(ErrCanceled)
		} else {
			op.OnError(err)
		}
		m.cleanup(jobID)
		return
	}

	op.OnSuccess()

	// Handle async cleanup if operation supports it
	if waitable, ok := op.(interface{ Wait() }); ok {
		go func() {
			waitable.Wait()
			m.cleanup(jobID)
		}()
	} else {
		m.cleanup(jobID)
	}
}

// StopJob cancels a running job by its ID
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

// IsRunning checks if a job is currently running
func (m *Manager) IsRunning(jobID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.runningJobs[jobID]
	return exists
}

// RunningCount returns the number of currently running jobs
func (m *Manager) RunningCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.runningJobs)
}

// Close shuts down the manager
func (m *Manager) Close() {
	m.cancel()
}
