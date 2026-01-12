package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

var (
	ErrManagerClosed = errors.New("manager is closed")
	ErrCanceled      = errors.New("operation canceled")
	ErrOneInstance   = errors.New("a job is still running; only one instance allowed")
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

type contextPair struct {
	cancel context.CancelFunc
	ctx    context.Context
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	taskMonitorQueue chan Operation
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
		taskMonitorQueue: make(chan Operation, queueSize),
		executionSem:     make(chan struct{}, maxConcurrent),
		runningJobs:      safemap.New[string, contextPair](),
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

	if _, exists := m.runningJobs.Get(jobID); exists {
		return fmt.Errorf("Job %s is already running/in queue.", jobID)
	}

	ctx, cancel := context.WithCancel(m.ctx)
	m.runningJobs.Set(jobID, contextPair{ctx: ctx, cancel: cancel})

	op.SetContext(ctx, cancel)

	select {
	case m.taskMonitorQueue <- op:
		return nil
	case <-m.ctx.Done():
		m.cleanup(op)
		op.OnError(ErrManagerClosed)
		return ErrManagerClosed
	case <-ctx.Done():
		m.cleanup(op)
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
	select {
	case <-op.Context().Done():
		op.OnError(ErrCanceled)
		m.cleanup(op)
		return
	default:
	}

	if err := op.PreExecute(); err != nil {
		if errors.Is(err, context.Canceled) {
			op.OnError(ErrCanceled)
		} else {
			op.OnError(err)
		}
		m.cleanup(op)
		return
	}

	select {
	case m.executionSem <- struct{}{}:
	case <-m.ctx.Done():
		m.cleanup(op)
		return
	case <-op.Context().Done():
		op.OnError(ErrCanceled)
		m.cleanup(op)
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
		m.cleanup(op)
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
			m.cleanup(op)
			<-m.executionSem
			return
		}

		op.OnSuccess()
		m.cleanup(op)
		<-m.executionSem
	}()
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

func (m *Manager) cleanup(op Operation) {
	op.Cleanup()
	m.runningJobs.Del(op.GetID())
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
