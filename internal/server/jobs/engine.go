//go:build linux

package jobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

type EngineConfig struct {
	Owner         string
	LeaseDuration time.Duration
	PollInterval  time.Duration
	MaxConcurrent int
}

type Engine struct {
	database      *database.Database
	owner         string
	leaseDuration time.Duration
	pollInterval  time.Duration
	slots         chan struct{}
	wake          chan struct{}

	runnersMu sync.RWMutex
	runners   map[string]Workflow
	running   sync.Map
	startupMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Workflow func(*WorkflowContext) error

type Activity func(context.Context, ActivityInfo) (json.RawMessage, error)

type ActivityInfo struct {
	Execution        database.WorkflowExecution
	Name             string
	ResumeCheckpoint json.RawMessage
	checkpoint       func(context.Context, json.RawMessage) error
}

type WorkflowContext struct {
	Context   context.Context
	Execution database.WorkflowExecution
	database  *database.Database
}

type RetryableError struct {
	Err   error
	Delay time.Duration
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string {
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

func Retryable(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err, Delay: delay}
}

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

func NewEngine(database *database.Database, config EngineConfig) (*Engine, error) {
	if database == nil {
		return nil, errors.New("workflow database is required")
	}
	if config.MaxConcurrent < 1 {
		return nil, errors.New("workflow max concurrency must be positive")
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = time.Minute
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.PollInterval >= config.LeaseDuration {
		return nil, errors.New("workflow poll interval must be shorter than the lease duration")
	}
	if config.Owner == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("getting workflow engine hostname: %w", err)
		}
		config.Owner = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	return &Engine{
		database:      database,
		owner:         config.Owner,
		leaseDuration: config.LeaseDuration,
		pollInterval:  config.PollInterval,
		slots:         make(chan struct{}, config.MaxConcurrent),
		wake:          make(chan struct{}, 1),
		runners:       make(map[string]Workflow),
	}, nil
}

func (e *Engine) Register(kind string, workflow Workflow) error {
	if kind == "" || workflow == nil {
		return errors.New("workflow kind and runner are required")
	}
	e.runnersMu.Lock()
	defer e.runnersMu.Unlock()
	if e.ctx != nil {
		return errors.New("cannot register a workflow after the engine starts")
	}
	if _, exists := e.runners[kind]; exists {
		return fmt.Errorf("workflow %q is already registered", kind)
	}
	e.runners[kind] = workflow
	return nil
}

func (e *Engine) Start(ctx context.Context) error {
	e.runnersMu.Lock()
	defer e.runnersMu.Unlock()
	if e.ctx != nil {
		return errors.New("workflow engine already started")
	}
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.wg.Go(e.run)
	return nil
}

func (e *Engine) Close() {
	if e.cancel == nil {
		return
	}
	e.cancel()
	e.running.Range(func(_, value any) bool {
		value.(context.CancelFunc)()
		return true
	})
	e.wg.Wait()
}

func (e *Engine) Submit(ctx context.Context, request database.WorkflowSubmit) (database.WorkflowExecution, bool, error) {
	if request.ID == "" {
		id, err := newExecutionID()
		if err != nil {
			return database.WorkflowExecution{}, false, err
		}
		request.ID = id
	}
	if request.RunAt.IsZero() {
		request.RunAt = time.Now()
	}
	execution, created, err := e.database.SubmitWorkflow(ctx, request)
	if err != nil {
		return database.WorkflowExecution{}, false, err
	}
	if created {
		e.signal()
	}
	return execution, created, nil
}

func (e *Engine) Cancel(ctx context.Context, id string) (database.WorkflowExecution, error) {
	execution, err := e.database.CancelWorkflowExecution(ctx, id, time.Now())
	if err != nil {
		return database.WorkflowExecution{}, err
	}
	if value, ok := e.running.Load(id); ok {
		value.(context.CancelFunc)()
	}
	e.signal()
	return execution, nil
}

func (e *Engine) CancelDefinition(ctx context.Context, kind, definitionID string) (database.WorkflowExecution, error) {
	execution, err := e.database.GetActiveWorkflowExecution(ctx, kind, definitionID)
	if err != nil {
		return database.WorkflowExecution{}, err
	}
	return e.Cancel(ctx, execution.ID)
}

func (e *Engine) Get(ctx context.Context, id string) (database.WorkflowExecution, error) {
	return e.database.GetWorkflowExecution(ctx, id)
}

func (e *Engine) Events(ctx context.Context, id string) ([]database.WorkflowEvent, error) {
	return e.database.ListWorkflowExecutionEvents(ctx, id)
}

func (e *Engine) StartupMu() *sync.Mutex {
	return &e.startupMu
}

func (w *WorkflowContext) Activity(name string, input json.RawMessage, activity Activity) (json.RawMessage, error) {
	if activity == nil {
		return nil, errors.New("workflow activity is required")
	}
	canonicalInput, err := canonicalJSON(input)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(canonicalInput)
	record, completed, err := w.database.StartWorkflowActivity(w.Context, w.Execution.ID, name, hex.EncodeToString(hash[:]), time.Now())
	if err != nil {
		return nil, err
	}
	if completed {
		return record.Result, nil
	}
	info := ActivityInfo{
		Execution:        w.Execution,
		Name:             name,
		ResumeCheckpoint: record.Checkpoint,
		checkpoint: func(ctx context.Context, value json.RawMessage) error {
			return w.database.CheckpointWorkflowActivity(ctx, w.Execution.ID, name, value)
		},
	}
	result, err := activity(w.Context, info)
	if err != nil {
		if retryErr := w.database.RetryWorkflowActivity(w.Context, w.Execution.ID, name, err.Error()); retryErr != nil {
			return nil, fmt.Errorf("recording workflow activity failure: %w", retryErr)
		}
		return nil, err
	}
	if err := w.database.CompleteWorkflowActivity(w.Context, w.Execution.ID, name, result, time.Now()); err != nil {
		return nil, err
	}
	return result, nil
}

func (i ActivityInfo) Checkpoint(ctx context.Context, value json.RawMessage) error {
	if i.checkpoint == nil {
		return errors.New("activity checkpoint is unavailable")
	}
	return i.checkpoint(ctx, value)
}

func (e *Engine) run() {
	ticker := time.NewTicker(e.pollInterval)
	defer ticker.Stop()
	for {
		if !e.dispatch() {
			return
		}
		select {
		case <-e.ctx.Done():
			return
		case <-e.wake:
		case <-ticker.C:
		}
	}
}

func (e *Engine) dispatch() bool {
	for {
		select {
		case <-e.ctx.Done():
			return false
		case e.slots <- struct{}{}:
		default:
			return true
		}

		execution, claimed, err := e.database.ClaimWorkflowExecution(e.ctx, e.owner, time.Now(), time.Now().Add(e.leaseDuration))
		if err != nil {
			<-e.slots
			log.Error(err, "workflow engine claim failed")
			return true
		}
		if !claimed {
			<-e.slots
			return true
		}
		e.wg.Go(func() {
			defer func() { <-e.slots }()
			e.runExecution(execution)
		})
	}
}

func (e *Engine) runExecution(execution database.WorkflowExecution) {
	e.runnersMu.RLock()
	workflow := e.runners[execution.Kind]
	e.runnersMu.RUnlock()
	if workflow == nil {
		e.finish(execution, NonRetryable(fmt.Errorf("workflow %q is not registered", execution.Kind)))
		return
	}

	ctx, cancel := context.WithCancel(e.ctx)
	e.running.Store(execution.ID, cancel)
	defer func() {
		e.running.Delete(execution.ID)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- workflow(&WorkflowContext{Context: ctx, Execution: execution, database: e.database})
	}()

	heartbeat := time.NewTicker(e.leaseDuration / 3)
	defer heartbeat.Stop()
	for {
		select {
		case err := <-done:
			e.finish(execution, err)
			return
		case <-heartbeat.C:
			if err := e.database.RenewWorkflowExecutionLease(e.ctx, execution.ID, e.owner, time.Now().Add(e.leaseDuration)); err != nil {
				cancel()
				log.Error(err, "workflow engine lease renewal failed", "executionID", execution.ID)
			}
		case <-e.ctx.Done():
			cancel()
			<-done
			return
		}
	}
}

func (e *Engine) finish(execution database.WorkflowExecution, runErr error) {
	current, err := e.database.GetWorkflowExecution(e.ctx, execution.ID)
	if err != nil {
		log.Error(err, "workflow engine state lookup failed", "executionID", execution.ID)
		return
	}
	execution = current
	if execution.CancelRequested {
		runErr = nil
	} else if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		return
	}
	if runErr == nil {
		state := database.WorkflowExecutionSucceeded
		if execution.CancelRequested {
			state = database.WorkflowExecutionCanceled
		}
		if err := e.database.FinishWorkflowExecution(e.ctx, execution.ID, e.owner, state, time.Now(), ""); err != nil {
			log.Error(err, "workflow engine completion failed", "executionID", execution.ID)
		}
		return
	}

	var nonRetryable nonRetryableError
	if errors.As(runErr, &nonRetryable) || execution.Attempt >= execution.MaxAttempts {
		if err := e.database.FinishWorkflowExecution(e.ctx, execution.ID, e.owner, database.WorkflowExecutionFailed, time.Now(), runErr.Error()); err != nil {
			log.Error(err, "workflow engine failure completion failed", "executionID", execution.ID)
		}
		return
	}

	delay := retryDelay(execution, runErr)
	if err := e.database.FinishWorkflowExecution(e.ctx, execution.ID, e.owner, database.WorkflowExecutionPending, time.Now().Add(delay), runErr.Error()); err != nil {
		log.Error(err, "workflow engine retry scheduling failed", "executionID", execution.ID)
		return
	}
	e.signal()
}

func retryDelay(execution database.WorkflowExecution, err error) time.Duration {
	delay := execution.RetryInitialDelay
	var retryable *RetryableError
	if errors.As(err, &retryable) && retryable.Delay > 0 {
		delay = retryable.Delay
	}
	if delay > execution.RetryMaxDelay {
		return execution.RetryMaxDelay
	}
	for attempt := 1; attempt < execution.Attempt; attempt++ {
		if delay >= execution.RetryMaxDelay/2 {
			return execution.RetryMaxDelay
		}
		delay *= 2
	}
	return delay
}

func canonicalJSON(value json.RawMessage) ([]byte, error) {
	if !json.Valid(value) {
		return nil, errors.New("workflow activity input must be valid JSON")
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return nil, fmt.Errorf("decoding workflow activity input: %w", err)
	}
	return json.Marshal(decoded)
}

func newExecutionID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generating workflow execution ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
