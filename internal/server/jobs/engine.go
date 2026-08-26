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
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/store"
)

type EngineConfig struct {
	Owner         string
	LeaseDuration time.Duration
	PollInterval  time.Duration
	MaxConcurrent int
}

type Engine struct {
	db            *store.DB
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
	Execution        store.Execution
	Name             string
	ResumeCheckpoint json.RawMessage
	checkpoint       func(context.Context, json.RawMessage) error
}

type WorkflowContext struct {
	Context   context.Context
	Execution store.Execution
	db        *store.DB
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

func NewEngine(db *store.DB, config EngineConfig) (*Engine, error) {
	if db == nil {
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
		db:            db,
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

func (e *Engine) Submit(ctx context.Context, request store.SubmitRequest) (store.Execution, bool, error) {
	if request.ID == "" {
		id, err := NewExecutionID()
		if err != nil {
			return store.Execution{}, false, err
		}
		request.ID = id
	}
	if request.RunAt.IsZero() {
		request.RunAt = time.Now()
	}
	execution, created, err := e.db.Submit(ctx, request)
	if err != nil {
		return store.Execution{}, false, err
	}
	if created {
		e.signal()
	}
	return execution, created, nil
}

func (e *Engine) Cancel(ctx context.Context, id string) (store.Execution, error) {
	execution, err := e.db.Cancel(ctx, id, time.Now())
	if err != nil {
		return store.Execution{}, err
	}
	if value, ok := e.running.Load(id); ok {
		value.(context.CancelFunc)()
	}
	e.signal()
	return execution, nil
}

func (e *Engine) CancelDefinition(ctx context.Context, kind, definitionID string) (store.Execution, error) {
	execution, err := e.db.GetActiveExecution(ctx, kind, definitionID)
	if err != nil {
		return store.Execution{}, err
	}
	return e.Cancel(ctx, execution.ID)
}

func (e *Engine) Get(ctx context.Context, id string) (store.Execution, error) {
	return e.db.GetExecution(ctx, id)
}

func (e *Engine) Events(ctx context.Context, id string) ([]store.Event, error) {
	return e.db.ListEvents(ctx, id)
}

func (e *Engine) StartupMu() *sync.Mutex {
	return &e.startupMu
}

func (w *WorkflowContext) Activity(name string, input json.RawMessage, activity Activity) (json.RawMessage, error) {
	return w.ActivityCtx(w.Context, name, input, activity)
}

// ActivityCtx runs an activity under an explicit context; finalizers
// use Detached so exactly-once completion work survives cancellation.
func (w *WorkflowContext) ActivityCtx(ctx context.Context, name string, input json.RawMessage, activity Activity) (json.RawMessage, error) {
	if activity == nil {
		return nil, errors.New("workflow activity is required")
	}
	canonicalInput, err := canonicalJSON(input)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(canonicalInput)
	record, completed, err := w.db.StartActivity(ctx, w.Execution.ID, name, hex.EncodeToString(hash[:]), time.Now())
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
			return w.db.CheckpointActivity(ctx, w.Execution.ID, name, value)
		},
	}
	result, err := activity(ctx, info)
	if err != nil {
		if retryErr := w.db.FailActivity(ctx, w.Execution.ID, name, err.Error()); retryErr != nil {
			return nil, fmt.Errorf("recording workflow activity failure: %w", retryErr)
		}
		return nil, err
	}
	if err := w.db.CompleteActivity(ctx, w.Execution.ID, name, result, time.Now()); err != nil {
		return nil, err
	}
	return result, nil
}

// Detached returns a context that outlives workflow cancellation, for
// finalization that must complete exactly once.
func (w *WorkflowContext) Detached() context.Context {
	return context.WithoutCancel(w.Context)
}

func (i ActivityInfo) Checkpoint(ctx context.Context, value json.RawMessage) error {
	if i.checkpoint == nil {
		return errors.New("activity checkpoint is unavailable")
	}
	return i.checkpoint(ctx, value)
}

// Invalidate un-completes a completed activity so the next attempt
// re-runs it instead of replaying its result.
func (w *WorkflowContext) Invalidate(name string) error {
	return w.db.InvalidateActivity(w.Context, w.Execution.ID, name)
}

// IsFinalAttempt reports whether the running attempt is the
// execution's last, so failure finalization can run exactly once.
func IsFinalAttempt(execution store.Execution) bool {
	return execution.Attempt >= execution.MaxAttempts
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

		execution, claimed, err := e.db.Claim(e.ctx, e.owner, time.Now(), time.Now().Add(e.leaseDuration))
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

func (e *Engine) runExecution(execution store.Execution) {
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
		done <- workflow(&WorkflowContext{Context: ctx, Execution: execution, db: e.db})
	}()

	heartbeat := time.NewTicker(e.leaseDuration / 3)
	defer heartbeat.Stop()
	for {
		select {
		case err := <-done:
			e.finish(execution, err)
			return
		case <-heartbeat.C:
			if err := e.db.RenewLease(e.ctx, execution.ID, e.owner, time.Now().Add(e.leaseDuration)); err != nil {
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

func (e *Engine) finish(execution store.Execution, runErr error) {
	current, err := e.db.GetExecution(e.ctx, execution.ID)
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
		state := store.StateSucceeded
		if execution.CancelRequested {
			state = store.StateCanceled
		}
		if err := e.db.Finish(e.ctx, execution.ID, e.owner, state, time.Now(), ""); err != nil {
			log.Error(err, "workflow engine completion failed", "executionID", execution.ID)
		}
		return
	}

	var nonRetryable nonRetryableError
	if errors.As(runErr, &nonRetryable) || execution.Attempt >= execution.MaxAttempts {
		if err := e.db.Finish(e.ctx, execution.ID, e.owner, store.StateFailed, time.Now(), runErr.Error()); err != nil {
			log.Error(err, "workflow engine failure completion failed", "executionID", execution.ID)
		}
		return
	}

	delay := retryDelay(execution, runErr)
	if err := e.db.Finish(e.ctx, execution.ID, e.owner, store.StatePending, time.Now().Add(delay), runErr.Error()); err != nil {
		log.Error(err, "workflow engine retry scheduling failed", "executionID", execution.ID)
		return
	}
	e.signal()
}

func retryDelay(execution store.Execution, err error) time.Duration {
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

func NewExecutionID() (string, error) {
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
