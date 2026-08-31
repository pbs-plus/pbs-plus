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
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

type EngineConfig struct {
	Owner         string
	LeaseDuration time.Duration
	PollInterval  time.Duration
	MaxConcurrent int
}

type Engine struct {
	db            *jobdb.Store
	owner         string
	leaseDuration time.Duration
	pollInterval  time.Duration
	slots         chan struct{}
	wake          chan struct{}

	runnersMu sync.RWMutex
	runners   map[string]map[string]Workflow
	current   map[string]string
	running   sync.Map

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// boundTasks tracks the latest task bound to each execution so the
	// engine can manage its lifetime: retries keep it alive (the UPID is
	// recorded in job history), terminal outcomes close it.
	boundTasks sync.Map
}

type Workflow func(*WorkflowContext) error

type Activity func(context.Context, ActivityInfo) (json.RawMessage, error)

type ActivityInfo struct {
	Execution        jobdb.Execution
	Name             string
	IdempotencyKey   string
	ResumeCheckpoint json.RawMessage
	checkpoint       func(context.Context, json.RawMessage) error
}

type WorkflowContext struct {
	Context   context.Context
	Execution jobdb.Execution
	db        *jobdb.Store
	engine    *Engine
	position  int
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

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

func NewEngine(db *jobdb.Store, config EngineConfig) (*Engine, error) {
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
		runners:       make(map[string]map[string]Workflow),
		current:       make(map[string]string),
	}, nil
}

func (e *Engine) Register(kind string, workflow Workflow) error {
	return e.RegisterVersion(kind, "1", workflow)
}

func (e *Engine) RegisterVersion(kind, version string, workflow Workflow) error {
	if kind == "" || version == "" || workflow == nil {
		return errors.New("workflow kind, version, and runner are required")
	}
	e.runnersMu.Lock()
	defer e.runnersMu.Unlock()
	if e.ctx != nil {
		return errors.New("cannot register a workflow after the engine starts")
	}
	if e.runners[kind] == nil {
		e.runners[kind] = make(map[string]Workflow)
	}
	if _, exists := e.runners[kind][version]; exists {
		return fmt.Errorf("workflow %q version %q is already registered", kind, version)
	}
	e.runners[kind][version] = workflow
	e.current[kind] = version
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

func (e *Engine) Submit(ctx context.Context, request jobdb.SubmitRequest) (jobdb.Execution, bool, error) {
	e.runnersMu.RLock()
	version, registered := e.current[request.Kind]
	e.runnersMu.RUnlock()
	if !registered {
		return jobdb.Execution{}, false, fmt.Errorf("workflow %q is not registered", request.Kind)
	}
	if request.ID == "" {
		id, err := NewExecutionID()
		if err != nil {
			return jobdb.Execution{}, false, err
		}
		request.ID = id
	}
	if request.RunAt.IsZero() {
		request.RunAt = time.Now()
	}
	request.WorkflowVersion = version
	execution, created, err := e.db.Submit(ctx, request)
	if err != nil {
		return jobdb.Execution{}, false, err
	}
	if created {
		e.signal()
	}
	return execution, created, nil
}

func (e *Engine) Cancel(ctx context.Context, id string) (jobdb.Execution, error) {
	execution, err := e.db.Cancel(ctx, id, time.Now())
	if err != nil {
		return jobdb.Execution{}, err
	}
	if value, ok := e.running.Load(id); ok {
		value.(context.CancelFunc)()
	}
	e.signal()
	return execution, nil
}

func (e *Engine) CancelDefinition(ctx context.Context, kind, definitionID string) (jobdb.Execution, error) {
	execution, err := e.db.GetActiveExecution(ctx, kind, definitionID)
	if err != nil {
		return jobdb.Execution{}, err
	}
	return e.Cancel(ctx, execution.ID)
}

func (e *Engine) Get(ctx context.Context, id string) (jobdb.Execution, error) {
	return e.db.GetExecution(ctx, id)
}

func (e *Engine) ActiveExecution(ctx context.Context, kind, definitionID string) (jobdb.Execution, error) {
	return e.db.GetActiveExecution(ctx, kind, definitionID)
}

func (e *Engine) ResourceHolder(ctx context.Context, resourceKey string) (jobdb.Execution, error) {
	return e.db.ResourceLockHolder(ctx, resourceKey)
}

func (e *Engine) Events(ctx context.Context, id string) ([]jobdb.Event, error) {
	return e.db.ListEvents(ctx, id)
}

func (w *WorkflowContext) Activity(name string, input json.RawMessage, activity Activity) (json.RawMessage, error) {
	return w.activity(w.Context, name, input, activity)
}

func (w *WorkflowContext) Step(name string, step func(context.Context) error) error {
	if step == nil {
		return errors.New("workflow step is required")
	}
	_, err := w.Activity(name, json.RawMessage(`{}`), func(ctx context.Context, _ ActivityInfo) (json.RawMessage, error) {
		if err := step(ctx); err != nil {
			return nil, err
		}
		return json.RawMessage(`{}`), nil
	})
	return err
}

// Finalize runs the durable finalizer after cancellation.
func (w *WorkflowContext) Finalize(finalizer func(context.Context) error) error {
	if finalizer == nil {
		return errors.New("workflow finalizer is required")
	}
	_, err := w.activity(context.WithoutCancel(w.Context), "finalize", json.RawMessage(`{}`), func(ctx context.Context, _ ActivityInfo) (json.RawMessage, error) {
		if err := finalizer(ctx); err != nil {
			return nil, err
		}
		return json.RawMessage(`{}`), nil
	})
	return err
}

func (w *WorkflowContext) activity(ctx context.Context, name string, input json.RawMessage, activity Activity) (json.RawMessage, error) {
	if activity == nil {
		return nil, errors.New("workflow activity is required")
	}
	canonicalInput, err := canonicalJSON(input)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(canonicalInput)
	w.position++
	record, completed, err := w.db.StartActivity(ctx, w.Execution.ID, w.Execution.LeaseOwner, w.Execution.Attempt, w.position, name, hex.EncodeToString(hash[:]), time.Now())
	if err != nil {
		return nil, err
	}
	if completed {
		return record.Result, nil
	}
	info := ActivityInfo{
		Execution:        w.Execution,
		Name:             name,
		IdempotencyKey:   w.Execution.ID + ":" + name,
		ResumeCheckpoint: record.Checkpoint,
		checkpoint: func(ctx context.Context, value json.RawMessage) error {
			return w.db.CheckpointActivity(ctx, w.Execution.ID, w.Execution.LeaseOwner, w.Execution.Attempt, name, value)
		},
	}
	result, err := activity(ctx, info)
	if err != nil {
		if retryErr := w.db.FailActivity(ctx, w.Execution.ID, w.Execution.LeaseOwner, w.Execution.Attempt, name, err.Error()); retryErr != nil {
			return nil, fmt.Errorf("recording workflow activity failure: %w", retryErr)
		}
		return nil, err
	}
	if err := w.db.CompleteActivity(ctx, w.Execution.ID, w.Execution.LeaseOwner, w.Execution.Attempt, name, result, time.Now()); err != nil {
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

// Invalidate un-completes a completed activity so the next attempt
// re-runs it instead of replaying its result.
func (w *WorkflowContext) Invalidate(name string) error {
	return w.db.InvalidateActivity(w.Context, w.Execution.ID, w.Execution.LeaseOwner, w.Execution.Attempt, name)
}

// IsFinalAttempt reports whether the running attempt is the
// execution's last, so failure finalization can run exactly once.
func IsFinalAttempt(execution jobdb.Execution) bool {
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

func (e *Engine) runExecution(execution jobdb.Execution) {
	e.runnersMu.RLock()
	workflow := e.runners[execution.Kind][execution.WorkflowVersion]
	e.runnersMu.RUnlock()
	if workflow == nil {
		e.finish(execution, NonRetryable(fmt.Errorf("workflow %q version %q is not registered", execution.Kind, execution.WorkflowVersion)))
		return
	}

	ctx, cancel := context.WithCancel(e.ctx)
	ctx = e.withAbortBinder(ctx, execution.ID)
	e.running.Store(execution.ID, cancel)
	defer func() {
		e.running.Delete(execution.ID)
		cancel()
	}()

	workflowContext := &WorkflowContext{Context: ctx, Execution: execution, db: e.db, engine: e}
	done := make(chan error, 1)
	go func() {
		done <- workflow(workflowContext)
	}()

	heartbeat := time.NewTicker(e.leaseDuration / 3)
	defer heartbeat.Stop()
	for {
		select {
		case err := <-done:
			if err == nil {
				if replayErr := e.db.EnsureReplayComplete(e.ctx, execution.ID, e.owner, execution.Attempt, workflowContext.position); replayErr != nil {
					err = NonRetryable(fmt.Errorf("validating workflow replay: %w", replayErr))
				}
			}
			e.finish(execution, err)
			return
		case <-heartbeat.C:
			if err := e.db.RenewLease(e.ctx, execution.ID, e.owner, execution.Attempt, time.Now().Add(e.leaseDuration)); err != nil {
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

func (e *Engine) finish(execution jobdb.Execution, runErr error) {
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
		state := jobdb.StateSucceeded
		if execution.CancelRequested {
			state = jobdb.StateCanceled
		}
		if err := e.db.Finish(e.ctx, execution.ID, e.owner, execution.Attempt, state, time.Now(), ""); err != nil {
			log.Error(err, "workflow engine completion failed", "executionID", execution.ID)
		}
		e.closeBoundTask(execution.ID)
		return
	}

	var nonRetryable nonRetryableError
	if errors.As(runErr, &nonRetryable) || execution.Attempt >= execution.MaxAttempts {
		if err := e.db.Finish(e.ctx, execution.ID, e.owner, execution.Attempt, jobdb.StateFailed, time.Now(), runErr.Error()); err != nil {
			log.Error(err, "workflow engine failure completion failed", "executionID", execution.ID)
		}
		e.closeBoundTask(execution.ID)
		return
	}

	delay := retryDelay(execution, runErr)
	if err := e.db.Finish(e.ctx, execution.ID, e.owner, execution.Attempt, jobdb.StatePending, time.Now().Add(delay), runErr.Error()); err != nil {
		log.Error(err, "workflow engine retry scheduling failed", "executionID", execution.ID)
		return
	}
	e.noteRetry(execution, delay, runErr)
	e.signal()
}

// closeBoundTask closes the execution's bound task (queued placeholder)
// once the execution reaches a terminal state. Real worker tasks do not
// implement Close and are left to their own lifecycle.
func (e *Engine) closeBoundTask(executionID string) {
	v, ok := e.boundTasks.LoadAndDelete(executionID)
	if !ok {
		return
	}
	if closer, ok := v.(interface{ Close() }); ok {
		closer.Close()
	}
}

// noteRetry records the failed attempt on the execution's bound queued task
// so the task log and live state show why the job is waiting to retry.
func (e *Engine) noteRetry(execution jobdb.Execution, delay time.Duration, runErr error) {
	v, ok := e.boundTasks.Load(execution.ID)
	if !ok {
		return
	}
	setter, ok := v.(interface{ SetState(string) })
	if !ok {
		return
	}
	msg := runErr.Error()
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	setter.SetState(fmt.Sprintf("RETRYING: attempt %d/%d failed: %s; retrying in %s", execution.Attempt, execution.MaxAttempts, msg, delay.Round(time.Second)))
}

func retryDelay(execution jobdb.Execution, err error) time.Duration {
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
