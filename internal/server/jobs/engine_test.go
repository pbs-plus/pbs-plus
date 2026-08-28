//go:build linux

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

func TestEngine_ReplaysCompletedActivities(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, db := newTestEngine(t, ctx)
	var firstRuns atomic.Int32
	var secondRuns atomic.Int32
	idempotencyKeys := make(chan string, 2)
	if err := engine.Register("test.replay", func(workflow *WorkflowContext) error {
		if err := workflow.Step("first", func(context.Context) error {
			firstRuns.Add(1)
			return nil
		}); err != nil {
			return err
		}
		_, err := workflow.Activity("second", json.RawMessage(`{"value":2}`), func(_ context.Context, info ActivityInfo) (json.RawMessage, error) {
			idempotencyKeys <- info.IdempotencyKey
			if secondRuns.Add(1) == 1 {
				return nil, &RetryableError{Err: errors.New("temporary failure"), Delay: time.Second}
			}
			return json.RawMessage(`{"done":true}`), nil
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	execution, created, err := engine.Submit(ctx, testWorkflowSubmit("test.replay", "replay"))
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first submission was not created")
	}

	execution = waitForWorkflowState(t, ctx, engine, execution.ID, jobdb.StateSucceeded)
	if firstRuns.Load() != 1 {
		t.Fatalf("first activity ran %d times, want 1", firstRuns.Load())
	}
	if secondRuns.Load() != 2 {
		t.Fatalf("second activity ran %d times, want 2", secondRuns.Load())
	}
	if execution.Attempt != 2 {
		t.Fatalf("execution attempts = %d, want 2", execution.Attempt)
	}
	firstKey, secondKey := <-idempotencyKeys, <-idempotencyKeys
	if firstKey != execution.ID+":second" || secondKey != firstKey {
		t.Fatalf("activity idempotency keys = %q, %q", firstKey, secondKey)
	}
	events, err := engine.Events(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 8 {
		t.Fatalf("event count = %d, want durable workflow history", len(events))
	}
	if events[0].Type != "execution.submitted" {
		t.Fatalf("first event = %q, want workflow.submitted", events[0].Type)
	}
	_ = db
}

func TestEngine_ReplaysWorkflowVersion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, db := newTestEngine(t, ctx)
	var v1Runs atomic.Int32
	var v2Runs atomic.Int32
	if err := engine.RegisterVersion("test.version", "1", func(*WorkflowContext) error {
		v1Runs.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterVersion("test.version", "2", func(*WorkflowContext) error {
		v2Runs.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	oldRequest := testWorkflowSubmit("test.version", "version-old")
	oldRequest.WorkflowVersion = "1"
	oldExecution, _, err := db.Submit(ctx, oldRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	currentExecution, _, err := engine.Submit(ctx, testWorkflowSubmit("test.version", "version-current"))
	if err != nil {
		t.Fatal(err)
	}
	waitForWorkflowState(t, ctx, engine, oldExecution.ID, jobdb.StateSucceeded)
	currentExecution = waitForWorkflowState(t, ctx, engine, currentExecution.ID, jobdb.StateSucceeded)
	if v1Runs.Load() != 1 || v2Runs.Load() != 1 {
		t.Fatalf("workflow versions ran v1=%d v2=%d, want 1 each", v1Runs.Load(), v2Runs.Load())
	}
	if currentExecution.WorkflowVersion != "2" {
		t.Fatalf("current workflow version = %q, want 2", currentExecution.WorkflowVersion)
	}
}

func TestEngine_RejectsNonDeterministicReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, db := newTestEngine(t, ctx)
	if err := engine.Register("test.determinism", func(workflow *WorkflowContext) error {
		_, err := workflow.Activity("changed", json.RawMessage(`{}`), func(context.Context, ActivityInfo) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	request := testWorkflowSubmit("test.determinism", "determinism")
	execution, _, err := db.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claimed, ok, err := db.Claim(ctx, "test-worker", now, now.Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("claiming execution = %t, %v", ok, err)
	}
	if _, completed, err := db.StartActivity(ctx, claimed.ID, claimed.LeaseOwner, claimed.Attempt, 1, "recorded", "input", now); err != nil || completed {
		t.Fatalf("recording activity = completed:%t, error:%v", completed, err)
	}
	if err := db.CompleteActivity(ctx, claimed.ID, claimed.LeaseOwner, claimed.Attempt, "recorded", []byte(`{}`), now); err != nil {
		t.Fatal(err)
	}
	if err := db.Finish(ctx, claimed.ID, claimed.LeaseOwner, claimed.Attempt, jobdb.StatePending, now, ""); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	execution = waitForWorkflowState(t, ctx, engine, execution.ID, jobdb.StateFailed)
	if execution.LastError == "" {
		t.Fatal("non-deterministic replay did not record an error")
	}
}

func TestEngine_CancelRunningWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, _ := newTestEngine(t, ctx)
	started := make(chan struct{})
	if err := engine.Register("test.cancel", func(workflow *WorkflowContext) error {
		_, err := workflow.Activity("wait", json.RawMessage(`{}`), func(ctx context.Context, _ ActivityInfo) (json.RawMessage, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	execution, _, err := engine.Submit(ctx, testWorkflowSubmit("test.cancel", "cancel"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("workflow did not start")
	}
	if _, err := engine.Cancel(ctx, execution.ID); err != nil {
		t.Fatal(err)
	}
	waitForWorkflowState(t, ctx, engine, execution.ID, jobdb.StateCanceled)
}

func TestEngine_InvalidateReRunsActivity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, _ := newTestEngine(t, ctx)
	var startRuns atomic.Int32
	if err := engine.Register("test.invalidate", func(workflow *WorkflowContext) error {
		if _, err := workflow.Activity("start", json.RawMessage(`{}`), func(context.Context, ActivityInfo) (json.RawMessage, error) {
			startRuns.Add(1)
			return json.RawMessage(`{"upid":"u1"}`), nil
		}); err != nil {
			return err
		}
		_, err := workflow.Activity("wait", json.RawMessage(`{}`), func(context.Context, ActivityInfo) (json.RawMessage, error) {
			return nil, &RetryableError{Err: errors.New("task failed"), Delay: 10 * time.Millisecond}
		})
		if err != nil {
			_ = workflow.Invalidate("start")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	if _, _, err := engine.Submit(ctx, testWorkflowSubmit("test.invalidate", "invalidate")); err != nil {
		t.Fatal(err)
	}

	execID := "workflow-invalidate"
	deadline := time.Now().Add(5 * time.Second)
	for {
		execution, err := engine.Get(ctx, execID)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == jobdb.StateFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution state = %q, want failed", execution.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if startRuns.Load() < 2 {
		t.Fatalf("start activity ran %d times, want >= 2 after invalidation", startRuns.Load())
	}
}

func TestEngine_FinalizerRunsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, _ := newTestEngine(t, ctx)
	started := make(chan struct{})
	finalized := make(chan struct{}, 1)
	if err := engine.Register("test.detached", func(workflow *WorkflowContext) error {
		_, err := workflow.Activity("work", json.RawMessage(`{}`), func(ctx context.Context, _ ActivityInfo) (json.RawMessage, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		if err != nil {
			if ferr := workflow.Finalize(func(context.Context) error {
				finalized <- struct{}{}
				return nil
			}); ferr != nil {
				t.Errorf("finalizer failed: %v", ferr)
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	execution, _, err := engine.Submit(ctx, testWorkflowSubmit("test.detached", "detached"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("workflow did not start")
	}
	if _, err := engine.Cancel(ctx, execution.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finalized:
	case <-time.After(3 * time.Second):
		t.Fatal("detached finalizer did not run after cancellation")
	}
	waitForWorkflowState(t, ctx, engine, execution.ID, jobdb.StateCanceled)
}

func TestEngine_CheckpointResumesActivity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, _ := newTestEngine(t, ctx)
	var resumedFrom atomic.Int32
	if err := engine.Register("test.checkpoint", func(workflow *WorkflowContext) error {
		_, err := workflow.Activity("verify", json.RawMessage(`{}`), func(ctx context.Context, info ActivityInfo) (json.RawMessage, error) {
			start := 0
			if len(info.ResumeCheckpoint) > 0 {
				var cp struct {
					Next int `json:"next"`
				}
				if err := json.Unmarshal(info.ResumeCheckpoint, &cp); err == nil && cp.Next > 0 {
					resumedFrom.Store(int32(cp.Next))
				}
				start = cp.Next
			}
			if start == 0 {
				cp, _ := json.Marshal(struct {
					Next int `json:"next"`
				}{Next: 3})
				if err := info.Checkpoint(ctx, cp); err != nil {
					return nil, err
				}
				return nil, &RetryableError{Err: errors.New("resume me"), Delay: 10 * time.Millisecond}
			}
			return json.RawMessage(`{}`), nil
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	if _, _, err := engine.Submit(ctx, testWorkflowSubmit("test.checkpoint", "checkpoint")); err != nil {
		t.Fatal(err)
	}
	waitForWorkflowState(t, ctx, engine, "workflow-checkpoint", jobdb.StateSucceeded)
	if resumedFrom.Load() != 3 {
		t.Fatalf("activity resumed from %d, want 3", resumedFrom.Load())
	}
}

func TestDatabase_CancelsExpiredWorkflow(t *testing.T) {
	ctx := context.Background()
	_, db := newTestEngine(t, ctx)
	execution, _, err := db.Submit(ctx, testWorkflowSubmit("test.cancel-expired", "cancel-expired"))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	if _, ok, err := db.Claim(ctx, "worker-a", now, now.Add(time.Second)); err != nil || !ok {
		t.Fatalf("claiming execution = %t, %v", ok, err)
	}
	if _, err := db.Cancel(ctx, execution.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.Claim(ctx, "worker-b", now.Add(2*time.Second), now.Add(3*time.Second)); err != nil || ok {
		t.Fatalf("recovering canceled execution = %t, %v", ok, err)
	}

	execution, err = db.GetExecution(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.State != jobdb.StateCanceled {
		t.Fatalf("execution state = %q, want %q", execution.State, jobdb.StateCanceled)
	}
}

func TestDatabase_FencesStaleActivityOwner(t *testing.T) {
	ctx := context.Background()
	_, db := newTestEngine(t, ctx)
	_, _, err := db.Submit(ctx, testWorkflowSubmit("test.activity-fence", "activity-fence"))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	claimed, ok, err := db.Claim(ctx, "worker-a", now, now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("claiming execution = %t, %v", ok, err)
	}
	if _, completed, err := db.StartActivity(ctx, claimed.ID, "worker-a", claimed.Attempt, 1, "work", "input", now); err != nil || completed {
		t.Fatalf("starting activity = completed:%t, error:%v", completed, err)
	}

	claimed, ok, err = db.Claim(ctx, "worker-b", now.Add(2*time.Second), now.Add(3*time.Second))
	if err != nil || !ok {
		t.Fatalf("recovering execution = %t, %v", ok, err)
	}
	activity, completed, err := db.StartActivity(ctx, claimed.ID, "worker-b", claimed.Attempt, 1, "work", "input", now.Add(2*time.Second))
	if err != nil || completed {
		t.Fatalf("restarting activity = completed:%t, error:%v", completed, err)
	}
	if activity.Attempt != 2 {
		t.Fatalf("activity attempts = %d, want 2", activity.Attempt)
	}
	if err := db.CompleteActivity(ctx, claimed.ID, "worker-a", 1, "work", []byte(`{}`), now.Add(2*time.Second)); !errors.Is(err, jobdb.ErrNotFound) {
		t.Fatalf("stale owner completion error = %v, want %v", err, jobdb.ErrNotFound)
	}
	if err := db.CompleteActivity(ctx, claimed.ID, "worker-b", claimed.Attempt, "work", []byte(`{}`), now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestDatabase_DoesNotRenewExpiredLease(t *testing.T) {
	ctx := context.Background()
	_, db := newTestEngine(t, ctx)
	now := time.Now().Add(-2 * time.Second)
	request := testWorkflowSubmit("test.lease-expired", "lease-expired")
	request.RunAt = now
	if _, _, err := db.Submit(ctx, request); err != nil {
		t.Fatal(err)
	}

	claimed, ok, err := db.Claim(ctx, "worker-a", now, now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("claiming execution = %t, %v", ok, err)
	}
	if err := db.RenewLease(ctx, claimed.ID, "worker-a", claimed.Attempt, time.Now().Add(time.Minute)); !errors.Is(err, jobdb.ErrNotFound) {
		t.Fatalf("renewing expired lease error = %v, want %v", err, jobdb.ErrNotFound)
	}
	if err := db.Finish(ctx, claimed.ID, "worker-a", claimed.Attempt, jobdb.StateSucceeded, time.Now(), ""); !errors.Is(err, jobdb.ErrNotFound) {
		t.Fatalf("finishing expired lease error = %v, want %v", err, jobdb.ErrNotFound)
	}
	if _, _, err := db.StartActivity(ctx, claimed.ID, "worker-a", claimed.Attempt, 1, "work", "input", time.Now()); !errors.Is(err, jobdb.ErrNotFound) {
		t.Fatalf("starting activity with expired lease error = %v, want %v", err, jobdb.ErrNotFound)
	}
	reclaimed, ok, err := db.Claim(ctx, "worker-b", time.Now(), time.Now().Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("reclaiming execution = %t, %v", ok, err)
	}
	if _, completed, err := db.StartActivity(ctx, reclaimed.ID, "worker-b", reclaimed.Attempt, 1, "work", "input", time.Now()); err != nil || completed {
		t.Fatalf("starting reclaimed activity = completed:%t, error:%v", completed, err)
	}
}

func TestDatabase_ClaimsResourceOnce(t *testing.T) {
	ctx := context.Background()
	_, db := newTestEngine(t, ctx)
	first := testWorkflowSubmit("test.lock", "first")
	first.Resources = []string{"target:test"}
	second := testWorkflowSubmit("test.lock", "second")
	second.Resources = []string{"target:test"}
	if _, _, err := db.Submit(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Submit(ctx, second); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claimed, ok, err := db.Claim(ctx, "worker-a", now, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || claimed.ID != first.ID {
		t.Fatalf("first claim = %#v, %t", claimed, ok)
	}
	if _, ok, err := db.Claim(ctx, "worker-b", now, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("second execution claimed while its resource was locked")
	}
	waiting, err := db.GetExecution(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Attempt != 0 {
		t.Fatalf("waiting execution attempts = %d, want 0", waiting.Attempt)
	}
	if err := db.Finish(ctx, first.ID, "worker-a", claimed.Attempt, jobdb.StateSucceeded, now, ""); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err = db.Claim(ctx, "worker-b", now.Add(time.Second), now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || claimed.ID != second.ID {
		t.Fatalf("second claim = %#v, %t", claimed, ok)
	}
}

func newTestEngine(t *testing.T, ctx context.Context) (*Engine, *jobdb.Store) {
	t.Helper()
	db, err := jobdb.Open(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	engine, err := NewEngine(db, EngineConfig{
		Owner:         "test-worker",
		LeaseDuration: 3 * time.Second,
		PollInterval:  10 * time.Millisecond,
		MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine, db
}

func testWorkflowSubmit(kind, suffix string) jobdb.SubmitRequest {
	return jobdb.SubmitRequest{
		ID:                "workflow-" + suffix,
		Kind:              kind,
		DefinitionID:      "definition-" + suffix,
		Trigger:           "manual",
		DedupeKey:         "dedupe-" + suffix,
		Payload:           json.RawMessage(`{}`),
		MaxAttempts:       3,
		RetryInitialDelay: time.Second,
		RetryMaxDelay:     time.Second,
		RunAt:             time.Now(),
	}
}

func waitForWorkflowState(t *testing.T, ctx context.Context, engine *Engine, id, want string) jobdb.Execution {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		execution, err := engine.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == want {
			return execution
		}
		select {
		case <-timeout.C:
			t.Fatalf("workflow state = %q, want %q", execution.State, want)
		case <-ticker.C:
		}
	}
}
