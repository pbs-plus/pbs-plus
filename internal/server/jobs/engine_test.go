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

	"github.com/pbs-plus/pbs-plus/internal/server/jobs/store"
)

func TestEngine_ReplaysCompletedActivities(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, db := newTestEngine(t, ctx)
	var firstRuns atomic.Int32
	var secondRuns atomic.Int32
	if err := engine.Register("test.replay", func(workflow *WorkflowContext) error {
		if _, err := workflow.Activity("first", json.RawMessage(`{"value":1}`), func(context.Context, ActivityInfo) (json.RawMessage, error) {
			firstRuns.Add(1)
			return json.RawMessage(`{"done":true}`), nil
		}); err != nil {
			return err
		}
		_, err := workflow.Activity("second", json.RawMessage(`{"value":2}`), func(context.Context, ActivityInfo) (json.RawMessage, error) {
			if secondRuns.Add(1) == 1 {
				return nil, Retryable(errors.New("temporary failure"), time.Second)
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

	execution = waitForWorkflowState(t, ctx, engine, execution.ID, store.StateSucceeded)
	if firstRuns.Load() != 1 {
		t.Fatalf("first activity ran %d times, want 1", firstRuns.Load())
	}
	if secondRuns.Load() != 2 {
		t.Fatalf("second activity ran %d times, want 2", secondRuns.Load())
	}
	if execution.Attempt != 2 {
		t.Fatalf("execution attempts = %d, want 2", execution.Attempt)
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
	waitForWorkflowState(t, ctx, engine, execution.ID, store.StateCanceled)
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
	if err := db.Finish(ctx, first.ID, "worker-a", store.StateSucceeded, now, ""); err != nil {
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

func newTestEngine(t *testing.T, ctx context.Context) (*Engine, *store.DB) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "jobs.db"))
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

func testWorkflowSubmit(kind, suffix string) store.SubmitRequest {
	return store.SubmitRequest{
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

func waitForWorkflowState(t *testing.T, ctx context.Context, engine *Engine, id, want string) store.Execution {
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
