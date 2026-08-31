//go:build linux

package jobs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

type fakeQueuedTask struct {
	mu     sync.Mutex
	states []string
	closed bool
}

func (f *fakeQueuedTask) OnAbort(hook func()) {}

func (f *fakeQueuedTask) SetState(state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states = append(f.states, state)
}

func (f *fakeQueuedTask) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeQueuedTask) snapshot() ([]string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.states...), f.closed
}

// The bound task must survive retries with RETRYING notes and close only on terminal outcome.
func TestEngine_BoundTaskSurvivesRetryAndClosesOnTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, _ := newTestEngine(t, ctx)
	task := &fakeQueuedTask{}
	if err := engine.Register("test.boundtask", func(workflow *WorkflowContext) error {
		workflow.BindTask(task)
		return errors.New("target unreachable")
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	execution, created, err := engine.Submit(ctx, testWorkflowSubmit("test.boundtask", "boundtask"))
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("submission was not created")
	}

	execution = waitForWorkflowState(t, ctx, engine, execution.ID, jobdb.StateFailed)
	if execution.Attempt != 3 {
		t.Fatalf("attempts = %d, want 3", execution.Attempt)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		states, closed := task.snapshot()
		if closed && len(states) >= 2 {
			if !strings.HasPrefix(states[0], "RETRYING: attempt 1/3") || !strings.HasPrefix(states[1], "RETRYING: attempt 2/3") {
				t.Fatalf("retry states = %q", states)
			}
			break
		}
		if closed && len(states) < 2 {
			t.Fatalf("task closed with retry states = %q, want 2", states)
		}
		if time.Now().After(deadline) {
			states, _ := task.snapshot()
			t.Fatalf("bound task not closed with 2 retry notes; states = %q", states)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
