//go:build linux

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

// A canceled ctx during activity recording must not wrap the real error.
func TestEngine_ActivityFailureRecordedWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	engine, _ := newTestEngine(t, ctx)
	boom := errors.New("boom")

	if err := engine.Register("test.cancelrecord", func(workflow *WorkflowContext) error {
		_, err := workflow.Activity("work", json.RawMessage(`{}`), func(ctx context.Context, _ ActivityInfo) (json.RawMessage, error) {
			_, _ = engine.Cancel(context.Background(), workflow.Execution.ID)
			return nil, boom
		})
		if err != nil && !errors.Is(err, boom) {
			t.Errorf("activity error wrapped: %v", err)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	execution, _, err := engine.Submit(ctx, testWorkflowSubmit("test.cancelrecord", "cancelrecord"))
	if err != nil {
		t.Fatal(err)
	}
	waitForWorkflowState(t, ctx, engine, execution.ID, jobdb.StateCanceled)
}
