package jobs

import (
	"context"
	"testing"
	"time"
)

func TestDatabase_ActivityFenceUsesCallerClock(t *testing.T) {
	ctx := context.Background()
	_, db := newTestEngine(t, ctx)
	if _, _, err := db.Submit(ctx, testWorkflowSubmit("test.lease-clock", "lease-clock")); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	claimed, ok, err := db.Claim(ctx, "worker-a", now, now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("claiming execution = %t, %v", ok, err)
	}

	time.Sleep(1200 * time.Millisecond)

	if _, completed, err := db.StartActivity(ctx, claimed.ID, "worker-a", claimed.Attempt, 1, "work", "input", now); err != nil || completed {
		t.Fatalf("starting activity = completed:%t, error:%v", completed, err)
	}
	if err := db.CompleteActivity(ctx, claimed.ID, "worker-a", claimed.Attempt, "work", []byte(`{}`), now); err != nil {
		t.Fatalf("completing activity: %v", err)
	}
}
