//go:build linux

package backup

import (
	"context"
	"testing"
)

func TestCleanupCancelsJobOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	b := &backupJob{cancel: func() { calls++; cancel() }}
	b.cleanup()
	b.cleanup()
	if ctx.Err() == nil || calls != 1 {
		t.Fatalf("cancellation=%v calls=%d", ctx.Err(), calls)
	}
}
