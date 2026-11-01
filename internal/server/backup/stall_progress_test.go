//go:build linux

package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

type fakeMount struct {
	connected atomic.Bool
	activity  atomic.Int64
}

func (f *fakeMount) Progress() (bool, int64) {
	return f.connected.Load(), f.activity.Load()
}

func newStallJob() *backupJob {
	return &backupJob{logger: log.WithScope(log.Scope{JobID: "stall-test"})}
}

func shortenStallChecks(t *testing.T, grace, tick time.Duration) {
	t.Helper()
	priorGrace, priorTick := noProgressGrace, stallCheckInterval
	noProgressGrace, stallCheckInterval = grace, tick
	t.Cleanup(func() { noProgressGrace, stallCheckInterval = priorGrace, priorTick })
}

func TestWatchStallFiresWhenConnectedMountServesNoOperations(t *testing.T) {
	shortenStallChecks(t, 50*time.Millisecond, 5*time.Millisecond)

	mount := &fakeMount{}
	mount.connected.Store(true)
	mount.activity.Store(1234)

	b := newStallJob()
	finished := make(chan struct{})
	defer close(finished)

	stalled := b.watchStall(context.Background(), "upid", finished, mount)

	select {
	case err := <-stalled:
		if !errors.Is(err, ErrBackupStalled) {
			t.Fatalf("want ErrBackupStalled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchStall never reported a stall for a connected but idle mount")
	}
}

func TestWatchStallStaysQuietWhileOperationsAdvance(t *testing.T) {
	shortenStallChecks(t, 80*time.Millisecond, 5*time.Millisecond)

	mount := &fakeMount{}
	mount.connected.Store(true)

	b := newStallJob()
	finished := make(chan struct{})
	defer close(finished)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 40 {
			mount.activity.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	stalled := b.watchStall(context.Background(), "upid", finished, mount)

	select {
	case err := <-stalled:
		t.Fatalf("watchStall killed a run that was still doing work: %v", err)
	case <-done:
	}
}
