//go:build linux

package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

type fakeMount struct {
	connected atomic.Bool
	entries   atomic.Int64
	bytes     atomic.Int64
	failEvery atomic.Int64
	calls     atomic.Int64
}

func (f *fakeMount) Progress() (mountrpc.StatusReply, error) {
	if n, every := f.calls.Add(1), f.failEvery.Load(); every > 0 && n%every != 0 {
		return mountrpc.StatusReply{}, errors.New("status rpc timed out")
	}
	return mountrpc.StatusReply{
		Connected: f.connected.Load(),
		Entries:   f.entries.Load(),
		Bytes:     f.bytes.Load(),
	}, nil
}

func newStallJob() *backupJob {
	return &backupJob{logger: log.WithScope(log.Scope{JobID: "stall-test"})}
}

func shortenStallChecks(t *testing.T, grace, tick time.Duration, minBytes int64) {
	t.Helper()
	priorGrace, priorTick, priorBytes := noProgressGrace, stallCheckInterval, minProgressBytes
	noProgressGrace, stallCheckInterval, minProgressBytes = grace, tick, minBytes
	t.Cleanup(func() { noProgressGrace, stallCheckInterval, minProgressBytes = priorGrace, priorTick, priorBytes })
}

func expectStall(t *testing.T, stalled <-chan error) {
	t.Helper()
	select {
	case err := <-stalled:
		if !errors.Is(err, ErrBackupStalled) {
			t.Fatalf("want ErrBackupStalled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchStall never reported a stall")
	}
}

func expectNoStall(t *testing.T, stalled <-chan error, done <-chan struct{}) {
	t.Helper()
	select {
	case err := <-stalled:
		t.Fatalf("watchStall killed a run that was still progressing: %v", err)
	case <-done:
	}
}

func driveFor(n int, step func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range n {
			step()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return done
}

func TestWatchStallFiresWhenConnectedMountServesNoOperations(t *testing.T) {
	shortenStallChecks(t, 50*time.Millisecond, 5*time.Millisecond, 1<<20)

	mount := &fakeMount{}
	mount.connected.Store(true)
	mount.entries.Store(1234)

	finished := make(chan struct{})
	defer close(finished)
	expectStall(t, newStallJob().watchStall(context.Background(), "upid", finished, mount))
}

func TestWatchStallStaysQuietWhileEntriesAdvance(t *testing.T) {
	shortenStallChecks(t, 80*time.Millisecond, 5*time.Millisecond, 1<<20)

	mount := &fakeMount{}
	mount.connected.Store(true)

	finished := make(chan struct{})
	defer close(finished)
	stalled := newStallJob().watchStall(context.Background(), "upid", finished, mount)
	expectNoStall(t, stalled, driveFor(40, func() { mount.entries.Add(1) }))
}

func TestWatchStallStaysQuietWhileBytesAdvance(t *testing.T) {
	shortenStallChecks(t, 80*time.Millisecond, 5*time.Millisecond, 1<<20)

	mount := &fakeMount{}
	mount.connected.Store(true)

	finished := make(chan struct{})
	defer close(finished)
	stalled := newStallJob().watchStall(context.Background(), "upid", finished, mount)
	expectNoStall(t, stalled, driveFor(40, func() { mount.bytes.Add(1 << 20) }))
}

func TestWatchStallFiresOnForwardCrawlBelowFloor(t *testing.T) {
	shortenStallChecks(t, 50*time.Millisecond, 5*time.Millisecond, 1<<20)

	mount := &fakeMount{}
	mount.connected.Store(true)

	finished := make(chan struct{})
	defer close(finished)
	stalled := newStallJob().watchStall(context.Background(), "upid", finished, mount)
	_ = driveFor(1000, func() { mount.bytes.Add(1 << 10) })
	expectStall(t, stalled)
}

func TestWatchStallTransientRPCFailuresNeitherProgressNorOutage(t *testing.T) {
	shortenStallChecks(t, 50*time.Millisecond, 5*time.Millisecond, 1<<20)

	mount := &fakeMount{}
	mount.connected.Store(true)
	mount.failEvery.Store(statusRPCAttempts)

	finished := make(chan struct{})
	defer close(finished)
	expectStall(t, newStallJob().watchStall(context.Background(), "upid", finished, mount))
}
