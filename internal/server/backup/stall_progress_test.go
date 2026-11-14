//go:build linux

package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
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
	return newStallJobWith(0, 0)
}

func newStallJobWith(timeout, minProgress int) *backupJob {
	return &backupJob{
		logger: log.WithScope(log.Scope{JobID: "stall-test"}),
		job:    coredb.Backup{StallTimeout: timeout, StallMinProgress: minProgress},
	}
}

// shortenStallChecks runs the rule in milliseconds and KiB, so the 120/1024 defaults become 120ms and 1 MiB.
func shortenStallChecks(t *testing.T, tick time.Duration) {
	t.Helper()
	priorTick, priorUnit, priorBytes := stallCheckInterval, stallTimeoutUnit, stallProgressUnit
	stallCheckInterval, stallTimeoutUnit, stallProgressUnit = tick, time.Millisecond, 1<<10
	t.Cleanup(func() { stallCheckInterval, stallTimeoutUnit, stallProgressUnit = priorTick, priorUnit, priorBytes })
}

func connectedMount() *fakeMount {
	m := &fakeMount{}
	m.connected.Store(true)
	return m
}

func watch(t *testing.T, b *backupJob, mount mountProgressReporter) <-chan error {
	t.Helper()
	finished := make(chan struct{})
	t.Cleanup(func() { close(finished) })
	return b.watchStall(context.Background(), "upid", finished, mount)
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

func TestStallRuleDefaults(t *testing.T) {
	cases := map[string]struct {
		timeout, minProgress int
		grace                time.Duration
		minBytes             int64
	}{
		"unset uses defaults":    {0, 0, 2 * time.Hour, 1 << 30},
		"custom values":          {30, 256, 30 * time.Minute, 256 << 20},
		"disabled":               {coredb.StallTimeoutDisabled, 0, 0, 0},
		"negative floor default": {45, -5, 45 * time.Minute, 1 << 30},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			grace, minBytes := newStallJobWith(tc.timeout, tc.minProgress).stallRule()
			if grace != tc.grace || minBytes != tc.minBytes {
				t.Fatalf("stallRule = (%s, %d), want (%s, %d)", grace, minBytes, tc.grace, tc.minBytes)
			}
		})
	}
}

func TestWatchStallFiresWhenConnectedMountServesNoOperations(t *testing.T) {
	shortenStallChecks(t, 5*time.Millisecond)
	mount := connectedMount()
	mount.entries.Store(1234)
	expectStall(t, watch(t, newStallJobWith(50, 1024), mount))
}

func TestWatchStallUsesDefaultsForUnsetJob(t *testing.T) {
	shortenStallChecks(t, 5*time.Millisecond)
	expectStall(t, watch(t, newStallJob(), connectedMount()))
}

func TestWatchStallStaysQuietWhileEntriesAdvance(t *testing.T) {
	shortenStallChecks(t, 5*time.Millisecond)
	mount := connectedMount()
	stalled := watch(t, newStallJobWith(80, 1024), mount)
	expectNoStall(t, stalled, driveFor(40, func() { mount.entries.Add(1) }))
}

func TestWatchStallStaysQuietWhileBytesAdvance(t *testing.T) {
	shortenStallChecks(t, 5*time.Millisecond)
	mount := connectedMount()
	stalled := watch(t, newStallJobWith(80, 1024), mount)
	expectNoStall(t, stalled, driveFor(40, func() { mount.bytes.Add(1 << 20) }))
}

func TestWatchStallFiresOnForwardCrawlBelowFloor(t *testing.T) {
	shortenStallChecks(t, 5*time.Millisecond)
	mount := connectedMount()
	stalled := watch(t, newStallJobWith(50, 1024), mount)
	_ = driveFor(1000, func() { mount.bytes.Add(1 << 10) })
	expectStall(t, stalled)
}

func TestWatchStallDisabledNeverFiresOnIdleMount(t *testing.T) {
	shortenStallChecks(t, 5*time.Millisecond)
	stalled := watch(t, newStallJobWith(coredb.StallTimeoutDisabled, 0), connectedMount())
	expectNoStall(t, stalled, driveFor(40, func() {}))
}

func TestWatchStallTransientRPCFailuresNeitherProgressNorOutage(t *testing.T) {
	shortenStallChecks(t, 5*time.Millisecond)
	mount := connectedMount()
	mount.failEvery.Store(statusRPCAttempts)
	expectStall(t, watch(t, newStallJobWith(50, 1024), mount))
}
