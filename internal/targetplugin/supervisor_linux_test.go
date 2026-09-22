package targetplugin

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSupervisorRejectsInvalidLimits(t *testing.T) {
	tests := []struct {
		name      string
		global    int
		perPlugin int
	}{
		{name: "zero global", perPlugin: 1},
		{name: "zero per plugin", global: 1},
		{name: "per plugin exceeds global", global: 1, perPlugin: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewSupervisor(test.global, test.perPlugin); err == nil {
				t.Fatal("NewSupervisor succeeded")
			}
		})
	}
}

func TestSupervisorEnforcesPerPluginLimit(t *testing.T) {
	supervisor, err := NewSupervisor(2, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- supervisor.Run(t.Context(), "org.pbs-plus.first", executable, func(context.Context, *Process) error {
			close(started)
			<-release
			return nil
		}, "-test.run=^TestPluginProcessHelper$")
	}()
	waitSupervisorSignal(t, started)

	if err := supervisor.Run(t.Context(), "org.pbs-plus.second", executable, func(context.Context, *Process) error {
		return nil
	}, "-test.run=^TestPluginProcessHelper$"); err != nil {
		t.Fatalf("Run different plugin: %v", err)
	}

	var invoked atomic.Bool
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	err = supervisor.Run(ctx, "org.pbs-plus.first", executable, func(context.Context, *Process) error {
		invoked.Store(true)
		return nil
	}, "-test.run=^TestPluginProcessHelper$")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run same plugin error = %v, want deadline exceeded", err)
	}
	if invoked.Load() {
		t.Fatal("capacity-blocked operation was invoked")
	}

	close(release)
	released = true
	if err := waitSupervisorResult(t, firstDone); err != nil {
		t.Fatalf("first Run: %v", err)
	}
}

func TestSupervisorReleasesGlobalCapacity(t *testing.T) {
	supervisor, err := NewSupervisor(1, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- supervisor.Run(t.Context(), "org.pbs-plus.first", executable, func(context.Context, *Process) error {
			close(started)
			<-release
			return nil
		}, "-test.run=^TestPluginProcessHelper$")
	}()
	waitSupervisorSignal(t, started)

	var invoked atomic.Bool
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	err = supervisor.Run(ctx, "org.pbs-plus.second", executable, func(context.Context, *Process) error {
		invoked.Store(true)
		return nil
	}, "-test.run=^TestPluginProcessHelper$")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run at global limit error = %v, want deadline exceeded", err)
	}
	if invoked.Load() {
		t.Fatal("capacity-blocked operation was invoked")
	}

	close(release)
	released = true
	if err := waitSupervisorResult(t, firstDone); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	operationErr := errors.New("operation failed")
	err = supervisor.Run(t.Context(), "org.pbs-plus.second", executable, func(context.Context, *Process) error {
		return operationErr
	}, "-test.run=^TestPluginProcessHelper$")
	if !errors.Is(err, operationErr) {
		t.Fatalf("Run operation error = %v", err)
	}
	var reaped *Process
	if err := supervisor.Run(t.Context(), "org.pbs-plus.second", executable, func(_ context.Context, process *Process) error {
		reaped = process
		return nil
	}, "-test.run=^TestPluginProcessHelper$"); err != nil {
		t.Fatalf("Run after operation error: %v", err)
	}
	assertProcessGone(t, reaped)
}

func TestSupervisorOpenHoldsCapacityUntilClose(t *testing.T) {
	supervisor, err := NewSupervisor(1, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	process, err := supervisor.Open(t.Context(), "org.pbs-plus.first", executable, "-test.run=^TestPluginProcessHelper$")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	if _, err := supervisor.Open(ctx, "org.pbs-plus.second", executable, "-test.run=^TestPluginProcessHelper$"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open at global limit error = %v, want deadline exceeded", err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	process, err = supervisor.Open(t.Context(), "org.pbs-plus.second", executable, "-test.run=^TestPluginProcessHelper$")
	if err != nil {
		t.Fatalf("Open after close: %v", err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSupervisorReleasesCapacityAfterStartFailure(t *testing.T) {
	supervisor, err := NewSupervisor(1, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	if err := supervisor.Run(t.Context(), "org.pbs-plus.test", "/missing/plugin", func(context.Context, *Process) error {
		return nil
	}); err == nil {
		t.Fatal("Run missing executable succeeded")
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	if err := supervisor.Run(t.Context(), "org.pbs-plus.test", executable, func(context.Context, *Process) error {
		return nil
	}, "-test.run=^TestPluginProcessHelper$"); err != nil {
		t.Fatalf("Run after start failure: %v", err)
	}
}

func waitSupervisorSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for plugin operation")
	}
}

func waitSupervisorResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for plugin result")
		return nil
	}
}
