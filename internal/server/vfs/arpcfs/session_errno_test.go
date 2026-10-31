//go:build linux

package arpcfs

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
)

func newTestFS() *ARPCFS {
	return &ARPCFS{VFSBase: vfs.InjectBase(vfs.VFSBase{})}
}

func TestErrnoForMapsSessionLossToEIO(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		fallback syscall.Errno
		want     syscall.Errno
	}{
		{
			name:     "lookup miss keeps ENOENT",
			err:      errors.New("agent reported no such entry"),
			fallback: syscall.ENOENT,
			want:     syscall.ENOENT,
		},
		{
			name:     "session loss overrides ENOENT",
			err:      fmt.Errorf("%w: %w", ErrSessionLost, context.DeadlineExceeded),
			fallback: syscall.ENOENT,
			want:     syscall.EIO,
		},
		{
			name:     "session loss overrides ESTALE",
			err:      fmt.Errorf("%w: severed", ErrSessionLost),
			fallback: syscall.ESTALE,
			want:     syscall.EIO,
		},
		{
			name:     "session loss overrides a success fallback",
			err:      fmt.Errorf("open: %w", ErrSessionLost),
			fallback: 0,
			want:     syscall.EIO,
		},
		{
			name:     "unrelated error keeps success fallback",
			err:      errors.New("short read"),
			fallback: 0,
			want:     0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errnoFor(tc.err, tc.fallback); got != tc.want {
				t.Fatalf("errnoFor(%v, %v) = %v, want %v", tc.err, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestSessionLostWrapsAndLatches(t *testing.T) {
	fs := newTestFS()

	first := fs.sessionLost(context.DeadlineExceeded)
	if !errors.Is(first, ErrSessionLost) {
		t.Fatalf("sessionLost did not tag the error with ErrSessionLost: %v", first)
	}
	if !errors.Is(first, context.DeadlineExceeded) {
		t.Fatalf("sessionLost dropped the cause: %v", first)
	}
	if !fs.sessionDown.Load() {
		t.Fatal("sessionLost did not latch the outage")
	}

	fs.sessionRestored()
	if fs.sessionDown.Load() {
		t.Fatal("sessionRestored did not clear the latch")
	}
}

func TestSessionLostDoesNotDoubleWrap(t *testing.T) {
	fs := newTestFS()

	already := fmt.Errorf("%w: severed", ErrSessionLost)
	if got := fs.sessionLost(already); got != already {
		t.Fatalf("sessionLost rewrapped an already tagged error: %v", got)
	}
}

func TestLogOnceStopsAtCap(t *testing.T) {
	fs := newTestFS()

	for i := range maxLoggedPaths + 10 {
		fs.logOnce(fmt.Sprintf("/path/%d", i), errors.New("boom"), "Open")
	}

	if got := fs.loggedPathCount.Load(); got != maxLoggedPaths {
		t.Fatalf("loggedPathCount = %d, want %d", got, maxLoggedPaths)
	}
}
