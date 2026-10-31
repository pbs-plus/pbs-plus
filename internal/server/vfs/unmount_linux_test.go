//go:build linux

package vfs

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func TestUnmountFallbacks(t *testing.T) {
	failure := errors.New("command failed")
	for _, tc := range []struct {
		name string
		pass int
	}{
		{"fusermount3 succeeds", 1},
		{"fusermount succeeds", 2},
		{"umount succeeds", 3},
		{"all fail", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			err := forceUnmount(context.Background(), "/test", func(_ context.Context, name string, args ...string) error {
				calls = append(calls, name)
				if args[len(args)-1] != "/test" {
					t.Fatal("lost mountpoint argument")
				}
				if len(calls) == tc.pass {
					return nil
				}
				return failure
			})
			want := []string{"fusermount3", "fusermount", "umount"}
			if tc.pass > 0 {
				want = want[:tc.pass]
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, failure) {
				t.Fatalf("lost command errors: %v", err)
			}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("calls %v, want %v", calls, want)
			}
		})
	}
}

func TestUnmountCancellationStopsFallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := forceUnmount(ctx, "/test", func(context.Context, string, ...string) error {
		calls++
		cancel()
		return context.Canceled
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestUnmountHangingCommandHasDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := forceUnmount(ctx, "/test", func(ctx context.Context, _ string, _ ...string) error {
		return exec.CommandContext(ctx, "sleep", "30").Run()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing deadline error: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("command did not stop after deadline")
	}
}

func TestForceUnmountAbsentMountIsIdempotent(t *testing.T) {
	path := t.TempDir()
	for range 2 {
		if err := ForceUnmount(context.Background(), path); err != nil {
			t.Fatal(err)
		}
	}
	mounted, err := isMounted("/")
	if err != nil || !mounted {
		t.Fatalf("root mount detection: %v %v", mounted, err)
	}
}
