//go:build linux

package arpcfs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
)

func TestUnmountWaitsForCacheCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &ARPCFS{
		VFSBase:    vfs.InjectBase(vfs.VFSBase{Ctx: ctx, Cancel: cancel}),
		mountpoint: t.TempDir(), cleanupDone: make(chan struct{}),
	}
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := fs.Unmount(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup wait: %v", err)
	}
	if fs.unmounted {
		t.Fatal("teardown completed before cache cleanup")
	}
	close(fs.cleanupDone)
	if err := fs.Unmount(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUnmountRetriesFailedDetach(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &ARPCFS{VFSBase: vfs.InjectBase(vfs.VFSBase{Ctx: ctx, Cancel: cancel}), mountpoint: t.TempDir()}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := fs.Unmount(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("first unmount: %v", err)
	}
	if fs.unmounted {
		t.Fatal("failed detach was marked complete")
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if err := fs.Unmount(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if !fs.unmounted || fs.Ctx.Err() == nil {
		t.Fatal("retry did not finish teardown")
	}
}
