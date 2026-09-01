//go:build linux

package arpcfs

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	"go.uber.org/goleak"
)

func TestDirStreamFetchesAllBatches(t *testing.T) {
	var calls atomic.Int64
	stream := newFetchDirStream(t.Context(), 100, func(context.Context) (fswire.ReadDirEntries, error) {
		if calls.Add(1) > 3 {
			return nil, os.ErrProcessDone
		}
		return fswire.ReadDirEntries{{Name: "a"}, {Name: "b"}}, nil
	})

	for stream.HasNext() {
		consumeDirBatch(stream)
	}
	if got := stream.totalReturned.Load(); got != 6 {
		t.Fatalf("returned %d entries, want 6", got)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("made %d fetches, want 4", got)
	}
}

func TestDirStreamPrefetchCancellation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	started := make(chan struct{})
	canceled := make(chan struct{})
	stream := newFetchDirStream(t.Context(), 100, func(ctx context.Context) (fswire.ReadDirEntries, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	})

	stream.startPrefetch()
	<-started
	stream.closed.Store(1)
	stream.stopPrefetch()
	<-canceled
}

func TestDirStreamRespectsMaxEntries(t *testing.T) {
	var calls atomic.Int64
	stream := newFetchDirStream(t.Context(), 3, func(context.Context) (fswire.ReadDirEntries, error) {
		calls.Add(1)
		return fswire.ReadDirEntries{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}}, nil
	})

	if !stream.HasNext() {
		t.Fatal("expected first batch")
	}
	consumeDirBatch(stream)
	if stream.HasNext() {
		t.Fatal("stream exceeded entry limit")
	}
	if got := stream.totalReturned.Load(); got != 3 {
		t.Fatalf("returned %d entries, want 3", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("made %d fetches, want 1", got)
	}
}

func BenchmarkDirStreamBatches(b *testing.B) {
	entries := make(fswire.ReadDirEntries, 1024)
	for i := range entries {
		entries[i].Name = "file"
	}

	b.ReportAllocs()
	for b.Loop() {
		var calls atomic.Int64
		stream := newFetchDirStream(b.Context(), 10000, func(context.Context) (fswire.ReadDirEntries, error) {
			time.Sleep(2 * time.Millisecond)
			if calls.Add(1) > 8 {
				return nil, os.ErrProcessDone
			}
			return entries, nil
		})
		for stream.HasNext() {
			time.Sleep(2 * time.Millisecond)
			consumeDirBatch(stream)
		}
	}
}

func newFetchDirStream(ctx context.Context, maxEntries int, fetch func(context.Context) (fswire.ReadDirEntries, error)) *DirStream {
	return &DirStream{
		fs: &ARPCFS{VFSBase: vfs.InjectBase(vfs.VFSBase{
			Ctx:    ctx,
			Backup: coredb.Backup{MaxDirEntries: maxEntries},
		})},
		fetch: fetch,
	}
}

func consumeDirBatch(stream *DirStream) {
	count := uint64(len(stream.lastResp))
	stream.curIdx.Store(count)
	stream.totalReturned.Add(count)
}
