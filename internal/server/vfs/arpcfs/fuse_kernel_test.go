//go:build linux

package arpcfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type probeFile struct {
	fs.Inode
	size    int64
	latency time.Duration

	mu          sync.Mutex
	maxRead     int
	calls       int
	inflight    atomic.Int64
	maxInflight atomic.Int64
}

var _ = (fs.NodeGetattrer)((*probeFile)(nil))
var _ = (fs.NodeOpener)((*probeFile)(nil))
var _ = (fs.NodeReader)((*probeFile)(nil))

func (f *probeFile) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0o444
	out.Size = uint64(f.size)
	return 0
}

func (f *probeFile) Open(context.Context, uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, 0
}

func (f *probeFile) Read(_ context.Context, _ fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	cur := f.inflight.Add(1)
	defer f.inflight.Add(-1)
	for m := f.maxInflight.Load(); cur > m && !f.maxInflight.CompareAndSwap(m, cur); m = f.maxInflight.Load() {
	}
	f.mu.Lock()
	f.calls++
	f.maxRead = max(f.maxRead, len(dest))
	f.mu.Unlock()
	time.Sleep(f.latency)
	n := max(0, min(int64(len(dest)), f.size-off))
	return fuse.ReadResultData(dest[:n]), 0
}

type probeRoot struct {
	fs.Inode
	file *probeFile
}

func (r *probeRoot) OnAdd(ctx context.Context) {
	r.AddChild("data", r.NewPersistentInode(ctx, r.file, fs.StableAttr{Mode: syscall.S_IFREG}), false)
}

// TestKernelFUSEReadShape records the read requests the kernel sends for a
// sequential reader on the production mount options. It needs root and /dev/fuse.
func TestKernelFUSEReadShape(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount FUSE")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse")
	}
	const size = 64 << 20
	const latency = time.Millisecond
	large := mountOptions("probe")
	large.MaxWrite, large.MaxReadAhead, large.MaxBackground = 1<<20, 1<<20, 64
	for _, tc := range []struct {
		name   string
		opts   *fs.Options
		appBuf int
	}{
		{"production", mountOptions("probe"), 64 << 10},
		{"production", mountOptions("probe"), 1 << 20},
		{"max_write=1M", large, 64 << 10},
		{"max_write=1M", large, 1 << 20},
	} {
		appBuf := tc.appBuf
		file := &probeFile{size: size, latency: latency}
		dir := t.TempDir()
		server, err := fs.Mount(dir, &probeRoot{file: file}, tc.opts)
		if err != nil {
			t.Fatalf("mount: %v", err)
		}
		f, err := os.Open(filepath.Join(dir, "data"))
		if err != nil {
			_ = server.Unmount()
			t.Fatalf("open: %v", err)
		}
		start := time.Now()
		n, err := io.CopyBuffer(io.Discard, struct{ io.Reader }{f}, make([]byte, appBuf))
		elapsed := time.Since(start)
		_ = f.Close()
		if uerr := server.Unmount(); uerr != nil {
			t.Errorf("unmount: %v", uerr)
		}
		if err != nil || n != size {
			t.Fatalf("read %d bytes: %v", n, err)
		}
		t.Logf("%s, app buffer %d KiB: %d requests, largest %d KiB, %d max in flight, %.1f MiB/s at %v per request",
			tc.name, appBuf>>10, file.calls, file.maxRead>>10, file.maxInflight.Load(), float64(size)/(1<<20)/elapsed.Seconds(), latency)
		if tc.opts.MaxWrite == 0 && file.maxRead > kernelReadSize {
			t.Errorf("largest kernel read %d exceeds kernelReadSize %d; update the readahead tests", file.maxRead, kernelReadSize)
		}
	}
}
