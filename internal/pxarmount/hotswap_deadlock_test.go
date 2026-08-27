package pxarmount

import (
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
)

type blockingReaderAt struct {
	entered chan struct{}
	release chan struct{}
}

func (r *blockingReaderAt) ReadAt(p []byte, _ int64) (int, error) {
	close(r.entered)
	<-r.release
	clear(p)
	return len(p), nil
}

func TestHotSwapWaitsForActiveRead(t *testing.T) {
	reader := &blockingReaderAt{entered: make(chan struct{}), release: make(chan struct{})}
	fs := &PxarFS{
		nodes: map[uint64]node{
			2: {inode: 2, fileSize: 8},
		},
		dirEntries: safemap.New[uint64, []dirEntrySlim](),
		readerAt:   reader,
	}

	readDone := make(chan struct{})
	go func() {
		fs.readFileContent(2, 0, 8, make([]byte, 8))
		close(readDone)
	}()
	<-reader.entered

	swapDone := make(chan struct{})
	go func() {
		fs.HotSwap(nil)
		close(swapDone)
	}()

	select {
	case <-swapDone:
		t.Fatal("HotSwap completed while the old reader was active")
	case <-time.After(20 * time.Millisecond):
	}

	close(reader.release)
	select {
	case <-readDone:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not finish")
	}
	select {
	case <-swapDone:
	case <-time.After(3 * time.Second):
		t.Fatal("HotSwap did not finish after the read drained")
	}
}

func TestCommitGateWaitsForActiveMutation(t *testing.T) {
	fs := NewMutableFS(nil, nil, t.TempDir())
	fs.beginMutation()

	done := make(chan struct{})
	go func() {
		fs.mutationMu.Lock()
		close(done)
		fs.mutationMu.Unlock()
	}()

	select {
	case <-done:
		t.Fatal("commit gate ignored an active mutation")
	case <-time.After(20 * time.Millisecond):
	}

	fs.endMutation()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("commit gate did not acquire after the mutation drained")
	}
}

func TestFlushWaitsForCommitGate(t *testing.T) {
	fs := NewMutableFS(nil, nil, t.TempDir())
	fs.mutationMu.Lock()

	done := make(chan fuse.Status)
	go func() {
		done <- fs.Flush(nil, &fuse.FlushIn{})
	}()

	select {
	case <-done:
		t.Fatal("Flush bypassed the commit gate")
	case <-time.After(20 * time.Millisecond):
	}

	fs.mutationMu.Unlock()
	select {
	case status := <-done:
		if status != fuse.OK {
			t.Fatalf("Flush status = %s, want OK", status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Flush did not resume after commit gate opened")
	}
}
