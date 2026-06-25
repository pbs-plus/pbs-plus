package pxarmount

import (
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/safemap"
)

func TestHotSwapSelfDeadlock(t *testing.T) {
	fs := &PxarFS{
		nodes:      make(map[uint64]node),
		dirEntries: safemap.New[uint64, []dirEntrySlim](),
	}

	fs.readerMu.RLock()
	fs.readerMu.RUnlock()

	done := make(chan struct{})
	go func() {
		fs.HotSwap(nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("HotSwap deadlocked: readerMu.Lock() blocked after RLock/RUnlock in same goroutine")
	}
}

func TestReaderMuNotHeldAfterCommitWalk(t *testing.T) {
	fs := &PxarFS{
		nodes:      make(map[uint64]node),
		dirEntries: safemap.New[uint64, []dirEntrySlim](),
	}

	var wg sync.WaitGroup

	wg.Go(func() {

		fs.readerMu.RLock()
		fs.readerMu.RUnlock()

		fs.HotSwap(nil)
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("readerMu was not released before HotSwap — self-deadlock detected")
	}
}
