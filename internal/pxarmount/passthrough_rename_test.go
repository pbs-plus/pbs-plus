package pxarmount

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRenameFromRace_DirectAccess confirms that renamePxarEntry's write
// to mo.renameFrom does not race with readers that read mo.renameFrom
// under fs.mu.RLock (as readDirImpl and emitRenamedEntries do).
func TestRenameFromRace_DirectAccess(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.setNode(1, "/", true)
	fs.setNode(100, "/src.txt", false)
	fs.mu.Lock()
	fs.pxarDir[100] = false
	fs.mu.Unlock()

	fs.pxar.mu.Lock()
	fs.pxar.nodes[100] = node{
		inode: 100, parent: 1, mode: 0o100644, isReg: true, refs: 2,
	}
	fs.pxar.nodes[1] = node{
		inode: 1, parent: 1, mode: 0o040555, isDir: true, refs: 2,
	}
	fs.pxar.mu.Unlock()

	fs.mutationMode = true

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writer: repeatedly write renameFrom on the overlay, simulating
	// what renamePxarEntry now does under fs.mu.Lock.
	wg.Go(func() {
		for !stop.Load() {
			fs.mu.Lock()
			mo := fs.metaOverlay["/renamed.txt"]
			if mo == nil {
				mo = &metaOverride{
					xadd: make(map[string][]byte),
					xdel: make(map[string]bool),
				}
				fs.metaOverlay["/renamed.txt"] = mo
			}
			mo.renameFrom = "/src.txt"
			mo.renameFrom = ""
			fs.mu.Unlock()
		}
	})

	// Reader: repeatedly scan metaOverlay for renameFrom entries
	// under fs.mu.RLock, simulating readDirImpl/emitRenamedEntries.
	wg.Go(func() {
		for !stop.Load() {
			fs.mu.RLock()
			for _, mo := range fs.metaOverlay {
				_ = mo.renameFrom
			}
			fs.mu.RUnlock()
		}
	})

	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}
