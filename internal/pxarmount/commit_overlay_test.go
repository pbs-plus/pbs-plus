package pxarmount

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// TestApplyOverlayToMeta_NoMutationUnderRLock verifies that applyOverlayToMeta
// does not mutate mo.xadd (no delete) while only holding fs.mu as RLock.
// Previously, applyOverlayToMeta did delete(mo.xadd, ...) which caused
// concurrent map read/write panics with concurrent GetXAttr/ListXAttr readers.
func TestApplyOverlayToMeta_NoMutationUnderRLock(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	modeVal := uint32(0o100644)
	fs.mu.Lock()
	fs.metaOverlay["/test.txt"] = &metaOverride{
		mode: &modeVal,
		xadd: map[string][]byte{
			"user.exists": []byte("replaced"),
		},
		xdel: map[string]bool{},
	}
	fs.mu.Unlock()

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Simulate the commit path: call applyOverlayToMeta under RLock.
	wg.Go(func() {
		for !stop.Load() {
			fs.mu.RLock()
			meta := pxar.Metadata{
				XAttrs: []format.XAttr{
					format.NewXAttr([]byte("user.exists"), []byte("original")),
				},
			}
			applyOverlayToMeta("/test.txt", &meta, fs.metaOverlay)
			fs.mu.RUnlock()
		}
	})

	// Concurrent reader iterating mo.xadd under RLock (simulates ListXAttr).
	wg.Go(func() {
		for !stop.Load() {
			fs.mu.RLock()
			if mo := fs.metaOverlay["/test.txt"]; mo != nil {
				for name, val := range mo.xadd {
					_ = name
					_ = val
				}
			}
			fs.mu.RUnlock()
		}
	})

	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}
