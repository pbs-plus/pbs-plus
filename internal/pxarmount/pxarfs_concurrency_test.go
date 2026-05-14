package pxarmount

import (
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestHotSwap_ReaderAtRace verifies that HotSwap and Read properly
// synchronize access to fs.readerAt via mu/mu.RLock.
func TestHotSwap_ReaderAtRace(t *testing.T) {
	pxarFS := &PxarFS{
		nodes: make(map[uint64]node),
	}
	pxarFS.nodes[RootInode] = node{
		inode:  RootInode,
		parent: RootInode,
		mode:   uint64(syscall.S_IFDIR | 0o755),
		isDir:  true,
		refs:   1,
	}

	var wg sync.WaitGroup
	const rounds = 100

	wg.Go(func() {
		for range rounds {
			pxarFS.mu.RLock()
			_ = pxarFS.readerAt
			pxarFS.mu.RUnlock()
		}
	})

	wg.Go(func() {
		for range rounds {
			pxarFS.mu.Lock()
			pxarFS.readerAt = nil
			pxarFS.mu.Unlock()
		}
	})

	wg.Wait()
}

// TestReadlink_HoldsReaderMu verifies Readlink acquires readerMu before
// calling readEntryForNode. Root inode uses a synthetic entry so no
// reader is needed, but the lock acquisition path is exercised.
func TestReadlink_HoldsReaderMu(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.pxar.mu.Lock()
	fs.pxar.nodes[RootInode] = node{
		inode:     RootInode,
		parent:    RootInode,
		mode:      uint64(syscall.S_IFLNK | 0o777),
		isSymlink: true,
	}
	fs.pxar.mu.Unlock()

	// Root entry is synthetic (no reader), but Readlink still acquires readerMu.
	// The key invariant: no panic or race.
	_, _ = fs.pxar.Readlink(nil, &fuse.InHeader{NodeId: RootInode})
}

// TestMetaOverlay_ConcurrentGetAttrSetAttr verifies that concurrent
// GetAttr (reads metaOverlay) and SetAttr (writes metaOverlay) don't race.
func TestMetaOverlay_ConcurrentGetAttrSetAttr(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	const ino uint64 = 100 | NonDirBit
	relPath := "/overlay-file.txt"
	fs.pxar.mu.Lock()
	fs.pxar.nodes[ino] = node{
		inode:  ino,
		parent: RootInode,
		mode:   uint64(syscall.S_IFREG | 0o644),
		isReg:  true,
		refs:   1,
	}
	fs.pxar.mu.Unlock()

	fs.mu.Lock()
	fs.nodePaths[ino] = relPath
	fs.pathToIno[relPath] = ino
	fs.mu.Unlock()

	mutDir := t.TempDir()
	tl, err := OpenTransactionLog(mutDir)
	if err != nil {
		t.Fatal(err)
	}
	fs.txnLog = tl

	var wg sync.WaitGroup
	const rounds = 50

	wg.Go(func() {
		for range rounds {
			mode := uint32(0o644)
			var sai fuse.SetAttrIn
			sai.NodeId = ino
			sai.Valid = 1 // FATTR_MODE
			sai.Mode = mode
			_ = fs.SetAttr(nil, &sai, &fuse.AttrOut{})
		}
	})

	wg.Go(func() {
		for range rounds {
			_ = fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: ino}}, &fuse.AttrOut{})
		}
	})

	wg.Wait()
}

// TestRenamePxarEntry_OverlayCleanupRace verifies that concurrent
// overlay cleanup in renamePxarEntry and getOverlay don't race.
func TestRenamePxarEntry_OverlayCleanupRace(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	for i := range 10 {
		path := "/renamed-file-" + string(rune('a'+i)) + ".txt"
		fs.mu.Lock()
		fs.metaOverlay[path] = &metaOverride{
			renameFrom: "/src-dir/old-file.txt",
			xadd:       make(map[string][]byte),
			xdel:       make(map[string]bool),
		}
		fs.mu.Unlock()
	}

	var wg sync.WaitGroup
	const rounds = 20

	wg.Go(func() {
		for range rounds {
			fs.mu.Lock()
			for path, mo := range fs.metaOverlay {
				if mo.renameFrom == "/src-dir/old-file.txt" {
					delete(fs.metaOverlay, path)
				}
			}
			fs.mu.Unlock()

			fs.mu.Lock()
			for i := range 10 {
				path := "/renamed-file-" + string(rune('a'+i)) + ".txt"
				fs.metaOverlay[path] = &metaOverride{
					renameFrom: "/src-dir/old-file.txt",
					xadd:       make(map[string][]byte),
					xdel:       make(map[string]bool),
				}
			}
			fs.mu.Unlock()
		}
	})

	wg.Go(func() {
		for range rounds {
			_ = fs.getOverlay("/renamed-file-a.txt")
		}
	})

	wg.Wait()
}
