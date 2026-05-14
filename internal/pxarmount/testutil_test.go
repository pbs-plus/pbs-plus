package pxarmount

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// newTestPassthroughFS creates a PassthroughFS backed by a temp directory.
func newTestPassthroughFS(t *testing.T) (*PassthroughFS, string, func()) {
	t.Helper()
	backingDir, err := os.MkdirTemp("", "pxar-race-test-")
	if err != nil {
		t.Fatal(err)
	}

	pxar := &PxarFS{
		nodes: make(map[uint64]node),
	}

	fs := &PassthroughFS{
		pxar:         pxar,
		backingDir:   backingDir,
		nodePaths:    make(map[uint64]string),
		pathToIno:    make(map[string]uint64),
		backed:       make(map[uint64]bool),
		pxarDir:      make(map[uint64]bool),
		deletedPaths: make(map[string]bool),
		handles:      make(map[uint64]*passFh),
		metaOverlay:  make(map[string]*metaOverride),
	}

	fs.setNode(RootInode, "/", true)
	fs.mu.Lock()
	fs.pxarDir[RootInode] = true
	fs.mu.Unlock()

	cleanup := func() {
		_ = os.RemoveAll(backingDir)
	}
	return fs, backingDir, cleanup
}

// countPaths returns the number of inode mappings per path.
func countPaths(fs *PassthroughFS) map[string]int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	m := make(map[string]int)
	for _, p := range fs.nodePaths {
		m[p]++
	}
	return m
}

// snapshotPaths returns a snapshot of path→inode for all non-root entries.
func snapshotPaths(fs *PassthroughFS) map[string]uint64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	m := make(map[string]uint64, len(fs.nodePaths))
	for ino, p := range fs.nodePaths {
		if p != "/" {
			m[p] = ino
		}
	}
	return m
}

var _ fuse.RawFileSystem = (*PassthroughFS)(nil)

func init() {
	_ = syscall.ENOENT
	_ = fmt.Sprintf
	_ = filepath.Join
	_ = sync.Mutex{}
}
