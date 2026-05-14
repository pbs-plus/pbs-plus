package pxarmount

import (
	"os"
	"sync"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// newTestMutableFS creates a MutableFS backed by a temp directory with a
// fresh journal. Returns the MutableFS, the mutable backing dir, and a cleanup
// function.
func newTestMutableFS(t *testing.T) (*MutableFS, string, func()) {
	t.Helper()
	backingDir, err := os.MkdirTemp("", "pxar-mfs-test-")
	if err != nil {
		t.Fatal(err)
	}

	journalDir := backingDir + "/" + JournalDir
	journal, err := OpenJournal(journalDir)
	if err != nil {
		os.RemoveAll(backingDir)
		t.Fatal(err)
	}

	pxar, _ := NewPxarFS(nil)
	mfs := NewMutableFS(pxar, journal, backingDir)

	// Map root inode.
	mfs.mapInode(RootInode, "/")

	// Ensure mutable root exists.
	if err := mfs.InitMutableRoot(); err != nil {
		journal.Close()
		os.RemoveAll(backingDir)
		t.Fatal(err)
	}

	cleanup := func() {
		mfs.Close()
		journal.Close()
		_ = os.RemoveAll(backingDir)
	}
	return mfs, backingDir, cleanup
}

// countPaths returns the number of inode-to-path mappings.
func countPaths(fs *MutableFS) map[string]int {
	pathInoMu.RLock()
	defer pathInoMu.RUnlock()
	m := make(map[string]int)
	for _, p := range inoToPath {
		m[p]++
	}
	return m
}

// snapshotPaths returns a snapshot of path→inode for all non-root entries.
func snapshotPaths(fs *MutableFS) map[string]uint64 {
	pathInoMu.RLock()
	defer pathInoMu.RUnlock()
	m := make(map[string]uint64, len(pathToIno))
	for p, ino := range pathToIno {
		if p != "/" {
			m[p] = ino
		}
	}
	return m
}

var _ fuse.RawFileSystem = (*MutableFS)(nil)

func init() {
	_ = sync.Mutex{}
}
