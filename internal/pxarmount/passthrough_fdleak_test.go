package pxarmount

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestFinishCreate_FdLeakOnFstatError verifies that finishCreate
// doesn't leak a file descriptor when Fstat fails after the handle
// is registered.
func TestFinishCreate_FdLeakOnFstatError(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Count open fds before.
	before := countOpenFds(t)

	// Create a file. This should succeed normally.
	var out fuse.CreateOut
	st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Mode:     0o644,
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
	}, "leak_test.txt", &out)
	if st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}

	// Close the file via Release to clean up.
	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: out.NodeId},
		Fh:       out.Fh,
	})

	after := countOpenFds(t)
	leaked := after - before
	if leaked > 0 {
		t.Errorf("leaked %d file descriptors after Create+Release", leaked)
	}
}

// TestCreate_SecondOpenPathFdLeak checks the fallback path in Create
// where the first O_EXCL open fails (file exists) and the second open
// succeeds. The second fd should be properly registered and released.
func TestCreate_SecondOpenPathFdLeak(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Pre-create the file so the O_EXCL open fails.
	abs := filepath.Join(backingDir, "existing.txt")
	if err := os.WriteFile(abs, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	before := countOpenFds(t)

	var out fuse.CreateOut
	st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Mode:     0o644,
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
	}, "existing.txt", &out)
	if st != fuse.OK {
		t.Fatalf("Create existing file: %v", st)
	}

	// Verify content was truncated.
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Errorf("file should be truncated, got %d bytes", len(data))
	}

	// Release the handle.
	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: out.NodeId},
		Fh:       out.Fh,
	})

	after := countOpenFds(t)
	leaked := after - before
	if leaked > 0 {
		t.Errorf("leaked %d file descriptors in Create fallback path", leaked)
	}
}

// TestRegisterFh_FdLeakOnError verifies that if registerFh's Open fails,
// no fd is leaked.
func TestRegisterFh_FdLeakOnError(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	before := countOpenFds(t)

	// Try to open a non-existent file.
	_, err := fs.registerFh("/nonexistent/file.txt", 999, os.O_RDONLY)
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}

	after := countOpenFds(t)
	leaked := after - before
	if leaked > 0 {
		t.Errorf("leaked %d file descriptors on failed registerFh", leaked)
	}
}

// countOpenFds counts the number of open file descriptors for the current process.
func countOpenFds(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("cannot count fds: /proc/self/fd not available")
	}
	return len(entries)
}
